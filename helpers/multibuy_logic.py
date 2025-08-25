# helpers/multibuy_logic.py
import os
import asyncio
import logging
import httpx
import random
from datetime import datetime, timezone, timedelta
from telegram.ext import ContextTypes
from pathlib import Path
import sys
import importlib.util
from html import escape as html_escape
from time import perf_counter
from math import ceil
import re
import shutil
import time

# Попытка подключить готовые функции из SolanaTrackerBot (не копируя код)
ST_WALLET_TRACKER = None
try:
    repo_root = Path(__file__).resolve().parent.parent
    sys.path.append(str(repo_root))
    sys.path.append(str(repo_root / 'SolanaTrackerBot'))
    from SolanaTrackerBot.helpers import wallet_tracker as ST_WALLET_TRACKER  # type: ignore
except Exception:
    # Fallback: загрузить модуль напрямую с пути
    try:
        repo_root = Path(__file__).resolve().parent.parent
        module_path = repo_root / 'SolanaTrackerBot' / 'helpers' / 'wallet_tracker.py'
        if module_path.exists():
            spec = importlib.util.spec_from_file_location("wallet_tracker_ext", str(module_path))
            if spec and spec.loader:
                _mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(_mod)  # type: ignore
                ST_WALLET_TRACKER = _mod  # type: ignore
    except Exception:
        ST_WALLET_TRACKER = None

# --- Configuration ---
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')
AXIOM_REF = os.getenv("AXIOM_REF", "")
AXIOM_BASE_URL = os.getenv("AXIOM_BASE_URL", "https://axiom.trade")
AXIOM_TOKEN_LINK_TEMPLATE = os.getenv("AXIOM_TOKEN_LINK_TEMPLATE", f"{os.getenv('AXIOM_BASE_URL','https://axiom.trade').rstrip('/')}/meme/{{address}}")
MULTI_EVENT_THRESHOLD = int(os.getenv("MULTI_EVENT_THRESHOLD", 3))
# Временные окна детекции: ранние 1,5,10 мин + расширенные 30,60 мин по умолчанию
TIME_WINDOW_MINUTES = int(os.getenv("TIME_WINDOW_MINUTES", 30))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", 10)) # Seconds
# Дефолт мин. капа 75k (можно переопределить на 50k/100k в .env)
MIN_MARKET_CAP = int(os.getenv("MIN_MARKET_CAP", 75000))
# Верхний порог капы (опционально). Не задан → не ограничиваем сверху
MAX_MARKET_CAP = int(os.getenv("MAX_MARKET_CAP")) if os.getenv("MAX_MARKET_CAP") else None
SOLANA_RPC_ENDPOINT = os.getenv("SOLANA_RPC_ENDPOINT", "https://api.mainnet-beta.solana.com")
SIMPLE_TX_FEED = os.getenv("SIMPLE_TX_FEED", "0") == "1"  # Optional per-tx debug feed
# Включение подробного дебага
DEBUG_VERBOSE = os.getenv("DEBUG_VERBOSE", "0") == "1"
# Сколько последних сигнатур обработать при первом заходе (для быстрой проверки конвейера)
BACKFILL_ON_START = int(os.getenv("BACKFILL_ON_START", "0"))
# Ранний алерт до полноценного мульти-сигнала
ENABLE_PREALERT = os.getenv("ENABLE_PREALERT", "1") == "1"
PREALERT_THRESHOLD = int(os.getenv("PREALERT_THRESHOLD", "2"))
# Управление отправкой UPDATE-сообщений
ENABLE_UPDATES = os.getenv("ENABLE_UPDATES", "1") == "1"
DEX_TTL_SECONDS = int(os.getenv("DEX_TTL_SECONDS", "60"))
RATE_LIMIT_SLEEP_SECONDS = float(os.getenv("RATE_LIMIT_SLEEP_SECONDS", "5"))
# NEW: SOL price cache TTL and cache
SOL_PRICE_TTL_SECONDS = int(os.getenv("SOL_PRICE_TTL_SECONDS", "60"))
_sol_price_cache = {"price": 0.0, "ts": 0.0}
# Optional Birdeye API key to avoid 401 responses on some endpoints
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "")
# Discord forwarding controls
DISCORD_ALLOWED_CHAT_IDS = {s.strip() for s in os.getenv("DISCORD_ALLOWED_CHAT_IDS", "").split(',') if s.strip()}
DISCORD_DEDUPE_TTL_SECONDS = int(os.getenv("DISCORD_DEDUPE_TTL_SECONDS", "60"))
_discord_dedupe: dict[str, float] = {}
# Cache cleanup controls
CACHE_CLEANUP_ENABLED = os.getenv("CACHE_CLEANUP_ENABLED", "1") == "1"
CACHE_CLEANUP_TARGETS = [p.strip() for p in os.getenv(
    "CACHE_CLEANUP_TARGETS",
    "~/.cache/ms-playwright,~/.local/share/pyppeteer"
).split(',') if p.strip()]
CACHE_CLEANUP_MAX_AGE_DAYS = int(os.getenv("CACHE_CLEANUP_MAX_AGE_DAYS", "7"))

# (Удалено) Ранее были флаги SHOW_TOTAL_STATS / SHOW_RECENT_EXITS — больше не используются
# Они убраны из кода, чтобы сообщение было короче и стабильнее.
RECENT_EXITS_MAX = int(os.getenv("RECENT_EXITS_MAX", "3"))

# Если модуль SolanaTrackerBot найден — используем его RPC URL
if ST_WALLET_TRACKER is not None:
    try:
        ST_WALLET_TRACKER.url = SOLANA_RPC_ENDPOINT
    except Exception:
        pass

# Multiple detection windows (minutes), earliest window wins
_raw_windows = [w.strip() for w in os.getenv("MULTI_WINDOWS", "1,5,10,30,60").split(',') if w.strip()]
MULTI_WINDOWS_SECONDS = []
for w in _raw_windows:
    try:
        wl = w.lower()
        if wl.endswith('s'):
            MULTI_WINDOWS_SECONDS.append(max(1, int(wl[:-1])))
        elif wl.endswith('m'):
            MULTI_WINDOWS_SECONDS.append(max(1, int(wl[:-1])) * 60)
        else:
            # Backward-compatible: bare numbers are minutes
            MULTI_WINDOWS_SECONDS.append(max(1, int(w)) * 60)
    except ValueError:
        continue
if not MULTI_WINDOWS_SECONDS:
    MULTI_WINDOWS_SECONDS = [max(1, int(TIME_WINDOW_MINUTES)) * 60]
MULTI_WINDOWS_SECONDS = sorted(set(MULTI_WINDOWS_SECONDS))

# Store events up to this lookback horizon (minutes)
# If not provided, derive from the largest detection window (in seconds) but at least 360 minutes.
_derived_lookback_min = int(ceil(max(MULTI_WINDOWS_SECONDS) / 60)) if _raw_windows else TIME_WINDOW_MINUTES
MAX_LOOKBACK_MINUTES = int(os.getenv("MAX_LOOKBACK_MINUTES", str(max(_derived_lookback_min, 360))))

logger = logging.getLogger(__name__)
_token_info_cache = {}

# Утилита для печати отладочного лога
def dlog(message: str) -> None:
    if DEBUG_VERBOSE:
        logger.info(f"[DEBUG] {message}")

# Безопасное извлечение числового значения из uiTokenAmount
def _to_float_token_amount(ui_token_amount: dict | None) -> float:
    if not isinstance(ui_token_amount, dict):
        return 0.0
    for key in ("uiAmountString", "uiAmount", "amount"):
        if key in ui_token_amount and ui_token_amount[key] is not None:
            try:
                return float(ui_token_amount[key])
            except Exception:
                continue
    return 0.0

# При старте модуля зафиксируем ключевые настройки
dlog(f"ST_WALLET_TRACKER loaded: {bool(ST_WALLET_TRACKER)}; RPC={SOLANA_RPC_ENDPOINT}")
dlog(f"MULTI_EVENT_THRESHOLD={MULTI_EVENT_THRESHOLD}, WINDOWS={os.getenv('MULTI_WINDOWS','1,5,10,30,60')}, MIN_CAP={MIN_MARKET_CAP}, MAX_CAP={MAX_MARKET_CAP}")
dlog(f"WINDOWS_SECONDS={MULTI_WINDOWS_SECONDS}")

# --- RPC rate limiting (serialize requests to avoid 429) ---
RPC_SEMAPHORE = asyncio.Semaphore(int(os.getenv("RPC_CONCURRENCY", "2")))
WALLET_CONCURRENCY = int(os.getenv("WALLET_CONCURRENCY", "6"))
RPC_DELAY_SECONDS = float(os.getenv("RPC_DELAY_SECONDS", "0.6"))
RPC_JITTER_MAX = float(os.getenv("RPC_JITTER_MAX", "0.2"))

async def rpc_post(client: httpx.AsyncClient, payload: dict, timeout: float = 30.0):
    async with RPC_SEMAPHORE:
        response = await client.post(SOLANA_RPC_ENDPOINT, json=payload, timeout=timeout)
        # small pacing to be nice to public endpoints + jitter to avoid thundering herd
        delay = max(0.0, RPC_DELAY_SECONDS) + (random.uniform(0, RPC_JITTER_MAX) if RPC_JITTER_MAX > 0 else 0)
        await asyncio.sleep(delay)
        return response

# --- In-memory Stores ---
recent_events = {}
# Structure: recent_events[token_addr] = {
#   'buys': [{'wallet': address, 'amount': float, 'time': datetime, 'name': string}],
#   'sells': [{'wallet': address, 'amount': float, 'time': datetime, 'name': string}]
# }
notified_events = {}
last_signatures = {} # Store last seen signature per wallet

# --- Notification Functions (remains the same) ---
async def send_discord_message(message, chat_id: str | int = None, dedupe_key: str | None = None):
    if not DISCORD_WEBHOOK_URL:
        return
    # Filter by allowed chat ids if set
    try:
        if DISCORD_ALLOWED_CHAT_IDS:
            cid = str(chat_id) if chat_id is not None else None
            if cid is None or cid not in DISCORD_ALLOWED_CHAT_IDS:
                return
    except Exception:
        pass
    # Dedupe
    try:
        if dedupe_key:
            now = perf_counter()
            ts = _discord_dedupe.get(dedupe_key)
            if ts and (now - ts) < max(1, DISCORD_DEDUPE_TTL_SECONDS):
                return
            _discord_dedupe[dedupe_key] = now
    except Exception:
        pass
    try:
        async with httpx.AsyncClient() as client:
            # Convert HTML-ish to Discord-friendly text
            raw = str(message)
            # Replace anchor tags with "Text: URL"
            raw = re.sub(r'<a\s+href=\"([^\"]+)\">([^<]+)</a>', r'\2: \1', raw)
            # Basic sanitization
            discord_message = (
                raw
                .replace('\n\n', '\n')
                .replace('<b>', '**').replace('</b>', '**')
                .replace('<i>', '*').replace('</i>', '*')
                .replace('<code>', '`').replace('</code>', '`')
                .replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
            )
            lines = [l for l in discord_message.split('\n') if l.strip()]
            title = lines[0][:256] if lines else "Multi Event"
            description = "\n".join(lines[1:])[:4000] if len(lines) > 1 else ''
            # Try to set embed.url to Dexscreener link
            dex_url = None
            m = re.search(r'https?://[^\s]*dexscreener\.com/\S+', discord_message)
            if m:
                dex_url = m.group(0)
            payload = {
                "embeds": [
                    {
                        "title": title,
                        "description": description,
                        "color": 0x00C853 if ('Buy' in title or '🔥' in title) else 0xD50000,
                        **({"url": dex_url} if dex_url else {})
                    }
                ]
            }
            # Fallback to content if embeds not allowed
            try:
                resp = await client.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
                if resp.status_code >= 400:
                    await client.post(DISCORD_WEBHOOK_URL, json={"content": discord_message}, timeout=10)
            except Exception:
                await client.post(DISCORD_WEBHOOK_URL, json={"content": discord_message}, timeout=10)
        logger.info("Discord notification sent.")
    except Exception as e:
        logger.error(f"Failed to send Discord message: {e}")


# Helper to format window label nicely (supports seconds/minutes)
def _format_window_label(window_seconds: int) -> str:
    try:
        ws = int(window_seconds or 0)
    except Exception:
        ws = 0
    if ws <= 0:
        return ""
    if ws < 60:
        return f"≤{ws}s"
    m, s = divmod(ws, 60)
    return f"≤{m}m" if s == 0 else f"≤{m}m{s}s"

def format_notification(event_type: str, token_info: dict, participants: list, window_minutes: int, is_update: bool = False, total_participants: list | None = None, recent_exits: list | None = None) -> str:
    is_buy = "Buy" in event_type
    if is_buy:
        title = "📈 <b>Updates Multibuy Wallets</b> 📈" if is_update else "🔥 <b>Multi-Buy Alert</b> 🔥"
    else:
        title = "📉 <b>Updates Multisell Wallets</b> 📉" if is_update else "🚨 <b>Multi-Sell Alert</b> 🚨"
    participant_label = "Buyers" if is_buy else "Sellers"

    action_tag = "BUY" if is_buy else "SELL"
    token_icon = "📈" if is_buy else "📉"

    symbol = html_escape(str(token_info.get('symbol', 'N/A')).strip())
    address = html_escape(str(token_info.get('address', '')).strip())
    market_cap_str = f"${int(token_info.get('market_cap', 0)):,}"
    # Treat the parameter as seconds now
    try:
        _window_seconds = int(window_minutes or 0)
    except Exception:
        _window_seconds = 0
    window_str = f"⏱ <b>Window:</b> {_format_window_label(_window_seconds)}" if _window_seconds else ""

    # Ссылка на Dexscreener: предпочтительно по pair, иначе по mint
    raw_addr = str(token_info.get('address', '')).strip()
    pair_addr = str(token_info.get('pair_address', '')).strip()
    dex_href = f"https://dexscreener.com/solana/{(pair_addr or raw_addr)}"

    # Window-only stats (current participants argument)
    caps = []
    for p in participants:
        try:
            cv = float(p.get('cap') or 0)
        except Exception:
            cv = 0.0
        if cv > 0:
            caps.append(cv)
    cap_line = None
    if caps:
        cmin = int(min(caps))
        cavg = int(sum(caps) / len(caps))
        cmax = int(max(caps))
        label = "Entry caps" if is_buy else "Exit caps"
        cap_line = f"📊 <b>{label}:</b> min ${cmin:,} · avg ${cavg:,} · max ${cmax:,}"

    # (Удалено) Total stats across lookback — больше не выводим
    # (Удалено) Recent exits/opposite-side info — больше не выводим

    lines = [
        f"{title}",
        "",
        window_str,
        f"🔖 <b>Type:</b> {action_tag}",
        f"{token_icon} <b>Token:</b> ${symbol} <code>{address}</code>",
        f"🔗 <a href=\"{dex_href}\">Open on Dexscreener</a>",
        f"🔎 <a href=\"https://kolscan.io/tokens\">Kolscan Tokens</a> · <a href=\"https://kolscan.io/trades\">Kolscan Trades</a> · <a href=\"https://kolscan.io/leaderboard\">Leaderboard</a>",
        f"💰 <b>Market Cap:</b> {html_escape(market_cap_str)}",
        cap_line,
        "",
        f"👛 <b>{participant_label}:</b>",
    ]
    for p in participants:
        amount_str = f"{p['amount']:.2f}"
        name = html_escape(str(p.get('name', '')))
        w = str(p.get('wallet', ''))
        short_addr = f"{w[:4]}...{w[-4:]}" if isinstance(w, str) and len(w) > 8 else w
        short_addr = html_escape(short_addr)
        lines.append(f"– {name} ({short_addr}): {amount_str} SOL")
    return "\n".join([l for l in lines if l is not None and l != ""]) 

async def send_notification(context: ContextTypes.DEFAULT_TYPE, message, chat_id: str):
    # Приведение chat_id к int, если это строка из цифр
    try:
        chat_id_cast = int(chat_id) if isinstance(chat_id, str) and chat_id.isdigit() else chat_id
    except Exception:
        chat_id_cast = chat_id
    try:
        await context.bot.send_message(chat_id=chat_id_cast, text=message, parse_mode='HTML', disable_web_page_preview=True)
        logger.info(f"Telegram notification sent to chat_id: {chat_id_cast} (HTML)")
    except Exception as e:
        logger.warning(f"HTML send failed, retrying MarkdownV2. Err: {e}")
        try:
            await context.bot.send_message(chat_id=chat_id_cast, text=message, parse_mode='MarkdownV2')
            logger.info(f"Telegram notification (MarkdownV2) sent to chat_id: {chat_id_cast}")
        except Exception as e2:
            logger.warning(f"MarkdownV2 failed, retrying plain text. Err: {e2}")
            try:
                await context.bot.send_message(chat_id=chat_id_cast, text=message)
                logger.info(f"Telegram notification (plain) sent to chat_id: {chat_id_cast}")
            except Exception as e3:
                logger.error(f"Failed to send Telegram message to {chat_id_cast}: {e3}")
    # Discord forward with dedupe
    try:
        key = f"{chat_id}|" + (message.split('\n',1)[0] if isinstance(message, str) else "")
        await send_discord_message(message, chat_id=str(chat_id_cast), dedupe_key=key)
    except Exception:
        pass

# --- Data Fetching & Analysis ---
async def get_token_info(token_address):
    # TTL cache to reduce Dexscreener/load; do NOT return cached zeros
    try:
        cached = _token_info_cache.get(token_address)
        if cached:
            ts = cached.get('ts', 0)
            data = cached.get('data') or {}
            if (perf_counter() - ts) < DEX_TTL_SECONDS and float(data.get('market_cap') or 0) > 0:
                return data
    except Exception:
        pass

    tokens_url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
    pairs_url = f"https://api.dexscreener.com/latest/dex/pairs/solana/{token_address}"

    default_headers = {"User-Agent": "multibuybot/1.0", "Accept": "application/json"}

    async def _fetch(url: str):
        async with httpx.AsyncClient(headers=default_headers) as client:
            resp = await client.get(url, timeout=10)
            dlog(f"DexScreener status={resp.status_code} for token={token_address} url={url}")
            if resp.status_code != 200:
                return None
            try:
                return resp.json()
            except Exception:
                return None

    def _pick_best_pair(pairs: list):
        if not pairs:
            return None
        def score(p):
            liq = 0.0
            try:
                liq = float((p.get('liquidity') or {}).get('usd') or 0)
            except Exception:
                liq = 0.0
            fdv = 0.0
            try:
                fdv = float(p.get('fdv') or 0)
            except Exception:
                fdv = 0.0
            return (liq, fdv)
        pairs_sorted = sorted(pairs, key=score, reverse=True)
        return pairs_sorted[0]

    # RPC fallback for token supply
    async def _get_token_supply_local(mint_address: str) -> float:
        try:
            async with httpx.AsyncClient(headers=default_headers) as client:
                payload = {"jsonrpc": "2.0", "id": 1, "method": "getTokenSupply", "params": [mint_address]}
                resp = await rpc_post(client, payload, timeout=15.0)
            body = resp.json() if resp is not None else {}
            value = (body.get('result') or {}).get('value') or {}
            amount_raw = value.get('amount', '0')
            decimals = int(value.get('decimals', 0) or 0)
            amount_float = float(amount_raw or 0)
            denom = float(10 ** max(decimals, 0))
            supply = amount_float / denom if denom > 0 else 0.0
            dlog(f"getTokenSupply {mint_address} -> {supply}")
            return supply
        except Exception as e:
            dlog(f"getTokenSupply failed token={mint_address} err={e}")
            return 0.0

    # 1) Try /tokens
    data = await _fetch(tokens_url)
    if not data or not data.get('pairs'):
        # Try chain-qualified tokens endpoint as well
        data = await _fetch(f"https://api.dexscreener.com/latest/dex/tokens/solana/{token_address}")
    if not data or not data.get('pairs'):
        # 2) fallback /pairs
        data = await _fetch(pairs_url)

    symbol = 'N/A'
    address = token_address
    market_cap = 0.0
    price_usd = 0.0
    dex_mc = 0.0
    fdv = 0.0
    supply_used = 0.0
    pair_address = ''

    if data and data.get('pairs'):
        best = _pick_best_pair(data['pairs'])
        if best:
            base = best.get('baseToken') or {}
            symbol = base.get('symbol') or 'N/A'
            address = base.get('address') or token_address
            pair_address = str(best.get('pairAddress') or '')
            # MC/FDV from Dexscreener
            try:
                dex_mc = float(best.get('marketCap') or 0)
            except Exception:
                dex_mc = 0.0
            try:
                fdv = float(best.get('fdv') or 0)
            except Exception:
                fdv = 0.0
            # priceUsd or priceNative * SOL
            try:
                if best.get('priceUsd'):
                    price_usd = float(best.get('priceUsd'))
                elif best.get('priceNative'):
                    sol_price = await _get_sol_price_usd()
                    price_usd = float(best.get('priceNative')) * sol_price if sol_price > 0 else 0.0
            except Exception:
                price_usd = 0.0

            market_cap = dex_mc
            if market_cap <= 0 and fdv > 0:
                market_cap = fdv
            # If still no MC, try price * supply (first with Dex price, later with Birdeye/Jupiter fallback)
            if market_cap <= 0 and price_usd > 0:
                mint_addr = address
                supply_used = await _get_token_supply_local(mint_addr)
                if supply_used > 0:
                    market_cap = price_usd * supply_used
                    dlog(f"fallback MC via price*supply: price={price_usd}, supply={supply_used}, mc={market_cap}")

    # Extra fallback: try Birdeye (then Jupiter) price if we still have no cap or no pairs
    if market_cap <= 0:
        mint_for_price = address or token_address
        birdeye_price = 0.0
        if BIRDEYE_API_KEY:
            try:
                birdeye_headers = {**default_headers, "x-chain": "solana", "X-API-KEY": BIRDEYE_API_KEY}
                async with httpx.AsyncClient(headers=birdeye_headers) as client:
                    r = await client.get(
                        f"https://public-api.birdeye.so/defi/price?address={mint_for_price}",
                        timeout=10
                    )
                if r.status_code == 200:
                    birdeye_price = float((r.json() or {}).get("data", {}).get("value", 0) or 0)
                    dlog(f"Birdeye price for {mint_for_price} -> {birdeye_price}")
            except Exception as e:
                dlog(f"Birdeye price fetch failed token={mint_for_price} err={e}")
                birdeye_price = 0.0
        fallback_price = birdeye_price
        # Jupiter final fallback
        if fallback_price <= 0:
            try:
                async with httpx.AsyncClient(headers=default_headers) as client:
                    rj = await client.get(
                        f"https://price.jup.ag/v4/price?ids={mint_for_price}", timeout=10
                    )
                if rj.status_code == 200:
                    j = rj.json() or {}
                    fp = float(((j.get('data') or {}).get(mint_for_price) or {}).get('price') or 0)
                    if fp > 0:
                        fallback_price = fp
                        dlog(f"Jupiter price for {mint_for_price} -> {fallback_price}")
            except Exception as e:
                dlog(f"Jupiter price fetch failed token={mint_for_price} err={e}")
        if fallback_price > 0:
            if supply_used <= 0:
                supply_used = await _get_token_supply_local(mint_for_price)
            if supply_used > 0:
                market_cap = fallback_price * supply_used
                if price_usd <= 0:
                    price_usd = fallback_price
                dlog(f"[MC Fallback External] price={fallback_price}, supply={supply_used}, mc={market_cap}")

    dlog(f"[MC] token={token_address} symbol={symbol} dex_mc={dex_mc} fdv={fdv} price_usd={price_usd} supply={supply_used} final_mc={market_cap}")

    result = {
        "market_cap": float(market_cap or 0),
        "symbol": symbol,
        "address": address,
        "pair_address": pair_address,
    }

    # Cache only positive MC
    try:
        if result["market_cap"] > 0:
            _token_info_cache[token_address] = {"ts": perf_counter(), "data": result}
    except Exception:
        pass
    return result

async def _get_sol_price_usd() -> float:
    now = perf_counter()
    try:
        if _sol_price_cache.get("ts", 0) and (now - _sol_price_cache["ts"]) < SOL_PRICE_TTL_SECONDS:
            return float(_sol_price_cache.get("price", 0.0))
    except Exception:
        pass
    price = 0.0
    # Try Dexscreener price for SOL mint
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get("https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112", timeout=10)
            if r.status_code == 200:
                data = r.json() or {}
                pairs = data.get("pairs") or []
                if pairs:
                    p0 = pairs[0]
                    v = p0.get("priceUsd")
                    if v:
                        price = float(v)
    except Exception:
        price = 0.0
    # Fallback: Birdeye
    if price <= 0 and BIRDEYE_API_KEY:
        try:
            headers = {"x-chain": "solana", "X-API-KEY": BIRDEYE_API_KEY}
            async with httpx.AsyncClient() as client:
                r = await client.get(
                    "https://public-api.birdeye.so/defi/price?address=So11111111111111111111111111111111111111112",
                    headers=headers, timeout=10
                )
                if r.status_code == 200:
                    price = float((r.json() or {}).get("data", {}).get("value", 0) or 0)
        except Exception:
            price = 0.0
    try:
        _sol_price_cache["price"] = float(price or 0.0)
        _sol_price_cache["ts"] = now
    except Exception:
        pass
    return float(price or 0.0)

# Helper to check market cap bounds
def _cap_ok(market_cap: float) -> bool:
    if market_cap < MIN_MARKET_CAP:
        dlog(f"cap {market_cap} < MIN_CAP {MIN_MARKET_CAP}: skip")
        return False
    if MAX_MARKET_CAP is not None and market_cap > MAX_MARKET_CAP:
        dlog(f"cap {market_cap} > MAX_CAP {MAX_MARKET_CAP}: skip")
        return False
    return True

async def analyze_and_store_transactions(wallets_to_track):
    """
    Fetches latest transactions wallet by wallet with delays, mimicking the original SolanaTrackerBot
    to ensure maximum reliability and avoid rate limits.
    """
    async with httpx.AsyncClient() as client:
        for wallet in wallets_to_track:
            try:
                dlog(f"Analyze wallet={wallet['name']} {wallet['address']}")
                # 1. Get latest signature for the wallet
                payload = {
                    "jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress",
                    "params": [wallet['address'], {"limit": 1}] # Only need the most recent one
                }
                response = await rpc_post(client, payload, timeout=20)
                dlog(f"getSignaturesForAddress status={response.status_code} wallet={wallet['name']}")
                
                # Gently handle 429 errors by just skipping this cycle for this wallet
                if response.status_code == 429:
                    logger.warning(f"Rate limited for {wallet['name']}. Skipping this cycle.")
                    await asyncio.sleep(2) # Extra wait time
                    continue
                response.raise_for_status()

                body = {}
                try:
                    body = response.json()
                except Exception:
                    body = {}
                # Fallback to legacy method if needed
                if isinstance(body.get('error'), dict) and 'method not found' in str(body['error'].get('message','')).lower():
                    payload = {
                        "jsonrpc": "2.0", "id": 1, "method": "getConfirmedSignaturesForAddress2",
                        "params": [wallet['address'], {"limit": 1}]
                    }
                    response = await rpc_post(client, payload, timeout=20)
                    if response.status_code == 429:
                        logger.warning(f"Rate limited (fallback) for {wallet['name']}. Skipping.")
                        await asyncio.sleep(2)
                        continue
                    response.raise_for_status()
                    body = response.json()

                signatures_data = body.get('result', [])
                dlog(f"signatures count={len(signatures_data)} wallet={wallet['name']}")
                if not signatures_data:
                    continue

                latest_signature = signatures_data[0]['signature']
                if last_signatures.get(wallet['address']) == latest_signature:
                    continue # No new transactions
                last_signatures[wallet['address']] = latest_signature

                # 2. Get the full transaction details
                tx_payload = {
                    "jsonrpc": "2.0", "id": 1, "method": "getTransaction",
                    "params": [latest_signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
                }
                tx_data = None
                # 2a) Попробовать через SolanaTrackerBot (TTL cache)
                if ST_WALLET_TRACKER is not None:
                    try:
                        details = await ST_WALLET_TRACKER.get_transaction_details(latest_signature)  # type: ignore
                        if isinstance(details, dict):
                            tx_data = details.get('result')
                    except Exception as e:
                        logger.warning(f"ST_WALLET_TRACKER.get_transaction_details failed: {e}")
                # 2b) Фолбэк: прямой RPC
                if tx_data is None:
                    tx_response = await rpc_post(client, tx_payload, timeout=20)
                    dlog(f"getTransaction status={tx_response.status_code} sig={latest_signature}")
                    if tx_response.status_code == 429:
                        logger.warning(f"Rate limited getting tx details for {wallet['name']}. Skipping.")
                        await asyncio.sleep(2)
                        continue
                    tx_response.raise_for_status()
                    tx_data = tx_response.json().get('result')
                if not tx_data: continue

                # 3. Process the transaction (use wallet index for SOL change)
                pre_balances = tx_data.get("meta", {}).get("preTokenBalances", [])
                post_balances = tx_data.get("meta", {}).get("postTokenBalances", [])
                changes = {}
                for balance in pre_balances:
                    if balance.get('owner') == wallet['address']:
                        addr = balance.get('mint')
                        changes[addr] = changes.get(addr, 0) - _to_float_token_amount(balance.get('uiTokenAmount'))
                for balance in post_balances:
                    if balance.get('owner') == wallet['address']:
                        addr = balance.get('mint')
                        changes[addr] = changes.get(addr, 0) + _to_float_token_amount(balance.get('uiTokenAmount'))
                # Compute SOL delta for this wallet to classify buy/sell
                meta = tx_data.get('meta', {})
                account_keys = tx_data.get('transaction', {}).get('message', {}).get('accountKeys', [])
                pubkeys = [k.get('pubkey') if isinstance(k, dict) else k for k in account_keys]
                sol_change = 0.0
                if pubkeys and wallet['address'] in pubkeys:
                    idx = pubkeys.index(wallet['address'])
                    try:
                        sol_change = (meta.get('postBalances', [0]*len(pubkeys))[idx] - meta.get('preBalances', [0]*len(pubkeys))[idx]) / 1e9
                    except Exception:
                        sol_change = 0.0
                dlog(f"tx sig={latest_signature} sol_change={sol_change}")
                event_time = datetime.fromtimestamp(tx_data.get('blockTime'), tz=timezone.utc)
                if datetime.now(timezone.utc) - event_time > timedelta(minutes=MAX_LOOKBACK_MINUTES):
                    continue
                if sol_change < 0:
                    for token_addr, change in changes.items():
                        if change > 0 and token_addr != "So11111111111111111111111111111111111111112":
                            recent_events.setdefault(token_addr, {"buys": [], "sells": []})
                            if not any(e['wallet'] == wallet['address'] for e in recent_events[token_addr]['buys']):
                                logger.info(f"New BUY: {wallet['name']} bought {token_addr}")
                                # Снимок капы на момент события
                                try:
                                    token_info_snapshot = await get_token_info(token_addr)
                                    cap_snapshot = token_info_snapshot.get('market_cap') if token_info_snapshot else None
                                except Exception:
                                    cap_snapshot = None
                                recent_events[token_addr]['buys'].append({"wallet": wallet['address'], "amount": abs(sol_change), "time": event_time, "name": wallet['name'], "cap": cap_snapshot})
                elif sol_change > 0:
                    for token_addr, change in changes.items():
                        if change < 0 and token_addr != "So11111111111111111111111111111111111111112":
                            recent_events.setdefault(token_addr, {"buys": [], "sells": []})
                            if not any(e['wallet'] == wallet['address'] for e in recent_events[token_addr]['sells']):
                                logger.info(f"New SELL: {wallet['name']} sold {token_addr}")
                                try:
                                    token_info_snapshot = await get_token_info(token_addr)
                                    cap_snapshot = token_info_snapshot.get('market_cap') if token_info_snapshot else None
                                except Exception:
                                    cap_snapshot = None
                                recent_events[token_addr]['sells'].append({"wallet": wallet['address'], "amount": sol_change, "time": event_time, "name": wallet['name'], "cap": cap_snapshot})
            except httpx.HTTPStatusError as e:
                logger.warning(f"HTTP error for {wallet['name']}: {e}") # Log as warning, don't crash
            except Exception as e:
                logger.error(f"Error processing wallet {wallet['name']}: {e}", exc_info=True)
            
            await asyncio.sleep(1) # Small delay between each wallet to be respectful to the API

async def clean_old_events():
    now = datetime.now(timezone.utc)
    retention = timedelta(minutes=MAX_LOOKBACK_MINUTES)
    for token_addr, events in list(recent_events.items()):
        events['buys'] = [e for e in events['buys'] if now - e['time'] <= retention]
        events['sells'] = [e for e in events['sells'] if now - e['time'] <= retention]
        if not events['buys'] and not events['sells']:
            del recent_events[token_addr]

async def check_for_multi_events(context: ContextTypes.DEFAULT_TYPE, chat_id: str):
    now = datetime.now(timezone.utc)
    windows_sorted = MULTI_WINDOWS_SECONDS
    for token_addr, events in list(recent_events.items()):
        # Prepare notification state for this token
        state = notified_events.setdefault(token_addr, {
            'buy': {'wallets': set(), 'windows': set(), 'prealert': False},
            'sell': {'wallets': set(), 'windows': set(), 'prealert': False},
        })

        # Helper to handle one side (buy or sell)
        for side_key in ('buys', 'sells'):
            side_label = 'buy' if side_key == 'buys' else 'sell'
            participants_all = [e for e in events.get(side_key, []) if now - e['time'] <= timedelta(minutes=MAX_LOOKBACK_MINUTES)]
            wallets_all = {p['wallet'] for p in participants_all}
            dlog(f"token={token_addr} side={side_label} total={len(wallets_all)} within lookback")

            # Pre-alert: ранний сигнал при достижении 2+ уникальных кошельков (по умолчанию)
            if ENABLE_PREALERT and not state[side_label]['windows'] and not state[side_label].get('prealert', False):
                for w in windows_sorted:
                    window_participants = [p for p in events.get(side_key, []) if now - p['time'] <= timedelta(seconds=w)]
                    unique_wallets = {p['wallet'] for p in window_participants}
                    if len(unique_wallets) >= PREALERT_THRESHOLD:
                        try:
                            token_info = await get_token_info(token_addr)
                        except Exception:
                            token_info = {"market_cap": 0.0, "symbol": "N/A", "address": token_addr}
                        # Opposite side in lookback for context
                        opposite = 'sells' if side_key == 'buys' else 'buys'
                        recent_exits = [p for p in events.get(opposite, []) if now - p['time'] <= timedelta(minutes=MAX_LOOKBACK_MINUTES)]
                        msg = format_notification(
                            f"{side_label.title()} PRE-ALERT", token_info, window_participants, w, is_update=False,
                            total_participants=[p for p in events.get(side_key, []) if now - p['time'] <= timedelta(minutes=MAX_LOOKBACK_MINUTES)],
                            recent_exits=recent_exits
                        )
                        await send_notification(context, msg, chat_id)
                        state[side_label]['prealert'] = True
                        break

            # Updates: new wallets joined after initial alert
            if ENABLE_UPDATES and state[side_label]['windows']:
                new_wallets = wallets_all - state[side_label]['wallets']
                if new_wallets:
                    token_info = await get_token_info(token_addr)
                    if token_info and _cap_ok(token_info.get('market_cap', 0)):
                        new_participants = [p for p in participants_all if p['wallet'] in new_wallets]
                        # recent exits: opposite side in lookback
                        opposite = 'sells' if side_key == 'buys' else 'buys'
                        recent_exits = [p for p in events.get(opposite, []) if now - p['time'] <= timedelta(minutes=MAX_LOOKBACK_MINUTES)]
                        msg = format_notification(
                            f"{side_label.title()} UPDATE", token_info, new_participants, min(windows_sorted), is_update=True,
                            total_participants=participants_all, recent_exits=recent_exits
                        )
                        await send_notification(context, msg, chat_id)
                        state[side_label]['wallets'].update(new_wallets)

            # Initial detection: earliest window only
            if not state[side_label]['windows']:
                for w in windows_sorted:
                    window_participants = [p for p in events.get(side_key, []) if now - p['time'] <= timedelta(seconds=w)]
                    unique_wallets = {p['wallet'] for p in window_participants}
                    dlog(f"[WINDOW] token={token_addr} side={side_label} w={w} unique={len(unique_wallets)}")
                    if len(unique_wallets) >= MULTI_EVENT_THRESHOLD:
                        token_info = await get_token_info(token_addr)
                        if token_info and _cap_ok(token_info.get('market_cap', 0)):
                            # recent exits: opposite side in lookback
                            opposite = 'sells' if side_key == 'buys' else 'buys'
                            recent_exits = [p for p in events.get(opposite, []) if now - p['time'] <= timedelta(minutes=MAX_LOOKBACK_MINUTES)]
                            msg = format_notification(
                                side_label.title(), token_info, window_participants, w, is_update=False,
                                total_participants=[p for p in events.get(side_key, []) if now - p['time'] <= timedelta(minutes=MAX_LOOKBACK_MINUTES)],
                                recent_exits=recent_exits
                            )
                            await send_notification(context, msg, chat_id)
                            state[side_label]['wallets'].update(unique_wallets)
                            state[side_label]['windows'].add(w)
                            break  # earliest window wins

# --- Simple feed helpers (like SolanaTrackerBot) ---
def build_simple_tx_message(wallet_name: str, signature: str, tx_data: dict, event_time: datetime) -> str:
    lines = [
        f"Wallet: {wallet_name}",
        f"Signature: {signature}",
        f"Transaction Time: {event_time.isoformat()}"
    ]
    try:
        instructions = tx_data.get('transaction', {}).get('message', {}).get('instructions', [])
        found = False
        for ins in instructions:
            parsed = ins.get('parsed') if isinstance(ins, dict) else None
            if not parsed: continue
            txn_type = parsed.get('type')
            info = parsed.get('info', {})
            if txn_type == 'transfer' and 'lamports' in info:
                lines.append(f"Type: transfer")
                lines.append(f"From: {info.get('source')}")
                lines.append(f"To: {info.get('destination')}")
                try:
                    sol = float(info.get('lamports', 0)) / 1e9
                    lines.append(f"Amount: {sol:.6f} SOL")
                except Exception:
                    pass
                found = True
                break
        return "\n".join(lines) if found else "\n".join(lines)
    except Exception:
        return "\n".join(lines)

async def sequential_tracker(chat_id: str, application):
    """
    Параллельная обработка кошельков батчами с ограничением по concurrency,
    чтобы ускорить сканирование и сохранить контроль над rate-limit.
    """
    # Per-chat last seen signatures map
    if not hasattr(application, "_runtime_last_sigs_by_chat"):
        application._runtime_last_sigs_by_chat = {}
    last_sigs_by_wallet = application._runtime_last_sigs_by_chat.setdefault(str(chat_id), {})

    while True:
        try:
            user_session_data = application.user_data[int(chat_id)]
            wallets_to_track = [w for w in user_session_data.get('wallets', []) if w.get('is_tracking')]
            if not wallets_to_track:
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                continue

            async with httpx.AsyncClient() as client:
                cycle_started = perf_counter()
                scanned_total = 0

                sem = asyncio.Semaphore(max(1, WALLET_CONCURRENCY))

                async def process_wallet(wallet: dict):
                    nonlocal scanned_total
                    wallet_address = wallet['address']
                    wallet_name = wallet['name']
                    try:
                        async with sem:
                            # 1) Get last 10 signatures (with method fallback if needed)
                            payload = {
                                "jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress",
                                "params": [wallet_address, {"limit": 10}]
                            }
                            dlog(f"seq:getSignatures wallet={wallet_name}")
                            response = await rpc_post(client, payload, timeout=30.0)
                            body = {}
                            try:
                                body = response.json()
                            except Exception:
                                body = {}
                            if isinstance(body.get('error'), dict) and 'method not found' in str(body['error'].get('message','')).lower():
                                payload = {
                                    "jsonrpc": "2.0", "id": 1, "method": "getConfirmedSignaturesForAddress2",
                                    "params": [wallet_address, {"limit": 10}]
                                }
                                response = await rpc_post(client, payload, timeout=30.0)

                            if response.status_code == 429:
                                logger.warning(f"Rate limited on getSignatures for {wallet_name}, sleeping for {RATE_LIMIT_SLEEP_SECONDS}s.")
                                await asyncio.sleep(RATE_LIMIT_SLEEP_SECONDS)
                                return
                            response.raise_for_status()

                            result = response.json().get('result', [])
                            if not result:
                                await asyncio.sleep(0.2)
                                scanned_total += 1
                                return

                            current_signatures = [item['signature'] for item in result]
                            last_seen_signatures = last_sigs_by_wallet.get(wallet_address)
                            if not last_seen_signatures:
                                # Первичная инициализация. По желанию обработаем последние N сигнатур как "новые"
                                if BACKFILL_ON_START > 0:
                                    new_signatures = current_signatures[:BACKFILL_ON_START]
                                    if new_signatures:
                                        logger.info(f"Backfill {len(new_signatures)} tx for {wallet_name}.")
                                        for signature in reversed(new_signatures):
                                            tx_payload = {
                                                "jsonrpc": "2.0", "id": 1, "method": "getTransaction",
                                                "params": [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
                                            }
                                            tx_data = None
                                            # 1) Попробовать через SolanaTrackerBot, если доступен
                                            if ST_WALLET_TRACKER is not None:
                                                try:
                                                    details = await ST_WALLET_TRACKER.get_transaction_details(signature)  # type: ignore
                                                    if isinstance(details, dict):
                                                        tx_data = details.get('result')
                                                except Exception as e:
                                                    logger.warning(f"ST_WALLET_TRACKER.get_transaction_details(backfill) failed: {e}")
                                            # 2) Фолбэк: прямой RPC
                                            if tx_data is None:
                                                tx_response = await rpc_post(client, tx_payload, timeout=30.0)
                                                if tx_response.status_code == 429:
                                                    logger.warning(f"Rate limited on getTransaction(backfill) for {wallet_name}, sleeping for {RATE_LIMIT_SLEEP_SECONDS}s.")
                                                    await asyncio.sleep(RATE_LIMIT_SLEEP_SECONDS)
                                                    continue
                                                tx_data = tx_response.json().get('result')
                                            if not tx_data:
                                                continue
                                            # Повторно используем обработку ниже — укороченная версия
                                            try:
                                                pre_balances = tx_data.get("meta", {}).get("preTokenBalances", [])
                                                post_balances = tx_data.get("meta", {}).get("postTokenBalances", [])
                                                changes = {}
                                                for balance in pre_balances:
                                                    if balance.get('owner') == wallet_address:
                                                        addr = balance.get('mint')
                                                        changes[addr] = changes.get(addr, 0) - _to_float_token_amount(balance.get('uiTokenAmount'))
                                                for balance in post_balances:
                                                    if balance.get('owner') == wallet_address:
                                                        addr = balance.get('mint')
                                                        changes[addr] = changes.get(addr, 0) + _to_float_token_amount(balance.get('uiTokenAmount'))
                                                meta = tx_data.get('meta', {})
                                                account_keys = tx_data.get('transaction', {}).get('message', {}).get('accountKeys', [])
                                                pubkeys = [k.get('pubkey') if isinstance(k, dict) else k for k in account_keys]
                                                sol_change = 0.0
                                                if pubkeys and wallet_address in pubkeys:
                                                    idx = pubkeys.index(wallet_address)
                                                    try:
                                                        sol_change = (meta.get('postBalances', [0]*len(pubkeys))[idx] - meta.get('preBalances', [0]*len(pubkeys))[idx]) / 1e9
                                                    except Exception:
                                                        sol_change = 0.0
                                                event_time = datetime.fromtimestamp(tx_data.get('blockTime'), tz=timezone.utc)
                                                if sol_change < -0.001:
                                                    for token_addr, change in changes.items():
                                                        if change > 0 and token_addr not in ["So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"]:
                                                            recent_events.setdefault(token_addr, {"buys": [], "sells": []})
                                                            if not any(e['wallet'] == wallet_address for e in recent_events[token_addr]['buys']):
                                                                try:
                                                                    token_info_snapshot = await get_token_info(token_addr)
                                                                    cap_snapshot = token_info_snapshot.get('market_cap') if token_info_snapshot else None
                                                                except Exception:
                                                                    cap_snapshot = None
                                                                recent_events[token_addr]['buys'].append({"wallet": wallet_address, "amount": abs(sol_change), "time": event_time, "name": wallet_name, "cap": cap_snapshot})
                                                elif sol_change > 0.001:
                                                    for token_addr, change in changes.items():
                                                        if change < 0 and token_addr not in ["So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"]:
                                                            recent_events.setdefault(token_addr, {"buys": [], "sells": []})
                                                            if not any(e['wallet'] == wallet_address for e in recent_events[token_addr]['sells']):
                                                                try:
                                                                    token_info_snapshot = await get_token_info(token_addr)
                                                                    cap_snapshot = token_info_snapshot.get('market_cap') if token_info_snapshot else None
                                                                except Exception:
                                                                    cap_snapshot = None
                                                                recent_events[token_addr]['sells'].append({"wallet": wallet_address, "amount": sol_change, "time": event_time, "name": wallet_name, "cap": cap_snapshot})
                                            except Exception:
                                                pass
                                    # Зафиксировать текущее состояние как "последнее"
                                    last_sigs_by_wallet[wallet_address] = current_signatures
                            else:
                                new_signatures = [sig for sig in current_signatures if sig not in last_seen_signatures]
                                if new_signatures:
                                    logger.info(f"Found {len(new_signatures)} new transaction(s) for {wallet_name}.")
                                    for signature in reversed(new_signatures):
                                        tx_payload = {
                                            "jsonrpc": "2.0", "id": 1, "method": "getTransaction",
                                            "params": [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
                                        }
                                        tx_data = None
                                        # 1) Если доступен модуль SolanaTrackerBot — используем его функцию
                                        if ST_WALLET_TRACKER is not None:
                                            try:
                                                details = await ST_WALLET_TRACKER.get_transaction_details(signature)  # type: ignore
                                                if isinstance(details, dict):
                                                    tx_data = details.get('result')
                                            except Exception as e:
                                                logger.warning(f"ST_WALLET_TRACKER.get_transaction_details failed: {e}")
                                        # 2) Фолбэк на прямой RPC, если по какой-то причине не сработало
                                        if tx_data is None:
                                            tx_response = await rpc_post(client, tx_payload, timeout=30.0)
                                            if tx_response.status_code == 429:
                                                logger.warning(f"Rate limited on getTransaction for {wallet_name}, sleeping for {RATE_LIMIT_SLEEP_SECONDS}s.")
                                                await asyncio.sleep(RATE_LIMIT_SLEEP_SECONDS)
                                                continue
                                            tx_data = tx_response.json().get('result')
                                        if not tx_data:
                                            continue
                                        # Process
                                        try:
                                            pre_balances = tx_data.get("meta", {}).get("preTokenBalances", [])
                                            post_balances = tx_data.get("meta", {}).get("postTokenBalances", [])
                                            changes = {}
                                            for balance in pre_balances:
                                                if balance.get('owner') == wallet_address:
                                                    addr = balance.get('mint')
                                                    changes[addr] = changes.get(addr, 0) - _to_float_token_amount(balance.get('uiTokenAmount'))
                                            for balance in post_balances:
                                                if balance.get('owner') == wallet_address:
                                                    addr = balance.get('mint')
                                                    changes[addr] = changes.get(addr, 0) + _to_float_token_amount(balance.get('uiTokenAmount'))

                                            meta = tx_data.get('meta', {})
                                            account_keys = tx_data.get('transaction', {}).get('message', {}).get('accountKeys', [])
                                            pubkeys = [k.get('pubkey') if isinstance(k, dict) else k for k in account_keys]
                                            sol_change = 0.0
                                            if pubkeys and wallet_address in pubkeys:
                                                idx = pubkeys.index(wallet_address)
                                                try:
                                                    sol_change = (meta.get('postBalances', [0]*len(pubkeys))[idx] - meta.get('preBalances', [0]*len(pubkeys))[idx]) / 1e9
                                                except Exception:
                                                    sol_change = 0.0
                                            event_time = datetime.fromtimestamp(tx_data.get('blockTime'), tz=timezone.utc)

                                            # Simple per-tx feed (optional, like SolanaTrackerBot)
                                            if SIMPLE_TX_FEED:
                                                try:
                                                    msg = build_simple_tx_message(wallet_name, signature, tx_data, event_time)
                                                    try:
                                                        chat_id_cast = int(chat_id) if isinstance(chat_id, str) and chat_id.isdigit() else chat_id
                                                    except Exception:
                                                        chat_id_cast = chat_id
                                                    await application.bot.send_message(chat_id_cast, msg)
                                                except Exception as e:
                                                    logger.warning(f"Failed to send simple feed message: {e}")

                                            if sol_change < -0.001: # Buy
                                                for token_addr, change in changes.items():
                                                    if change > 0 and token_addr not in ["So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"]:
                                                        recent_events.setdefault(token_addr, {"buys": [], "sells": []})
                                                        if not any(e['wallet'] == wallet_address for e in recent_events[token_addr]['buys']):
                                                            logger.info(f"BUY EVENT: {wallet_name} bought {token_addr}")
                                                            try:
                                                                token_info_snapshot = await get_token_info(token_addr)
                                                                cap_snapshot = token_info_snapshot.get('market_cap') if token_info_snapshot else None
                                                            except Exception:
                                                                cap_snapshot = None
                                                            recent_events[token_addr]['buys'].append({"wallet": wallet_address, "amount": abs(sol_change), "time": event_time, "name": wallet_name, "cap": cap_snapshot})
                                            elif sol_change > 0.001: # Sell
                                                for token_addr, change in changes.items():
                                                    if change < 0 and token_addr not in ["So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"]:
                                                        recent_events.setdefault(token_addr, {"buys": [], "sells": []})
                                                        if not any(e['wallet'] == wallet_address for e in recent_events[token_addr]['sells']):
                                                            logger.info(f"SELL EVENT: {wallet_name} sold {token_addr}")
                                                            try:
                                                                token_info_snapshot = await get_token_info(token_addr)
                                                                cap_snapshot = token_info_snapshot.get('market_cap') if token_info_snapshot else None
                                                            except Exception:
                                                                cap_snapshot = None
                                                            recent_events[token_addr]['sells'].append({"wallet": wallet_address, "amount": sol_change, "time": event_time, "name": wallet_name, "cap": cap_snapshot})
                                        except Exception as e:
                                            logger.error(f"Error processing transaction {signature} for {wallet_name}: {e}", exc_info=True)
                                    last_sigs_by_wallet[wallet_address] = current_signatures
                    finally:
                        # small pause between wallets
                        await asyncio.sleep(float(os.getenv("WALLET_SPACING_SECONDS", "0.1")))
                        scanned_total += 1
                
                # Запускаем обработки пачкой с лимитом одновременности
                tasks = [asyncio.create_task(process_wallet(w)) for w in wallets_to_track]
                await asyncio.gather(*tasks, return_exceptions=True)
                elapsed = perf_counter() - cycle_started
                dlog(f"scan cycle chat={chat_id} scanned={scanned_total}/{len(wallets_to_track)} elapsed={elapsed:.1f}s avg_per_wallet={(elapsed/scanned_total) if scanned_total else 0:.2f}s")
        except Exception as e:
            logger.error(f"Unexpected error in sequential_tracker loop for chat {chat_id}: {e}", exc_info=True)
        await asyncio.sleep(POLL_INTERVAL_SECONDS)

async def start_multibuy_tracker(chat_id, application):
    # This is now the single source of truth, no more confusion
    user_session_data = application.user_data[int(chat_id)]
    wallets_to_track = [w for w in user_session_data.get('wallets', []) if w.get('is_tracking')]
    dlog(f"start_multibuy_tracker chat={chat_id} wallets_to_track={len(wallets_to_track)}")
    dlog(f"addresses={[w['address'] for w in wallets_to_track]}")
    
    if not wallets_to_track:
        logger.warning(f"No wallets selected for tracking for chat_id: {chat_id}")
        return

    logger.info(f"Starting multi-buy/sell tracker for {len(wallets_to_track)} wallets for chat {chat_id}.")
    
    # Prepare runtime-only task store on application (not persisted)
    if not hasattr(application, "_runtime_tracking_tasks"):
        application._runtime_tracking_tasks = {}
    runtime_tasks_by_chat = application._runtime_tracking_tasks.setdefault(str(chat_id), {})

    # UI state: keep a persistable map of addresses being tracked (no Task objects)
    ui_tracking = user_session_data.setdefault('tracking_tasks', {})
    for wallet in wallets_to_track:
        ui_tracking[wallet['address']] = True

    # Start the single sequential tracker task (no per-wallet tasks)
    seq_key = f"{chat_id}_sequential_tracker"
    if seq_key not in runtime_tasks_by_chat:
        seq_task = asyncio.create_task(sequential_tracker(str(chat_id), application))
        runtime_tasks_by_chat[seq_key] = seq_task

    # Start the central monitoring task
    monitor_task_key = f"{chat_id}_monitor"
    if monitor_task_key not in runtime_tasks_by_chat:
        monitor_task = asyncio.create_task(monitor_for_multievents(chat_id, application))
        runtime_tasks_by_chat[monitor_task_key] = monitor_task

async def stop_multibuy_tracker(chat_id, context):
    user_session_data = context.application.user_data[int(chat_id)]
    runtime_tasks_by_chat = getattr(context.application, "_runtime_tracking_tasks", {}).get(str(chat_id), {})

    if not runtime_tasks_by_chat:
        logger.info(f"No active tracking tasks to stop for chat_id: {chat_id}.")
        user_session_data['tracking_tasks'] = {}
        return

    logger.info(f"Stopping {len(runtime_tasks_by_chat)} tracking tasks for chat_id: {chat_id}.")
    for task in runtime_tasks_by_chat.values():
        task.cancel()
    
    await asyncio.gather(*runtime_tasks_by_chat.values(), return_exceptions=True)
    
    getattr(context.application, "_runtime_tracking_tasks", {}).pop(str(chat_id), None)
    user_session_data['tracking_tasks'] = {}
    logger.info(f"All tracking tasks for chat {chat_id} have been cancelled.")

WINDOW_CHECK_INTERVAL_SECONDS = int(os.getenv("WINDOW_CHECK_INTERVAL_SECONDS", "5"))

async def monitor_for_multievents(chat_id, application):
    """
    This is the central task that periodically checks the collected events 
    for multi-buy/sell patterns and sends notifications.
    """
    while True:
        await check_for_multi_events(application, str(chat_id))
        await clean_old_events()
        await asyncio.sleep(max(1, WINDOW_CHECK_INTERVAL_SECONDS)) # configurable frequency 

async def cache_cleanup_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Periodically remove old cached browser data to avoid disk fill."""
    if not CACHE_CLEANUP_ENABLED:
        return
    removed_bytes = 0
    now_ts = time.time()
    max_age = CACHE_CLEANUP_MAX_AGE_DAYS * 86400
    for raw in CACHE_CLEANUP_TARGETS:
        try:
            base = os.path.expanduser(os.path.expandvars(raw))
            if not os.path.exists(base):
                continue
            # Walk and remove entries older than threshold
            for root, dirs, files in os.walk(base, topdown=False):
                for name in files:
                    fp = os.path.join(root, name)
                    try:
                        if (now_ts - os.path.getmtime(fp)) > max_age:
                            removed_bytes += os.path.getsize(fp)
                            os.remove(fp)
                    except Exception:
                        pass
                for name in dirs:
                    dp = os.path.join(root, name)
                    try:
                        if not os.listdir(dp) or (now_ts - os.path.getmtime(dp)) > max_age:
                            # remove empty or stale dirs
                            shutil.rmtree(dp, ignore_errors=True)
                    except Exception:
                        pass
        except Exception:
            continue
    if removed_bytes > 0:
        logger.info(f"Cache cleanup removed ~{int(removed_bytes/1024/1024)} MB") 