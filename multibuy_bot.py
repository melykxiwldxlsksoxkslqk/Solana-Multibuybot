# multibuy_bot.py
import os
import time
import asyncio
import logging
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from telegram import Bot
from telegram.constants import ParseMode
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# --- Configuration ---
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')
MORALIS_API_KEY = os.getenv('MORALIS_API_KEY')
AXIOM_REF = os.getenv("AXIOM_REF", "")
AXIOM_BASE_URL = os.getenv("AXIOM_BASE_URL", "https://axiom.trade")
AXIOM_TOKEN_LINK_TEMPLATE = os.getenv("AXIOM_TOKEN_LINK_TEMPLATE", f"{os.getenv('AXIOM_BASE_URL','https://axiom.trade').rstrip('/')}/meme/{{address}}")

MULTI_EVENT_THRESHOLD = 3
TIME_WINDOW_MINUTES = 10
POLL_INTERVAL_SECONDS = 120

# --- Logging Setup ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- In-memory Stores ---
recent_events = {}
notified_events = {}

# --- Notification Functions ---
async def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode=ParseMode.MARKDOWN_V2)
        logger.info("Telegram notification sent.")
    except Exception as e: logger.error(f"Failed to send Telegram message: {e}")

async def send_discord_message(message):
    if not DISCORD_WEBHOOK_URL: return
    try:
        loop = asyncio.get_running_loop()
        discord_message = message.replace('\\_', '_').replace('\\*', '*').replace('\\`', '`')
        # Run synchronous requests in a separate thread to not block the event loop
        await loop.run_in_executor(
            None, 
            lambda: requests.post(DISCORD_WEBHOOK_URL, json={"content": discord_message}).raise_for_status()
        )
        logger.info("Discord notification sent.")
    except requests.exceptions.RequestException as e: logger.error(f"Failed to send Discord message: {e}")

async def send_notification(token_info, participants):
    event_type = token_info.get('event_type', 'buy')
    is_update = token_info.get('is_update', False)
    title = f"📈 *Multi-Buy UPDATE* 📈" if is_update and event_type == "buy" else \
            f"📉 *Multi-Sell UPDATE* 📉" if is_update and event_type == "sell" else \
            f"🔥 *Multi-Buy Alert* 🔥" if event_type == "buy" else \
            f"🚨 *Multi-Sell Alert* 🚨"
    participant_label = "Buyers" if event_type == "buy" else "Sellers"
    
    def escape(text):
        reserved_chars = r'_*[]()~`>#+-=|{}.!'
        for char in reserved_chars: text = str(text).replace(char, f'\\{char}')
        return text

    market_cap_str = f"${int(token_info.get('market_cap', 0)):,}"
    message = (
        f"{title}\n\n"
        f"📈 *Token:* ${escape(token_info.get('symbol', 'N/A'))} `({escape(token_info.get('address'))})`\n"
        f"🔗 [Open on Dexscreener](https://dexscreener.com/solana/{escape(token_info.get('pair_address') or token_info.get('address'))})\n"
        f"💰 *Market Cap:* {escape(market_cap_str)}\n\n"
        f"👛 *{participant_label}:*\n"
    )
    for p in participants:
        message += f"– {escape(p['name'])}: {escape(f'{p['amount']:.2f}')} SOL\n"
        
    await asyncio.gather(
        send_telegram_message(message),
        send_discord_message(message)
    )

# --- Data Fetching and Analysis ---
def get_kolscan_wallets():
    url = "https://kolscan.io/leaderboard"
    wallets = []
    logger.info("Launching browser to scrape kolscan.io...")
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            # Increase timeout because the page can be slow
            page.goto(url, timeout=60000)
            
            # Wait for the table body to be populated, this is crucial
            page.wait_for_selector("table tbody tr", timeout=30000)
            
            # Now that the content is loaded, get the HTML
            content = page.content()
            browser.close()

        soup = BeautifulSoup(content, 'html.parser')
        
        # This selector is now more reliable
        for row in soup.select("table tbody tr"):
            cols = row.select("td")
            if len(cols) > 3:
                name_tag = cols[1].select_one("div > div")
                address_tag = cols[3].select_one("a")
                
                if name_tag and address_tag:
                    name = name_tag.text.strip()
                    address = address_tag['href'].split('/')[-1]
                    wallets.append({"name": name, "address": address})
        
        logger.info(f"Successfully scraped {len(wallets)} wallets from kolscan.")

    except Exception as e:
        logger.error(f"Failed to get KOL wallets using Playwright: {e}")
    
    return wallets

def get_token_info(token_address):
    url = f"https://api.dexscreener.com/latest/dex/pairs/solana/{token_address}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data and data.get('pairs'):
            pair = data['pairs'][0]
            return {
                "market_cap": float(pair.get('fdv', 0)),
                "symbol": pair.get('baseToken', {}).get('symbol', 'N/A'),
                "address": pair.get('baseToken', {}).get('address', token_address),
                "pair_address": pair.get('pairAddress', ''),
            }
    except requests.exceptions.RequestException:
        pass
    return {"market_cap": 0, "symbol": "N/A", "address": token_address, "pair_address": ''}

def analyze_and_store_transactions(wallet):
    url = f"https://solana-gateway.moralis.io/account/mainnet/{wallet['address']}/transactions?limit=5"
    headers = {"accept": "application/json", "X-API-Key": MORALIS_API_KEY}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        # --- Simplified parsing ---
        for tx in response.json():
            # This is a placeholder. Real parsing is complex.
            # We will simulate finding a "buy" of a token if SOL was transferred out.
            if tx['meta']['postBalances'][0] < tx['meta']['preBalances'][0]:
                token_address = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263" # BONK for demo
                event_type = "buy"
                
                if token_address not in recent_events: recent_events[token_address] = {"buys": [], "sells": []}
                
                if not any(e['wallet'] == wallet['address'] for e in recent_events[token_address][f"{event_type}s"]):
                    logger.info(f"New {event_type} event: {wallet['name']} -> {token_address}")
                    recent_events[token_address][f"{event_type}s"].append({"wallet": wallet['address'], "amount": 1, "time": datetime.now(timezone.utc), "name": wallet['name']})
    except Exception as e: logger.error(f"Could not process tx for {wallet['name']}: {e}")

def clean_old_events():
    now = datetime.now(timezone.utc)
    for token, events in list(recent_events.items()):
        events['buys'] = [b for b in events['buys'] if now - b['time'] < timedelta(minutes=TIME_WINDOW_MINUTES)]
        events['sells'] = [s for s in events['sells'] if now - s['time'] < timedelta(minutes=TIME_WINDOW_MINUTES)]
        if not events['buys'] and not events['sells']: del recent_events[token]

def check_for_multi_events():
    for token, events in recent_events.items():
        for event_type in ["buys", "sells"]:
            participants = events[event_type]
            if len(participants) >= MULTI_EVENT_THRESHOLD:
                participant_wallets = tuple(sorted([p['wallet'] for p in participants]))
                event_id = (token, event_type, participant_wallets)
                if event_id in notified_events: continue

                is_update = any(p[0]==token and p[1]==event_type and len(participant_wallets)>d['count'] for p,d in list(notified_events.items()))
                
                logger.info(f"--- Multi-{event_type[:-1]} event confirmed for {token} ---")
                token_info = get_token_info(token)
                token_info['event_type'] = "buy" if event_type == "buys" else "sell"
                token_info['is_update'] = is_update
                
                asyncio.run(send_notification(token_info, participants)) # Run the async function in a thread
                notified_events[event_id] = {"count": len(participants)}

# --- Main Loop ---
async def main_tracker_loop():
    logger.info("Starting multi-event tracker...")
    if not MORALIS_API_KEY:
        logger.error("MORALIS_API_KEY not set. Exiting.")
        return
    
    await send_telegram_message("🤖 *Multi\\-Buy Bot Started*\n\nI am now live and will start tracking wallets\\.")

    while True:
        logger.info("--- Starting new tracking cycle ---")
        clean_old_events()
        wallets = get_kolscan_wallets()
        if wallets:
            for wallet in wallets:
                analyze_and_store_transactions(wallet)
                await asyncio.sleep(2)
        check_for_multi_events()
        logger.info(f"--- Cycle complete. Waiting {POLL_INTERVAL_SECONDS} seconds. ---")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)

if __name__ == '__main__':
    asyncio.run(main_tracker_loop()) 