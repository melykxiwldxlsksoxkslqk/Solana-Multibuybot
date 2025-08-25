# helpers/menu_handlers.py
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import json
import os
import logging
from telegram.error import BadRequest
import time

from kolscan import get_kolscan_wallets
from helpers.multibuy_logic import start_multibuy_tracker, stop_multibuy_tracker

logger = logging.getLogger(__name__)

# Общая блокировка, чтобы автообновление не запускалось параллельно
KOL_REFRESH_LOCK = asyncio.Lock()

USER_DATA_FILE = "user_data.json"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = str(update.effective_chat.id)
    # Use context.user_data for session state management
    context.user_data.setdefault('wallets', [])
    context.user_data.setdefault('is_adding_wallet', False)
    # Ничего не обновляем автоматически при старте, только показываем меню
    await show_main_menu(update, context)

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, message: str = "Select an option:") -> None:
    # Use context.user_data for checking state
    try:
        chat_id_str = str(update.effective_chat.id)
    except Exception:
        chat_id_str = None

    # Синхронизация с реальным состоянием фоновых задач
    runtime_tasks = {}
    try:
        runtime_tasks = getattr(context.application, "_runtime_tracking_tasks", {}).get(chat_id_str or "", {})
    except Exception:
        runtime_tasks = {}

    # Определяем состояние трекинга: либо реальные задачи, либо флаг UI (который ставим сразу при старте)
    user_flag = bool(context.user_data.get('tracking_tasks'))
    is_tracking = bool(runtime_tasks) or user_flag

    # Добавим строку о последнем обновлении KOL
    last_ts = None
    try:
        last_ts = context.application.bot_data.get('kol_last_refresh_ts')
    except Exception:
        last_ts = None
    last_line = ""
    if isinstance(last_ts, (int, float)) and last_ts:
        try:
            from datetime import datetime, timezone
            dt = datetime.fromtimestamp(int(last_ts), tz=timezone.utc)
            last_line = f"\n\nLast KOL refresh: {dt.strftime('%Y-%m-%d %H:%M:%S')} UTC"
        except Exception:
            pass

    keyboard = [
        [InlineKeyboardButton("Add Wallet Manually", callback_data='add_wallet')],
        [InlineKeyboardButton("Load KOL Wallets", callback_data='load_kols')],
        [InlineKeyboardButton("View & Select Wallets", callback_data='view_wallets:0')],
        [InlineKeyboardButton(
            "🛑 Stop Tracking" if is_tracking else "▶️ Start Tracking",
            callback_data='stop_tracking' if is_tracking else 'start_tracking'
        )],
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    text = f"{message}{last_line}"
    
    message_sender = (update.callback_query.message.edit_text if update and update.callback_query 
                      else lambda text, reply_markup: context.bot.send_message(update.effective_chat.id, text, reply_markup=reply_markup))
    
    try:
        await message_sender(text=text, reply_markup=reply_markup)
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            logger.error(f"Error updating main menu: {e}")

async def main_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    action_handlers = {
        'add_wallet': add_wallet_handler, 'view_wallets': view_wallets,
        'start_tracking': start_tracking, 'stop_tracking': stop_tracking,
        'load_kols': load_kols_wallets
    }
    
    if query.data.startswith('view_wallets:'):
        # open wallets with explicit page from callback data, e.g. 'view_wallets:0'
        try:
            page = int(query.data.split(':', 1)[1])
        except Exception:
            page = 0
        await view_wallets(update, context, page=page)
    elif query.data.startswith('view_wallets_page_'):
        page = int(query.data.split('_')[-1])
        await view_wallets(update, context, page=page)
    elif query.data in action_handlers:
        await action_handlers[query.data](update, context)

async def add_wallet_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Use context.user_data to set state
    context.user_data['is_adding_wallet'] = True
    await update.callback_query.message.reply_text('Please send wallet address and name: `address YourName`')

async def receive_wallet_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    # Use context.user_data for all operations
    if not context.user_data.get('is_adding_wallet'): return
    try:
        address, name = update.message.text.split(' ', 1)
        # Prevent duplicates
        if not any(w['address'] == address for w in context.user_data['wallets']):
            context.user_data['wallets'].append({'address': address, 'name': name, 'is_tracking': False})
            await update.message.reply_text(f"Added '{name}'.")
        else:
            await update.message.reply_text(f"Wallet '{name}' with address {address} is already in the list.")
    except ValueError:
        await update.message.reply_text("Invalid format. Use: `address YourName`")
    context.user_data['is_adding_wallet'] = False
    # No manual save needed, persistence handles it.
    await show_main_menu(update, context)

async def view_wallets(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 0, chat_id_override: int = None):
    chat_id = chat_id_override or update.effective_chat.id
    message_sender = (update.callback_query.message.edit_text if update and update.callback_query 
                      else lambda text, reply_markup: context.bot.send_message(chat_id, text, reply_markup=reply_markup))
    if update and update.callback_query: await update.callback_query.answer()

    # Use context.user_data to get wallets
    wallets = context.user_data.get('wallets', [])
    if not wallets:
        await message_sender(text="No wallets to display.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main Menu", callback_data='back_to_main_menu')]]))
        return
    
    items_per_page = 10
    start_index = page * items_per_page
    paginated_wallets = wallets[start_index : start_index + items_per_page]
    keyboard = []
    for w in paginated_wallets:
        keyboard.append([
            InlineKeyboardButton(f"{'✅' if w.get('is_tracking') else '❌'} {w['name']}", callback_data=f"toggle_wallet:{page}:{w['address']}"),
            InlineKeyboardButton("🗑️", callback_data=f"remove_wallet:{page}:{w['address']}")
        ])
    nav_buttons = []
    if page > 0: nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f'view_wallets_page_{page-1}'))
    if start_index + items_per_page < len(wallets): nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f'view_wallets_page_{page+1}'))
    if nav_buttons: keyboard.append(nav_buttons)

    # Add "Select All" / "Deselect All" buttons
    control_buttons = [
        InlineKeyboardButton("✅ Select All", callback_data=f'select_all:{page}'),
        InlineKeyboardButton("❌ Deselect All", callback_data=f'deselect_all:{page}')
    ]
    keyboard.append(control_buttons)

    keyboard.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data='back_to_main_menu')])
    
    try:
        await message_sender(text=f"Wallets (Page {page + 1}/{ -(-len(wallets) // items_per_page) }):", reply_markup=InlineKeyboardMarkup(keyboard))
    except BadRequest as e:
        if "Message is not modified" in str(e):
            # Ignore this error as it's harmless (e.g., clicking "Select All" when all are already selected)
            pass
        else:
            # Re-raise other BadRequest errors
            raise

async def select_all_wallets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(query.data.split(':')[1])
    # Use context.user_data to modify wallets
    for w in context.user_data.get('wallets', []):
        w['is_tracking'] = True
    # No manual save needed
    await view_wallets(update, context, page=page)

async def deselect_all_wallets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(query.data.split(':')[1])
    # Use context.user_data to modify wallets
    for w in context.user_data.get('wallets', []):
        w['is_tracking'] = False
    # No manual save needed
    await view_wallets(update, context, page=page)

async def toggle_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, page_str, address = query.data.split(':', 2)
    page = int(page_str)
    # Use context.user_data to modify wallets
    for w in context.user_data.get('wallets', []):
        if w['address'] == address:
            w['is_tracking'] = not w.get('is_tracking', False)
    # No manual save needed
    await view_wallets(update, context, page=page)

async def remove_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, page_str, address = query.data.split(':', 2)
    page = int(page_str)
    # Use context.user_data to modify wallets
    context.user_data['wallets'] = [w for w in context.user_data.get('wallets', []) if w['address'] != address]
    # No manual save needed
    await view_wallets(update, context, page=page)

async def start_tracking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = str(query.message.chat_id)
    
    # Use context.user_data to check for selected wallets
    wallets_to_track = [w for w in context.user_data.get('wallets', []) if w.get('is_tracking')]
    
    if not wallets_to_track:
        await query.message.reply_text("⚠️ No wallets selected for tracking. Please select wallets from the 'View & Select Wallets' menu first.")
        return
        
    # Обновляем состояние UI сразу валидной картой адресов, а не пустым словарём
    context.user_data['tracking_tasks'] = {w['address']: True for w in wallets_to_track}

    # Обновляем меню сразу (без необходимости ручной остановки)
    await show_main_menu(update, context, message="🚀 Starting or updating tracker...")

    # Запускаем/гарантируем фоновые задачи (если уже запущены, они просто не продублируются)
    context.application.create_task(start_multibuy_tracker(chat_id, context.application))


async def stop_tracking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = str(query.message.chat_id)
    
    # Запускаем остановку в фоне, UI обновим сразу
    context.application.create_task(stop_multibuy_tracker(chat_id, context))
    # Не удаляем user_data['tracking_tasks'] преждевременно, чтобы кнопка не "скакала"
    await show_main_menu(update, context, message="🛑 Tracker stopping...")
    
    # Дождаться фактической остановки и обновить меню на Start
    async def _await_stop_and_refresh():
        for _ in range(40):  # ~20 секунд ожидания
            runtime_tasks = getattr(context.application, "_runtime_tracking_tasks", {}).get(chat_id, {})
            if not runtime_tasks and not context.user_data.get('tracking_tasks'):
                await show_main_menu(update, context, message="🛑 Tracker stopped.")
                return
            await asyncio.sleep(0.5)
        # Таймаут: всё равно попробовать перерисовать
        await show_main_menu(update, context, message="🛑 Tracker stopped.")
    context.application.create_task(_await_stop_and_refresh())

async def load_kols_wallets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("Request received. Fetching wallets in the background...")
    await query.edit_message_text("⏳ Загружаю KOL-кошельки... пришлю сообщение, когда закончу.")
    context.application.create_task(_background_load_kols(query.message.chat_id, context))

async def _background_load_kols(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    try:
        logger.info(f"Manual KOL sync started for chat_id={chat_id}")
    except Exception:
        pass
    try:
        kols_wallets = await get_kolscan_wallets()
        if not kols_wallets:
            await context.bot.send_message(chat_id, "🚨 Could not fetch wallets from kolscan.io.")
            return
        
        # SMART MERGE SYNC: объединяем новый список с закреплёнными трекаемыми адресами
        async with KOL_REFRESH_LOCK:
            prev_wallets = context.user_data.get('wallets', []) or []
            prev_by_addr = {w.get('address'): {'is_tracking': w.get('is_tracking', False), 'name': w.get('name', w.get('address'))} for w in prev_wallets if w.get('address')}
            fresh = [w for w in kols_wallets if w.get('address')]
            new_wallets = []
            used = set()
            for w in fresh:
                addr = w['address']
                name = w.get('name', addr)
                # prefer previous custom name if existed
                if addr in prev_by_addr and prev_by_addr[addr].get('name'):
                    name = prev_by_addr[addr]['name']
                is_tr = prev_by_addr.get(addr, {}).get('is_tracking', False)
                new_wallets.append({'name': name, 'address': addr, 'is_tracking': is_tr})
                used.add(addr)
            # keep pinned tracked wallets not present in fresh
            kept = 0
            for addr, meta in prev_by_addr.items():
                if meta.get('is_tracking') and addr not in used:
                    new_wallets.append({'name': meta.get('name', addr), 'address': addr, 'is_tracking': True})
                    kept += 1
            # If chat had tracking enabled, optionally auto-track refreshed list
            had_tracking = bool(context.user_data.get('tracking_tasks'))
            if had_tracking and os.getenv("KOL_AUTO_TRACK_REFRESH", "1") == "1":
                for item in new_wallets:
                    item['is_tracking'] = True
            context.user_data['wallets'] = new_wallets
            total_count = len(new_wallets)
        
        await context.bot.send_message(chat_id, f"✅ Все кошельки собраны. Всего: {total_count}. Закреплённых сохранено: {kept}.")
        # No manual save needed
        await view_wallets(None, context, page=0, chat_id_override=chat_id)

        # Зафиксируем момент успешной ручной синхронизации для устойчивого таймера при рестарте
        try:
            context.application.bot_data['kol_last_refresh_ts'] = int(time.time())
            # Сохранить persistence немедленно, если возможно
            try:
                await context.application.update_persistence()  # PTB v20+
            except Exception:
                pass
            # Отобразить новую метку времени в меню
            try:
                await show_main_menu(update=None, context=context, message="Menu updated after KOL sync")
            except Exception:
                pass
        except Exception:
            logger.warning("Failed to persist kol_last_refresh_ts after manual sync", exc_info=True)

        # --- RESCHEDULE AUTO-REFRESH TIMER STRICTLY FROM NOW ---
        try:
            jq = context.application.job_queue
            job_name = "kol_auto_refresh"
            # Remove existing repeating job(s)
            for job in jq.get_jobs_by_name(job_name):
                job.schedule_removal()
            # Schedule next run in full interval from now
            import os
            interval_seconds = int(os.getenv("KOL_REFRESH_INTERVAL_SECONDS", str(12*60*60)))
            jq.run_repeating(
                auto_refresh_kols_for_all_users,
                interval=interval_seconds,
                first=interval_seconds,
                name=job_name,
            )
            logger.info(f"Manual KOL sync done. Rescheduled '{job_name}' to run every {interval_seconds}s, next in {interval_seconds}s.")
        except Exception as e:
            logger.warning(f"Failed to reschedule KOL auto-refresh after manual sync: {e}")
    except Exception as e:
        logger.error(f"Error in _background_load_kols: {e}", exc_info=True)
        await context.bot.send_message(chat_id, "An error occurred while loading KOL wallets.")

async def auto_refresh_kols_for_all_users(context: ContextTypes.DEFAULT_TYPE):
    """Периодически подтягивает кошельки с kolscan.io и ПОЛНОСТЬЮ заменяет списки у всех пользователей, сохраняя is_tracking для совпадающих адресов.
    Запускать раз в 12 часов по умолчанию (настраивается через KOL_REFRESH_INTERVAL_SECONDS)."""
    async with KOL_REFRESH_LOCK:
        try:
            kols_wallets = await get_kolscan_wallets()
            if not kols_wallets:
                logger.warning("Auto-refresh: no wallets fetched from kolscan.io")
                return
            # Новый список из Kolscan
            fresh = [w for w in kols_wallets if w.get('address')]
            total_users = 0
            notify = os.getenv("KOL_AUTO_REFRESH_NOTIFY", "0") == "1"
            auto_track_refresh = os.getenv("KOL_AUTO_TRACK_REFRESH", "1") == "1"
            for user_id, udata in list(context.application.user_data.items()):
                try:
                    prev_wallets = udata.get('wallets', []) or []
                    prev_by_addr = {w.get('address'): {'is_tracking': w.get('is_tracking', False), 'name': w.get('name', w.get('address'))} for w in prev_wallets if w.get('address')}
                    new_wallets = []
                    used = set()
                    for w in fresh:
                        addr = w['address']
                        name = w.get('name', addr)
                        if addr in prev_by_addr and prev_by_addr[addr].get('name'):
                            name = prev_by_addr[addr]['name']
                        is_tr = prev_by_addr.get(addr, {}).get('is_tracking', False)
                        new_wallets.append({'name': name, 'address': addr, 'is_tracking': is_tr})
                        used.add(addr)
                    # keep pinned tracked wallets not present in fresh
                    for addr, meta in prev_by_addr.items():
                        if meta.get('is_tracking') and addr not in used:
                            new_wallets.append({'name': meta.get('name', addr), 'address': addr, 'is_tracking': True})
                    # If chat had tracking enabled, optionally auto-track refreshed list
                    had_tracking = bool(udata.get('tracking_tasks'))
                    if had_tracking and auto_track_refresh:
                        for item in new_wallets:
                            item['is_tracking'] = True
                    udata['wallets'] = new_wallets
                    total_users += 1
                    # Авто‑уведомление (по умолчанию выключено)
                    if notify:
                        try:
                            await context.bot.send_message(chat_id=int(user_id), text=f"🔄 Авто‑синхронизация KOL: {len(new_wallets)} кошельков. Отметки трекинга сохранены; закреплённые адреса сохранены.")
                        except Exception:
                            try:
                                await context.bot.send_message(chat_id=user_id, text=f"🔄 Авто‑синхронизация KOL: {len(new_wallets)} кошельков. Отметки трекинга сохранены; закреплённые адреса сохранены.")
                            except Exception as e:
                                logger.warning(f"Failed to notify user {user_id} about smart KOL sync: {e}")
                    # Гарантируем, что трекер активен для чата, где был запущен трекинг
                    try:
                        if had_tracking:
                            context.application.create_task(start_multibuy_tracker(str(user_id), context.application))
                    except Exception as e:
                        logger.warning(f"Failed to reassert tracker for chat {user_id}: {e}")
                except Exception as e:
                    logger.warning(f"Auto-refresh smart sync failed for user {user_id}: {e}", exc_info=True)
            logger.info(f"Auto-refresh KOL smart merge complete. Users updated: {total_users}.")

            # Зафиксируем момент успешного авто‑обновления для восстановления таймера после рестарта
            try:
                context.application.bot_data['kol_last_refresh_ts'] = int(time.time())
                try:
                    await context.application.update_persistence()
                except Exception:
                    pass
            except Exception:
                logger.warning("Failed to persist kol_last_refresh_ts after auto refresh", exc_info=True)
        except Exception as e:
            logger.error(f"Auto-refresh KOL wallets job failed: {e}", exc_info=True)

async def back_to_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_main_menu(update, context)
