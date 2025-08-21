# main.py
import logging
import os
import sys
import psutil
import asyncio
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler, PicklePersistence
from dotenv import load_dotenv
import time

# Ensure environment variables are loaded BEFORE importing modules that read them
load_dotenv(override=True)

from helpers.menu_handlers import (
    start,
    show_main_menu,
    receive_wallet_address,
    main_menu_handler,
    remove_wallet,
    toggle_wallet,
    back_to_main_menu,
    load_kols_wallets,
    select_all_wallets,
    deselect_all_wallets,
    auto_refresh_kols_for_all_users,
)

# Enable logging (configurable)
LOG_LEVEL = os.getenv("LOG_LEVEL", "WARNING").upper()
_level = getattr(logging, LOG_LEVEL, logging.WARNING)
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=_level
)
# Quiet noisy libraries
for noisy in ("httpx", "aiohttp", "urllib3", "playwright", "telegram", "telegram.ext", "asyncio"):
    try:
        logging.getLogger(noisy).setLevel(logging.WARNING)
    except Exception:
        pass

async def main() -> None:
    """Start the bot."""
    if not os.path.exists('.env'):
        logging.error(".env file not found! Please create it.")
        return
        
    # load_dotenv()  # Already loaded at import time
    
    lock_file = 'bot.lock'
    if os.path.exists(lock_file):
        try:
            with open(lock_file, 'r') as f:
                pid = int(f.read())
            if psutil.pid_exists(pid):
                logging.error(f"Lock file exists for running process {pid}. Exiting.")
                return
            else:
                logging.warning("Stale lock file found. Removing.")
                os.remove(lock_file)
        except (ValueError, FileNotFoundError):
            logging.warning("Corrupt or empty lock file found. Removing.")
            os.remove(lock_file)

    application = None
    try:
        with open(lock_file, 'w') as f:
            f.write(str(os.getpid()))

        token = os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
        if not token:
            logging.error("TELEGRAM_BOT_TOKEN not found in .env file!")
            return
            
        # Use the library's built-in persistence
        persistence = PicklePersistence(filepath="bot_data.pickle")
        
        application = Application.builder().token(token).persistence(persistence).build()

        # Main menu and wallet management handlers
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CallbackQueryHandler(toggle_wallet, pattern=r'^toggle_wallet:'))
        application.add_handler(CallbackQueryHandler(remove_wallet, pattern=r'^remove_wallet:'))
        application.add_handler(CallbackQueryHandler(select_all_wallets, pattern=r'^select_all:'))
        application.add_handler(CallbackQueryHandler(deselect_all_wallets, pattern=r'^deselect_all:'))
        application.add_handler(CallbackQueryHandler(back_to_main_menu, pattern=r'^back_to_main_menu$'))
        application.add_handler(CallbackQueryHandler(main_menu_handler)) # Default handler
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, receive_wallet_address))

        logging.info("Bot starting...")
        await application.initialize()
        # Ensure webhook is removed and pending updates are dropped before polling
        await application.bot.delete_webhook(drop_pending_updates=True)
        await application.start()
        await application.updater.start_polling()

        # Schedule auto-refresh job with respect to last refresh time
        try:
            interval_seconds = int(os.getenv("KOL_REFRESH_INTERVAL_SECONDS", str(12*60*60)))
            now_ts = int(time.time())
            last_ts = application.bot_data.get('kol_last_refresh_ts')
            if isinstance(last_ts, (int, float)) and last_ts > 0:
                elapsed = max(0, now_ts - int(last_ts))
                first_delay = max(1, interval_seconds - elapsed) if elapsed < interval_seconds else 1
                logging.info(f"Recovered kol_last_refresh_ts={last_ts}, elapsed={elapsed}s → scheduling first run in {first_delay}s")
            else:
                first_delay = interval_seconds
                logging.info("No kol_last_refresh_ts found → scheduling first run in full interval")

            application.job_queue.run_repeating(
                auto_refresh_kols_for_all_users,
                interval=interval_seconds,
                first=first_delay,
                name="kol_auto_refresh",
            )
            logging.info(f"Scheduled kol_auto_refresh job every {interval_seconds} seconds (first run in {first_delay}s)")
        except Exception as e:
            logging.error(f"Failed to schedule kol_auto_refresh job: {e}")
        
        # Keep the script running
        while True:
            await asyncio.sleep(3600)

    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopping...")
    finally:
        if application:
            await application.updater.stop()
            await application.stop()
            await application.shutdown()
        if os.path.exists(lock_file):
            os.remove(lock_file)
        logging.info("Bot stopped and lock file removed.")

if __name__ == '__main__':
    asyncio.run(main())
