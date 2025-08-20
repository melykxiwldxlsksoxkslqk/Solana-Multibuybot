import os
import asyncio

# Ensure silent notifications during test
os.environ.setdefault("KOL_AUTO_REFRESH_NOTIFY", "0")

async def main() -> None:
    from helpers import menu_handlers as mh

    # ---- Dummy Telegram objects ----
    class DummyBot:
        async def send_message(self, chat_id, text, **kwargs):
            # keep quiet; just prove the code path works
            pass

    class DummyJobQueue:
        def get_jobs_by_name(self, name: str):
            return []

    class DummyApp:
        def __init__(self) -> None:
            # Chat 12345 has tracking enabled
            self.user_data = {
                12345: {
                    'wallets': [{'name': 'Prev', 'address': 'A1', 'is_tracking': True}],
                    'tracking_tasks': {'A1': True},
                }
            }
            self.bot_data = {}
            self.job_queue = DummyJobQueue()
        def create_task(self, coro):
            return asyncio.create_task(coro)

    class DummyCtx:
        def __init__(self, app) -> None:
            self.application = app
            self.bot = DummyBot()

    # ---- Monkeypatch dependencies ----
    async def fake_get_kolscan_wallets():
        # New list comes with a different address; pinned should be preserved
        return [{'name': 'New', 'address': 'A2'}]

    calls: list[str] = []
    async def fake_start_multibuy_tracker(chat_id, application):
        calls.append(str(chat_id))

    # Apply patches
    mh.get_kolscan_wallets = fake_get_kolscan_wallets  # type: ignore
    mh.start_multibuy_tracker = fake_start_multibuy_tracker  # type: ignore

    ctx = DummyCtx(DummyApp())
    await mh.auto_refresh_kols_for_all_users(ctx)

    # Assert tracker was reasserted for tracked chat
    if '12345' in calls:
        print('OK: tracker reasserted for chat 12345')
    else:
        raise SystemExit('FAIL: tracker was not reasserted')

if __name__ == '__main__':
    asyncio.run(main()) 