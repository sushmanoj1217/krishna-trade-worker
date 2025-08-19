import os, asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from utils.logger import log
from utils.time_windows import is_market_close_now
from analytics.oc_refresh import refresh_once
from agents.signal_generator import run_once as run_signal
from agents.trade_loop import manage_open_trades
from agents.tp_sl_watcher import run_once as tpw
from housekeeping.auto_backup import run_backup
from agents.eod_tuner import run_nightly
from telegram_bot import init as init_bot
from integrations.sheets import ensure_tabs


TICK = int(os.getenv("TICK_SECS", "10"))


async def day_loop():
log.info("Day loop started")
while True:
refresh_once()
run_signal()
manage_open_trades()
tpw()
await asyncio.sleep(TICK)


async def nightly_jobs():
run_backup()
run_nightly()


async def main():
ensure_tabs()
app = await init_bot()
scheduler = AsyncIOScheduler()
scheduler.add_job(nightly_jobs, "cron", hour=20, minute=5, timezone="Asia/Kolkata")
scheduler.start()


tasks = [asyncio.create_task(day_loop())]
if app:
tasks.append(app.initialize())
tasks.append(app.start())
log.info("Telegram bot started")


try:
await asyncio.gather(*tasks)
except Exception as e:
log.error(f"Main crashed: {e}")
finally:
if app:
await app.stop()
await app.shutdown()


if __name__ == "__main__":
asyncio.run(main())
