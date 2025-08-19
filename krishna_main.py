import os, asyncio, signal
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

from utils.logger import log
from integrations import sheets as sh
from telegram_bot import init as init_bot
from analytics.oc_refresh import refresh_once
from agents import signal_generator
from agents.trade_loop import trade_tick, force_flat_all
from agents.tp_sl_watcher import trail_tick

IST = ZoneInfo("Asia/Kolkata")
OC_REFRESH_SECS = int(os.getenv("OC_REFRESH_SECS", "10"))
MARKET_START = dtime(9, 15)
MARKET_NO_TRADE_1 = dtime(9, 15)
MARKET_NO_TRADE_2 = dtime(9, 30)
MARKET_CUTOFF = dtime(15, 15)  # hard flat

async def day_loop():
    log.info("Day loop started")
    while True:
        now = datetime.now(tz=IST).time()
        # hard flat & sleep past cutoff
        if now >= MARKET_CUTOFF:
            try:
                await force_flat_all(reason="auto-flat 15:15 IST")
            except Exception as e:
                log.error(f"auto-flat failed: {e}")
            await asyncio.sleep(30)
            continue

        # refresh OC snapshot
        snap = refresh_once()

        # run signal gen (writes Signals row inside)
        try:
            signal_generator.run_once()
        except Exception as e:
            log.error(f"signal gen err: {e}")

        # trade execution & trailing/exit handling
        try:
            await trade_tick()
        except Exception as e:
            log.error(f"trade loop err: {e}")

        try:
            await trail_tick()
        except Exception as e:
            log.error(f"tp/sl watcher err: {e}")

        await asyncio.sleep(OC_REFRESH_SECS)

async def main():
    # Sheets tabs ready
    try:
        sh.ensure_tabs()
        log.info("✅ Sheets tabs ensured")
    except Exception as e:
        log.error(f"Sheets ensure_tabs failed: {e}")

    # Telegram bot (optional)
    app = await init_bot()
    if app:
        await app.initialize()
        await app.start()
        log.info("Telegram bot started")

    # Day loop
    loop_task = asyncio.create_task(day_loop())

    # graceful shutdown
    stop_event = asyncio.Event()
    def _stop(*_):
        stop_event.set()
    for s in (signal.SIGINT, signal.SIGTERM):
        try: asyncio.get_running_loop().add_signal_handler(s, _stop)
        except NotImplementedError: pass

    await stop_event.wait()
    loop_task.cancel()

    if app:
        await app.stop()
        await app.shutdown()

if __name__ == "__main__":
    log.info(f"Python runtime: {os.sys.version}")
    asyncio.run(main())
