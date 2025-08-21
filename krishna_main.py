import asyncio
import os
import signal
from datetime import datetime, timedelta, timezone

from utils.logger import log
from utils.time_windows import is_market_open_now, next_market_close_dt_ist, NOW_IST, sleep_until
from utils.params import Params
from integrations import sheets as sh
from analytics.oc_refresh import day_oc_loop, get_snapshot
from agents.signal_generator import signal_loop_once
from agents.tp_sl_watcher import trail_tick
from agents.trade_loop import trade_loop_tick
from telegram_bot import init as init_bot

PYTHON_RUNTIME = f"{os.sys.version.split()[0]}"

# global toggle managed by /run oc_auto
OC_AUTO = os.getenv("OC_AUTO_DEFAULT", "true").lower() == "true"

async def day_loop():
    """
    Main day loop: refresh OC, generate signals, trade loop, TP/SL watcher, heartbeat.
    Runs only during market hours; auto-flat at 15:15 IST via watcher.
    """
    log.info("Day loop started")
    hb = 0
    while True:
        try:
            if is_market_open_now():
                # 1) refresh OC (snapshot & write sheet inside)
                await day_oc_loop()

                # 2) signals
                try:
                    await signal_loop_once()
                except Exception as e:
                    log.error(f"signal gen err: {e}")

                # 3) paper trade loop
                try:
                    await trade_loop_tick()
                except Exception as e:
                    log.error(f"trade loop err: {e}")

                # 4) trailing & MV exits
                try:
                    changed = await trail_tick()
                    _ = changed
                except Exception as e:
                    log.error(f"tp/sl watcher err: {e}")
            else:
                await asyncio.sleep(5)

            # heartbeat
            hb += 1
            if hb % 3 == 0:
                log.info("heartbeat: day_loop alive")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"day_loop crash-protect: {e}")
        await asyncio.sleep(2)

async def main():
    log.info(f"Python runtime: {PYTHON_RUNTIME}")
    # Sheets tabs
    try:
        await sh.ensure_tabs()
        log.info("✅ Sheets tabs ensured")
    except Exception as e:
        log.error(f"Sheets ensure_tabs failed: {e}")

    # Telegram
    app = None
    try:
        app = await init_bot()
        log.info("Telegram polling started")
    except Exception as e:
        log.error(f"Telegram Application init failed: {e}")

    # start concurrent tasks
    tasks = []
    if app:
        tasks.append(asyncio.create_task(app.run_polling(close_loop=False)))
        log.info("Telegram bot started")

    tasks.append(asyncio.create_task(day_loop()))

    # graceful stop on SIGTERM
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _sigterm():
        stop_event.set()

    loop.add_signal_handler(signal.SIGTERM, _sigterm)

    await stop_event.wait()
    log.info("Stopping...")
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    if app:
        await app.stop()
        await app.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
