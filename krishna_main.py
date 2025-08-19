# krishna_main.py
from __future__ import annotations
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
from utils.state import is_oc_auto

IST = ZoneInfo("Asia/Kolkata")
OC_REFRESH_SECS = int(os.getenv("OC_REFRESH_SECS", "10"))
MARKET_CUTOFF = dtime(15, 15)

async def day_loop():
    log.info("Day loop started")
    while True:
        now = datetime.now(tz=IST).time()
        if now >= MARKET_CUTOFF:
            try: await force_flat_all(reason="auto-flat 15:15 IST")
            except Exception as e: log.error(f"auto-flat failed: {e}")
            await asyncio.sleep(30); continue

        refresh_once()

        if is_oc_auto():
            try: signal_generator.run_once()
            except Exception as e: log.error(f"signal gen err: {e}")
            try: await trade_tick()
            except Exception as e: log.error(f"trade loop err: {e}")
            try: await trail_tick()
            except Exception as e: log.error(f"tp/sl watcher err: {e}")

        await asyncio.sleep(OC_REFRESH_SECS)

async def main():
    try:
        sh.ensure_tabs()
        log.info("✅ Sheets tabs ensured")
    except Exception as e:
        log.error(f"Sheets ensure_tabs failed: {e}")

    app = await init_bot()
    if app:
        await app.initialize(); await app.start()
        log.info("Telegram bot started")

    loop_task = asyncio.create_task(day_loop())

    stop_event = asyncio.Event()
    def _stop(*_): stop_event.set()
    for s in (signal.SIGINT, signal.SIGTERM):
        try: asyncio.get_running_loop().add_signal_handler(s, _stop)
        except NotImplementedError: pass

    await stop_event.wait()
    loop_task.cancel()
    if app:
        await app.stop(); await app.shutdown()

if __name__ == "__main__":
    log.info(f"Python runtime: {os.sys.version}")
    asyncio.run(main())
