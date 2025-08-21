# krishna_main.py
# App bootstrap: starts Telegram bot (non-blocking), day loops, health logs.

import os
import sys
import asyncio
import signal
import time
from typing import Optional

from utils.logger import log

# Sheets bootstrap
from integrations import sheets as sh

# OC refresh (pulls Dhan OC / compute levels / write OC_Live etc.)
from analytics.oc_refresh_shim import refresh_once

# Telegram application factory (must return telegram.ext.Application or None if disabled)
from telegram_bot import init as init_bot

# Optional: TP/SL watcher (may be sync or async; handle both)
try:
    from agents.tp_sl_watcher import trail_tick as _trail_tick
except Exception:  # noqa
    _trail_tick = None

# Optional: trade loop tick (paper execution)
try:
    from agents.trade_loop import tick as _trade_tick
except Exception:  # noqa
    _trade_tick = None

# Optional: signal generator tick (6-checks etc.) if you keep it separate
try:
    from agents.signal_generator import tick as _signal_tick
except Exception:  # noqa
    _signal_tick = None


def _get_int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


async def _maybe_call(func):
    """
    Call a function that might be sync or async. If None, do nothing.
    """
    if func is None:
        return
    try:
        res = func()
        if asyncio.iscoroutine(res):
            await res
    except TypeError:
        # In case it's defined as async def but called without parentheses above
        try:
            res = await func()  # type: ignore
            return res
        except Exception as e:
            log.error("call err: %s", e)
    except Exception as e:
        log.error("call err: %s", e)


async def day_loop():
    """
    Main intraday loop:
    - refresh OC snapshot
    - run signal/trade/tp-sl ticks (if available)
    - health heartbeat
    """
    refresh_secs = _get_int_env("OC_REFRESH_SECS", 15)
    heartbeat_every = 30
    last_heartbeat = 0.0

    log.info("Day loop started")

    while True:
        # 1) OC refresh
        try:
            await refresh_once()
        except Exception as e:
            # oc_refresh internally logs fine-grained errors; this is a guard
            log.error("OC refresh failed: %s", e)

        # 2) Run signal generator (if exposed as tick)
        try:
            await _maybe_call(_signal_tick)
        except Exception as e:
            log.error("signal gen err: %s", e)

        # 3) Paper trade loop (entries/exits)
        try:
            await _maybe_call(_trade_tick)
        except Exception as e:
            log.error("trade loop err: %s", e)

        # 4) TP/SL watcher (trail / MV reversal exits)
        try:
            # Support both sync & async versions
            if _trail_tick is not None:
                res = _trail_tick()
                if asyncio.iscoroutine(res):
                    await res
        except Exception as e:
            log.error("tp/sl watcher err: %s", e)

        # 5) Heartbeat
        now = time.time()
        if now - last_heartbeat >= heartbeat_every:
            log.info("heartbeat: day_loop alive")
            last_heartbeat = now

        # sleep till next cycle
        try:
            await asyncio.sleep(max(1, refresh_secs))
        except asyncio.CancelledError:
            # Shutdown requested
            break


async def main():
    # Basic runtime info
    log.info("Python runtime: %s", ".".join(map(str, sys.version_info[:3])))

    # Ensure Sheets tabs exist (idempotent)
    try:
        ok = sh.ensure_tabs()
        if ok:
            log.info("✅ Sheets tabs ensured")
        else:
            log.warning("Sheets ensure_tabs returned False (check creds/ids)")
    except Exception as e:
        log.error("Sheets ensure_tabs failed: %s", e)

    # Build Telegram Application (may return None if TELEGRAM_DISABLED=true)
    app = await init_bot()
    tasks = []

    # Start Telegram non-blocking within our asyncio loop
    if app:
        # Proper async lifecycle for PTB v20+
        await app.initialize()
        log.info("Telegram polling started")
        await app.start()
        await app.updater.start_polling()
        log.info("Telegram bot started")

    # Start our day loop as a background task
    day_task = asyncio.create_task(day_loop())
    tasks.append(day_task)

    # Graceful shutdown on SIGTERM/SIGINT
    shutdown_event = asyncio.Event()

    def _signal_handler():
        try:
            shutdown_event.set()
        except Exception:
            pass

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows or restricted env
            pass

    # Wait for shutdown request
    await shutdown_event.wait()

    # Cancel background tasks
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    # Telegram clean shutdown
    if app:
        try:
            await app.updater.stop()
        except Exception:
            pass
        try:
            await app.stop()
        except Exception:
            pass
        try:
            await app.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    # Run the async main
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
