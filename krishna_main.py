# krishna_main.py
from __future__ import annotations

import os
import sys
import asyncio
import logging
import inspect
import signal
import platform
from typing import Optional

# Ensure all startup hooks (singleton, env guards) load early
import sitecustomize  # noqa: F401

# ---------- Logging ----------
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------- Imports that may be optional in your tree ----------
# Telegram application factory (sync in our drop-in, but we handle both)
try:
    from telegram_bot import init as init_bot
except Exception:
    init_bot = None

try:
    from telegram_bot import build_application as build_app_fallback
except Exception:
    build_app_fallback = None

# OC refresh shim (awaitable)
try:
    from analytics.oc_refresh_shim import refresh_once, get_snapshot
except Exception:
    async def refresh_once(*args, **kwargs):
        return {"status": "noop"}  # type: ignore[misc]
    def get_snapshot():
        return None  # type: ignore[misc]

# Sheets tabs ensure (optional; may be async or sync)
def _resolve_ensure_tabs():
    """
    Try to locate an 'ensure_tabs' function from your codebase.
    Accepts either async or sync function; returns callable or None.
    """
    for mod_name in ("sheets_admin", "skills.sheets_admin", "utils.sheets_admin"):
        try:
            mod = __import__(mod_name, fromlist=["*"])
            fn = getattr(mod, "ensure_tabs", None)
            if callable(fn):
                return fn
        except Exception:
            continue
    return None

ENSURE_TABS_FN = _resolve_ensure_tabs()

# ---------- Helpers ----------
async def _maybe_await(callable_obj, *args, **kwargs):
    """Call a function which might be sync or async."""
    try:
        res = callable_obj(*args, **kwargs)
        if inspect.isawaitable(res):
            return await res
        return res
    except TypeError:
        # try calling with no args if signature mismatch
        res = callable_obj()
        if inspect.isawaitable(res):
            return await res
        return res


async def _ensure_tabs_safe():
    if ENSURE_TABS_FN is None:
        return
    try:
        await _maybe_await(ENSURE_TABS_FN)
        log.info("✅ Sheets tabs ensured")
    except Exception as e:
        log.warning("Sheets ensure failed: %s", e)


async def _build_application():
    """
    Create Telegram Application. Supports both sync/async init().
    """
    if init_bot is None and build_app_fallback is None:
        raise RuntimeError("telegram_bot.init/build_application not found")

    factory = init_bot or build_app_fallback
    if inspect.iscoroutinefunction(factory):
        app = await factory()  # type: ignore[misc]
    else:
        app = factory()        # type: ignore[misc]
    return app


async def day_oc_loop():
    """
    Periodically refresh OC snapshot (handles both success & rate-limit).
    """
    try:
        secs = int(os.environ.get("OC_REFRESH_SECS", "12"))
    except Exception:
        secs = 12
    if secs < 5:
        secs = 12

    log.info("Day loop started")
    while True:
        try:
            await refresh_once()
        except Exception as e:
            log.error("OC refresh failed: %s", e)
        await asyncio.sleep(secs)


async def heartbeat_loop():
    while True:
        log.info("heartbeat: day_loop alive")
        await asyncio.sleep(10)


async def start_polling(app):
    """
    v20-safe start sequence. sitecustomize may skip start if disabled/locked.
    """
    # Application.init & start
    try:
        await app.initialize()
        await app.start()
        # Updater.start_polling() is awaitable in PTB v20
        await app.updater.start_polling()
        log.info("Telegram bot started")
    except Exception as e:
        # If TELEGRAM_DISABLED=true or singleton lock busy, sitecustomize
        # will skip start_polling() silently; we just log and continue.
        log.warning("Telegram polling start skipped/failed: %s", e)


async def stop_polling(app):
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


async def main():
    log.info("Python runtime: %s", platform.python_version())

    # Ensure Sheets tabs (async/sync supported)
    await _ensure_tabs_safe()

    # Build Telegram application (sync or async factory)
    app = await _build_application()

    # Start polling (guarded by sitecustomize singleton)
    await start_polling(app)

    # Start background loops
    tasks = [
        asyncio.create_task(day_oc_loop(), name="day_oc_loop"),
        asyncio.create_task(heartbeat_loop(), name="heartbeat"),
    ]

    # Graceful shutdown handling
    stop_event = asyncio.Event()

    def _signal_handler():
        log.info("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows or restricted env
            pass

    await stop_event.wait()

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await stop_polling(app)
    log.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
