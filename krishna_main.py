# krishna_main.py
from __future__ import annotations

import os
import sys
import asyncio
import logging
import inspect
import signal
import platform
import random
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
    res = callable_obj(*args, **kwargs)
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

# ---------- OC refresh loops ----------
def _read_int_env(name: str, default: int) -> int:
    try:
        v = int(os.environ.get(name, str(default)))
        return v
    except Exception:
        return default

async def day_oc_loop():
    """
    Periodically refresh OC snapshot.
    Now with jitter to avoid thundering herd / 429 spikes.
    Env:
      - OC_REFRESH_SECS (default 15)
      - OC_REFRESH_JITTER_SECS (default 3)  -> actual sleep = base + U(0, jitter)
    """
    base = _read_int_env("OC_REFRESH_SECS", 15)
    jitter = _read_int_env("OC_REFRESH_JITTER_SECS", 3)
    if base < 5:
        base = 15
    if jitter < 0:
        jitter = 0

    log.info("Day loop started (cadence ~%ss + jitter 0–%ss)", base, jitter)
    while True:
        try:
            await refresh_once()
        except Exception as e:
            log.error("OC refresh failed: %s", e)
        # jittered sleep
        sleep_for = float(base) + (random.random() * float(jitter))
        await asyncio.sleep(sleep_for)

async def heartbeat_loop():
    while True:
        log.info("heartbeat: day_loop alive")
        await asyncio.sleep(10)

# ---------- Telegram start/stop ----------
async def start_polling(app):
    """
    v20-safe start sequence. sitecustomize may skip start if disabled/locked.
    """
    try:
        await app.initialize()
        await app.start()
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

# ---------- Main ----------
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
