# sitecustomize.py
# ------------------------------------------------------------
# Auto-loaded by Python at startup (via site.py). We use this to enforce a
# cross-process singleton for python-telegram-bot polling. No changes needed
# in your existing krishna_main.py.
# ------------------------------------------------------------
from __future__ import annotations
import asyncio
import atexit
import logging

# Keep imports guarded so app still runs if PTB is not present at import time.
try:
    from telegram.ext._updater import Updater  # PTB v20 still exposes Updater under Application.updater
except Exception:
    Updater = None  # type: ignore

try:
    from infra.ptb_singleton import acquire_lock, release_lock, is_disabled_by_env, token_from_env
except Exception:
    # As a last resort, provide safe fallbacks (no-op guards)
    def acquire_lock(token):  # type: ignore
        return True
    def release_lock():       # type: ignore
        pass
    def is_disabled_by_env(): # type: ignore
        return False
    def token_from_env():     # type: ignore
        return None

_log = logging.getLogger(__name__)

def _patch_updater_start_polling():
    if Updater is None:
        return
    original_start = getattr(Updater, "start_polling", None)
    if original_start is None:
        return
    # Avoid double-patching
    if getattr(Updater.start_polling, "_singleton_patched", False):  # type: ignore[attr-defined]
        return

    async def _wrapped_start_polling(self, *args, **kwargs):  # type: ignore[no-redef]
        # 1) Disabled by env? (Night/Cron)
        if is_disabled_by_env():
            _log.warning("PTB singleton: TELEGRAM_DISABLED=true → skipping start_polling()")
            return None

        # 2) Singleton lock per token
        token = token_from_env()
        if not acquire_lock(token):
            _log.warning("PTB singleton: lock busy → skipping start_polling() in this process")
            return None

        # 3) Proceed with original start
        _log.info("Telegram polling started (singleton OK)")
        res = original_start(self, *args, **kwargs)
        # If original returns coroutine/future, await it to preserve semantics
        if asyncio.iscoroutine(res):
            res = await res  # type: ignore[func-returns-value]
        _log.info("Telegram bot started")
        return res

    # mark and patch
    setattr(_wrapped_start_polling, "_singleton_patched", True)
    Updater.start_polling = _wrapped_start_polling  # type: ignore[assignment]

    # Ensure we release the lock at exit (not strictly necessary)
    atexit.register(release_lock)

# Apply patches at import time
try:
    _patch_updater_start_polling()
    _log.debug("PTB singleton: Updater.start_polling patched")
except Exception:
    # Never block app startup due to patching
    _log.exception("PTB singleton: patching failed; continuing without singleton")
