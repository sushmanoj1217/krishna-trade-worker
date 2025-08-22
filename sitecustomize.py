# sitecustomize.py
# ------------------------------------------------------------
# Auto-loaded hook on Python startup.
# 1) OC symbol/env guard (safe; independent of Telegram)
# 2) (Unchanged) Telegram single-poller guard:
#    - TELEGRAM_DISABLED=true -> skip polling
#    - Local file lock (same host)
#    - Google Sheets distributed lock (cross-host)
# ------------------------------------------------------------
from __future__ import annotations
import asyncio, atexit, logging, os, socket

_log = logging.getLogger(__name__)

# -------- 1) OC SYMBOL GUARD (always safe) --------
try:
    from infra.oc_symbol_guard import apply as _oc_guard_apply
    _oc_info = _oc_guard_apply()
    if isinstance(_oc_info, dict) and _oc_info.get("symbol"):
        _log.debug("oc_symbol_guard applied: %s", _oc_info)
except Exception as e:
    _log.warning("oc_symbol_guard: failed to apply: %s", e)

# -------- 2) Telegram single-poller (as-is) --------
try:
    from telegram.ext._updater import Updater
except Exception:
    Updater = None  # type: ignore

# Local lock
try:
    from infra.ptb_singleton import acquire_lock as _local_lock, release_lock as _local_release, is_disabled_by_env as _is_disabled, token_from_env as _token
except Exception:
    def _local_lock(_t): return True
    def _local_release(): pass
    def _is_disabled(): return False
    def _token(): return os.environ.get("TELEGRAM_BOT_TOKEN")

# Distributed lock (Sheets)
try:
    from infra.sheets_lock import acquire as _d_acquire, refresh as _d_refresh, release as _d_release
except Exception:
    _d_acquire = None
    _d_refresh = None
    _d_release = None

_LOCK_KEY = "TELEGRAM_POLL_LOCK"
_HOLDER = f"{socket.gethostname()}:{os.getpid()}"
_HEARTBEAT_TASK: asyncio.Task | None = None
_DISTRIBUTED_HELD = False

async def _heartbeat():
    global _DISTRIBUTED_HELD
    if _d_refresh is None:
        return
    while True:
        try:
            ok = _d_refresh(_LOCK_KEY, _HOLDER, ttl_sec=120)
            if not ok:
                _log.warning("PTB singleton: distributed lock refresh failed; another node may take over.")
                _DISTRIBUTED_HELD = False
                return
        except Exception as e:
            _log.warning("PTB singleton: distributed refresh error: %s", e)
        await asyncio.sleep(30)

def _release_all():
    try:
        if _d_release and _DISTRIBUTED_HELD:
            _d_release(_LOCK_KEY, _HOLDER)
    except Exception:
        pass
    try:
        _local_release()
    except Exception:
        pass

def _patch_updater_start_polling():
    if Updater is None:
        return
    orig = getattr(Updater, "start_polling", None)
    if orig is None or getattr(Updater.start_polling, "_singleton_patched", False):  # type: ignore[attr-defined]
        return

    async def wrapped(self, *args, **kwargs):  # type: ignore[no-redef]
        # 0) disabled by env?
        if _is_disabled():
            _log.warning("PTB singleton: TELEGRAM_DISABLED=true → skipping start_polling()")
            return None

        tok = _token()
        # 1) local lock
        if not _local_lock(tok):
            _log.warning("PTB singleton: local lock busy → skipping start_polling()")
            return None

        # 2) distributed lock
        global _DISTRIBUTED_HELD, _HEARTBEAT_TASK
        if _d_acquire is not None:
            try:
                ok, holder = _d_acquire(_LOCK_KEY, ttl_sec=120)
                if not ok:
                    _log.warning("PTB singleton: distributed lock busy (holder=%s) → skipping start_polling()", holder)
                    return None
                _DISTRIBUTED_HELD = True
            except Exception as e:
                _log.warning("PTB singleton: distributed lock error: %s (continuing with local lock only)", e)

        # 3) start polling
        res = orig(self, *args, **kwargs)
        if asyncio.iscoroutine(res):
            res = await res  # type: ignore[func-returns-value]
        _log.info("Telegram polling started (singleton OK)")

        # 4) heartbeat
        if _DISTRIBUTED_HELD and _d_refresh is not None:
            try:
                loop = asyncio.get_running_loop()
                _HEARTBEAT_TASK = loop.create_task(_heartbeat())
            except Exception as e:
                _log.warning("PTB singleton: heartbeat schedule failed: %s", e)
        return res

    setattr(wrapped, "_singleton_patched", True)
    Updater.start_polling = wrapped  # type: ignore[assignment]
    atexit.register(_release_all)

try:
    _patch_updater_start_polling()
    _log.debug("PTB singleton: patch applied")
except Exception as e:
    _log.exception("PTB singleton: patching failed: %s", e)
