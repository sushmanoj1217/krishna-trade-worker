# infra/ptb_singleton.py
# ------------------------------------------------------------
# Cross-process singleton utilities for python-telegram-bot poller.
# Provides an fcntl-based file lock so only one process holding the same token
# can start getUpdates() polling. Safe on Linux (Render).
# ------------------------------------------------------------
from __future__ import annotations
import os
import errno
import fcntl
import logging
from typing import Optional

_log = logging.getLogger(__name__)

_LOCK_FD: Optional[int] = None
_LOCK_PATH: Optional[str] = None

def _lock_path_for_token(token: str | None) -> str:
    # Last 6 chars to differentiate if multiple bots on the same machine.
    suffix = (token[-6:] if token else "no_token")
    return f"/tmp/telegram_poller.{suffix}.lock"

def acquire_lock(token: str | None) -> bool:
    """
    Try to acquire an exclusive (non-blocking) lock.
    Returns True if acquired; False if already locked by another process.
    """
    global _LOCK_FD, _LOCK_PATH
    if _LOCK_FD is not None:
        # Already locked in this process
        return True

    path = _lock_path_for_token(token)
    try:
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        os.fsync(fd)
        _LOCK_FD = fd
        _LOCK_PATH = path
        _log.info("singleton: acquired lock %s (pid=%s)", path, os.getpid())
        return True
    except OSError as e:
        if e.errno in (errno.EACCES, errno.EAGAIN):
            _log.warning("singleton: another poller holds lock %s; skipping start", path)
            return False
        _log.exception("singleton: lock error on %s", path)
        return False

def release_lock() -> None:
    """
    Release the singleton lock (if held). Usually not needed because the OS
    releases on process exit, but kept for completeness.
    """
    global _LOCK_FD, _LOCK_PATH
    if _LOCK_FD is None:
        return
    try:
        fcntl.flock(_LOCK_FD, fcntl.LOCK_UN)
        os.close(_LOCK_FD)
        _log.info("singleton: released lock %s", _LOCK_PATH)
    except Exception:
        _log.exception("singleton: error while releasing lock")
    finally:
        _LOCK_FD = None
        _LOCK_PATH = None

def is_disabled_by_env() -> bool:
    """
    If TELEGRAM_DISABLED=true (case-insensitive) → treat as disabled.
    Useful for Night/Cron workers so they don't start polling.
    """
    val = os.environ.get("TELEGRAM_DISABLED", "")
    return val.strip().lower() in ("1", "true", "yes", "y", "on")

def token_from_env() -> str | None:
    return os.environ.get("TELEGRAM_BOT_TOKEN")
