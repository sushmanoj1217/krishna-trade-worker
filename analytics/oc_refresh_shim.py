# analytics/oc_refresh_shim.py
# -----------------------------------------------------------------------------
# OC refresh shim that provides:
#   - Dynamic function resolution (env overrides)
#   - **Single-flight** guard (no overlapping refresh calls)
#   - Last-snapshot caching (serve while refresh inflight)
#   - Tiny helpers for market-hours checks (optional for callers)
#
# Default chain:
#   OC_REFRESH_FUNC (env)  -> dotted async(p)->dict
#   else DHAN_PROVIDER_FUNC-> dotted async(p)->dict
#   else providers.dhan_oc.refresh_once
# -----------------------------------------------------------------------------

from __future__ import annotations
import asyncio, importlib, logging, os, time
from typing import Any, Awaitable, Callable, Dict, Optional

log = logging.getLogger(__name__)

def _env(name: str, default: Optional[str]=None) -> Optional[str]:
    v = os.environ.get(name)
    return v.strip() if v and str(v).strip() else default

def _now_ist() -> float:
    return time.time() + 5.5 * 3600

def _now_ist_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S IST", time.gmtime(_now_ist()))

# ------------ market hours helper (09:15–15:30 IST; Fri special same) ----------
def is_market_hours(ts: Optional[float]=None) -> bool:
    t = ts if ts is not None else _now_ist()
    lt = time.gmtime(t)
    # lt.tm_wday: Mon=0 … Sun=6
    if lt.tm_wday >= 5:   # Sat/Sun
        return False
    hh = lt.tm_hour; mm = lt.tm_min
    # Convert IST gmtime already
    minutes = hh*60 + mm
    open_m  = 9*60 + 15
    close_m = 15*60 + 30
    return open_m <= minutes <= close_m

# ------------ function resolution --------------------------------------------
def _resolve_refresh_func() -> Callable[..., Awaitable[Dict[str, Any]]]:
    # priority: OC_REFRESH_FUNC → DHAN_PROVIDER_FUNC → providers.dhan_oc.refresh_once
    path = _env("OC_REFRESH_FUNC")
    if not path:
        path = _env("DHAN_PROVIDER_FUNC")
        if not path:
            path = "providers.dhan_oc.refresh_once"
    mod_path, _, fn_name = path.rpartition(".")
    if not mod_path:
        raise ImportError(f"OC refresh func invalid: {path!r}")
    mod = importlib.import_module(mod_path)
    fn = getattr(mod, fn_name, None)
    if fn is None:
        raise ImportError(f"{path}: function not found")
    if not asyncio.iscoroutinefunction(fn):
        raise TypeError(f"{path}: must be async def")
    log.info("oc_refresh_shim: selected %s (async=True)", fn_name)
    return fn

# Global single-flight state
_lock = asyncio.Lock()
_inflight: Optional[asyncio.Task] = None
_last_snapshot: Optional[Dict[str, Any]] = None
_last_ts: Optional[float] = None

async def _do_refresh(p, fn: Callable[..., Awaitable[Dict[str, Any]]]) -> Dict[str, Any]:
    global _last_snapshot, _last_ts
    snap = await fn(p)
    _last_snapshot = snap
    _last_ts = time.time()
    return snap

def get_refresh() -> Callable[..., Awaitable[Dict[str, Any]]]:
    """
    Returns an async function(p)->dict that:
      - ensures single-flight
      - returns last snapshot if refresh is inflight
    """
    fn = _resolve_refresh_func()

    async def refresh_once(p) -> Dict[str, Any]:
        global _inflight
        if _lock.locked():
            # Someone else is refreshing: serve last snapshot (if any) to avoid overlap
            if _last_snapshot is not None:
                log.debug("oc_refresh: single-flight in progress → serving cached snapshot")
                return _last_snapshot
            # No cache yet → wait for inflight to finish to avoid empty result
            if _inflight:
                try:
                    return await _inflight
                except Exception:
                    # if inflight failed, fall through to try ourselves
                    pass

        async with _lock:
            # create and store inflight task so parallel callers (if any) can await
            _inflight = asyncio.create_task(_do_refresh(p, fn))
            try:
                return await _inflight
            finally:
                _inflight = None

    return refresh_once

# Convenience: sometimes callers import refresh_once directly
refresh_once = get_refresh()
