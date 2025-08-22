# analytics/oc_refresh_shim.py
# ------------------------------------------------------------
# Safe async shim around analytics.oc_refresh
# - Always exposes `async refresh_once(...)` (awaitable) even if target is sync
# - Accepts arg variants: (), (None,), ({},)
# - Extracts a snapshot from the target's return value (dict/tuple/list)
# - Publishes snapshot to:
#     1) analytics.oc_refresh.set_snapshot / update_snapshot if available
#     2) shim-local _LAST_SNAPSHOT (fallback)
# - `get_snapshot()` first tries oc_refresh.get_snapshot / accessors; else returns _LAST_SNAPSHOT
# ------------------------------------------------------------
from __future__ import annotations
import importlib
import inspect
import logging
import asyncio
from typing import Any, Callable, Optional, Tuple

_log = logging.getLogger(__name__)

# ------- Module discovery -------
try:
    _m = importlib.import_module("analytics.oc_refresh")
except Exception as e:
    _log.exception("oc_refresh_shim: failed to import analytics.oc_refresh: %s", e)
    _m = None

# ------- Snapshot storage (fallback) -------
_LAST_SNAPSHOT: Any = None

# ------- Helpers: score/select a callable -------
def _required_positional_count(fn: Callable) -> int:
    try:
        sig = inspect.signature(fn)
    except Exception:
        return 0
    req = 0
    for p in sig.parameters.values():
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD) and p.default is inspect._empty:
            req += 1
    return req

def _score_name(name: str) -> int:
    n = name.lower()
    order = [
        "refresh_once",
        "refresh_now",
        "run_once",
        "refresh",
        "do_refresh", "refresh_one", "do_oc_refresh",
        "refresh_tick", "refresh_snapshot", "oc_refresh",
        "update_levels", "fetch_levels",
    ]
    if n in order:
        return order.index(n)
    if "refresh" in n:
        return 50
    if any(k in n for k in ("snapshot", "tick", "levels", "oc")):
        return 60
    return 999

def _pick_refresh_callable(mod) -> Tuple[Optional[Callable[..., Any]], str, bool]:
    cands = []
    for name in dir(mod):
        obj = getattr(mod, name, None)
        if callable(obj):
            sc = _score_name(name)
            if sc < 999:
                cands.append((sc, _required_positional_count(obj), name, obj, inspect.iscoroutinefunction(obj)))
    if not cands:
        return None, "", False
    cands.sort(key=lambda t: (t[0], t[1]))  # preference, then fewer required args
    _, _req, nm, fn, is_coro = cands[0]
    return fn, nm, is_coro

_FN, _FN_name, _FN_is_async = (None, "", False)
if _m is not None:
    _FN, _FN_name, _FN_is_async = _pick_refresh_callable(_m)
    if _FN:
        _log.info("oc_refresh_shim: selected %s (async=%s)", _FN_name, _FN_is_async)
    else:
        _log.error("oc_refresh_shim: NO suitable refresh function found in analytics.oc_refresh")

# ------- Snapshot publishing / extraction -------
def _try_call_setters(snap: Any) -> None:
    """Push snapshot into oc_refresh module if it provides a setter, else keep local."""
    global _LAST_SNAPSHOT
    _LAST_SNAPSHOT = snap
    if _m is None:
        return
    for setter_name in ("set_snapshot", "update_snapshot", "publish_snapshot", "store_snapshot"):
        setter = getattr(_m, setter_name, None)
        if callable(setter):
            try:
                setter(snap)
                _log.debug("oc_refresh_shim: snapshot published via %s()", setter_name)
                return
            except Exception:
                _log.warning("oc_refresh_shim: %s() raised; keeping local snapshot", setter_name)

def _looks_like_snapshot(d: Any) -> bool:
    if not isinstance(d, dict):
        return False
    keys = set(k.lower() for k in d.keys())
    # Heuristics: presence of spot + any level-ish keys OR symbol/expiry
    if "spot" in keys and ({"s1","s2","r1","r2"} & keys or "levels" in keys):
        return True
    if "symbol" in keys and "expiry" in keys and "spot" in keys:
        return True
    return False

def _extract_snapshot(ret: Any) -> Optional[Any]:
    """Extract snapshot from various return formats."""
    # direct dict
    if _looks_like_snapshot(ret):
        return ret
    # tuple/list → try each element
    if isinstance(ret, (tuple, list)):
        for x in ret:
            if _looks_like_snapshot(x):
                return x
    # object with attribute 'snapshot' or 'data'
    for attr in ("snapshot", "data"):
        if hasattr(ret, attr):
            try:
                val = getattr(ret, attr)
                if _looks_like_snapshot(val):
                    return val
            except Exception:
                pass
    return None

def _build_variants(args: tuple, kwargs: dict) -> list[tuple]:
    variants: list[tuple] = []
    if args or kwargs:
        variants.append(("caller", args, kwargs))
    variants.extend([
        ("zero", tuple(), {}),
        ("none", (None,), {}),
        ("dict", ({},), {}),
    ])
    return variants

async def _call_async(fn: Callable, args: tuple, kwargs: dict) -> Any:
    res = fn(*args, **kwargs)
    if inspect.isawaitable(res):
        return await res
    return res

# ------- Public API -------
async def refresh_once(*args, **kwargs) -> Any:
    """
    Async, safe, single-shot OC refresh entrypoint.
    Calls underlying oc_refresh function with arg variants, and
    extracts+publishes a snapshot if present in return value.
    """
    if _FN is None:
        _log.warning("oc_refresh_shim: Using NO-OP refresh_once() (no target)")
        return {"status": "noop", "message": "No refresh function available (shim)"}

    variants = _build_variants(args, kwargs)

    # async target
    if _FN_is_async:
        last_err: Optional[BaseException] = None
        for tag, a, k in variants:
            try:
                ret = await _call_async(_FN, a, k)
                snap = _extract_snapshot(ret)
                if snap is not None:
                    _try_call_setters(snap)
                return ret
            except TypeError as e:
                last_err = e
                continue
            except Exception as e:
                _log.exception("oc_refresh_shim: async call (%s) raised", tag)
                return {"status": "error", "message": str(e)}
        _log.error("oc_refresh_shim: async arg-mismatch across variants: %s", last_err)
        return {"status": "error", "message": str(last_err)}

    # sync target -> run in thread
    loop = asyncio.get_running_loop()
    last_te: Optional[TypeError] = None
    for tag, a, k in variants:
        try:
            ret = await loop.run_in_executor(None, lambda: _FN(*a, **k))  # type: ignore[misc]
            snap = _extract_snapshot(ret)
            if snap is not None:
                _try_call_setters(snap)
            return ret
        except TypeError as e:
            last_te = e
            continue
        except Exception as e:
            _log.exception("oc_refresh_shim: sync call (%s) raised", tag)
            return {"status": "error", "message": str(e)}
    _log.error("oc_refresh_shim: sync arg-mismatch across variants: %s", last_te)
    return {"status": "error", "message": str(last_te)}

def get_snapshot():
    """
    Try multiple ways to fetch latest snapshot.
    Order:
      1) analytics.oc_refresh.get_snapshot()/latest_snapshot()/snapshot()/export_snapshot()
      2) analytics.oc_refresh.SNAPSHOT/LATEST (common globals)
      3) shim-local _LAST_SNAPSHOT
    """
    # Module accessors
    if _m is not None:
        for nm in ("get_snapshot", "latest_snapshot", "snapshot", "export_snapshot"):
            fn = getattr(_m, nm, None)
            if callable(fn):
                try:
                    val = fn()
                    if val is not None:
                        return val
                except Exception:
                    pass
        # Module globals
        for nm in ("SNAPSHOT", "LATEST", "LAST_SNAPSHOT"):
            if hasattr(_m, nm):
                try:
                    val = getattr(_m, nm)
                    if val is not None:
                        return val
                except Exception:
                    pass
    # Fallback to our memory
    return _LAST_SNAPSHOT
