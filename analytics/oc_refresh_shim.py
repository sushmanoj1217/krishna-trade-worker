# analytics/oc_refresh_shim.py
# ------------------------------------------------------------
# Safe shim to expose `refresh_once()` for callers (e.g. krishna_main.py)
# without breaking existing oc_refresh internals. It dynamically binds to
# whichever suitable function exists in analytics.oc_refresh and wraps it
# to avoid TypeError (arg-mismatch) crashes.
# ------------------------------------------------------------
from __future__ import annotations
import importlib
import inspect
import logging
from typing import Any, Callable, Optional, Tuple

_log = logging.getLogger(__name__)

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
    # Strong preferences first (lower is better)
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

def _pick_refresh_callable(mod) -> Tuple[Optional[Callable[..., Any]], str, int]:
    cands = []
    for name in dir(mod):
        obj = getattr(mod, name, None)
        if callable(obj):
            sc = _score_name(name)
            if sc < 999:
                cands.append((sc, _required_positional_count(obj), name, obj))
    if not cands:
        return None, "", 0
    # Sort by preference then fewer required args
    cands.sort(key=lambda t: (t[0], t[1]))
    _, req, nm, fn = cands[0]
    return fn, nm, req

# Bind target at import time
try:
    _m = importlib.import_module("analytics.oc_refresh")
except Exception as e:
    _log.exception("oc_refresh_shim: failed to import analytics.oc_refresh: %s", e)
    _FN = None
    _FN_name = ""
    _FN_req = 0
else:
    _FN, _FN_name, _FN_req = _pick_refresh_callable(_m)
    if _FN is None:
        _log.error("oc_refresh_shim: NO suitable refresh function found in analytics.oc_refresh")
    else:
        _log.info("oc_refresh_shim: selected %s (requires %d positional args)", _FN_name, _FN_req)

def refresh_once(*args, **kwargs) -> Any:
    """
    Safe single-shot OC refresh entrypoint.
    Tries 0-arg, then (None), then ({}) calling patterns.
    Never raises TypeError outward; returns dict with status on failure.
    """
    if _FN is None:
        _log.warning("oc_refresh_shim: Using NO-OP refresh_once()")
        return {"status": "noop", "message": "No refresh function available (shim)"}

    # Try zero-arg
    try:
        return _FN()
    except TypeError as e0:
        # Try None
        try:
            return _FN(None)
        except TypeError as e1:
            # Try empty dict
            try:
                return _FN({})
            except Exception as e2:
                _log.exception(
                    "oc_refresh_shim: refresh call failed (fn=%s). "
                    "Errors: zero-arg=%s | None=%s | {}=%s",
                    _FN_name, e0, e1, e2
                )
                return {"status": "error", "message": str(e2)}
        except Exception as e:
            _log.exception("oc_refresh_shim: refresh call (None) raised")
            return {"status": "error", "message": str(e)}
    except Exception as e:
        _log.exception("oc_refresh_shim: refresh call raised")
        return {"status": "error", "message": str(e)}

# Optional pass-throughs (nice to have): expose snapshot if module provides it
def get_snapshot():
    try:
        _m = importlib.import_module("analytics.oc_refresh")
        return getattr(_m, "get_snapshot")()
    except Exception:
        return None
