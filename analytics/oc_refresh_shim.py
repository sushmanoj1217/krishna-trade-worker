# analytics/oc_refresh_shim.py
# ------------------------------------------------------------
# Safe async shim to expose `refresh_once()` for callers (e.g. krishna_main.py).
# It dynamically binds to whichever suitable function exists in
# analytics.oc_refresh and wraps it so that callers can always `await refresh_once()`.
# - If target is sync -> runs in thread (async wrapper).
# - If target is async -> awaited normally.
# - Arg mismatch -> tries (), (None,), ({}) variants safely.
# ------------------------------------------------------------
from __future__ import annotations
import importlib
import inspect
import logging
import asyncio
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
    # Lower is better (preference order)
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

def _pick_refresh_callable(mod) -> Tuple[Optional[Callable[..., Any]], str, int, bool]:
    cands = []
    for name in dir(mod):
        obj = getattr(mod, name, None)
        if callable(obj):
            sc = _score_name(name)
            if sc < 999:
                req = _required_positional_count(obj)
                is_coro = inspect.iscoroutinefunction(obj)
                cands.append((sc, req, name, obj, is_coro))
    if not cands:
        return None, "", 0, False
    # Sort by preference then fewer required args
    cands.sort(key=lambda t: (t[0], t[1]))
    _, req, nm, fn, is_coro = cands[0]
    return fn, nm, req, is_coro

# Bind target at import-time
try:
    _m = importlib.import_module("analytics.oc_refresh")
except Exception as e:
    _log.exception("oc_refresh_shim: failed to import analytics.oc_refresh: %s", e)
    _FN: Optional[Callable[..., Any]] = None
    _FN_name = ""
    _FN_req = 0
    _FN_is_coro = False
else:
    _FN, _FN_name, _FN_req, _FN_is_coro = _pick_refresh_callable(_m)
    if _FN is None:
        _log.error("oc_refresh_shim: NO suitable refresh function found in analytics.oc_refresh")
    else:
        _log.info(
            "oc_refresh_shim: selected %s (requires %d positional args, async=%s)",
            _FN_name, _FN_req, _FN_is_coro
        )

def _build_variants(args: tuple, kwargs: dict) -> list[tuple]:
    """
    Build call-argument variants.
    If caller passed args/kwargs, try them first; else try (), (None,), ({},).
    """
    variants: list[tuple] = []
    if args or kwargs:
        variants.append(("caller", args, kwargs))
    # Fallback patterns for legacy single-param APIs
    variants.extend([
        ("zero", tuple(), {}),
        ("none", (None,), {}),
        ("dict", ({},), {}),
    ])
    return variants

async def _call_async(fn: Callable, args: tuple, kwargs: dict) -> Any:
    res = fn(*args, **kwargs)
    # If fn returned a coroutine, await it; else return value directly
    if inspect.isawaitable(res):
        return await res
    return res

async def refresh_once(*args, **kwargs) -> Any:
    """
    Async, safe, single-shot OC refresh entrypoint.
    Always awaitable. Never raises bare TypeError outward due to arg mismatch.
    Returns whatever underlying fn returns (tuple/dict/obj).
    """
    if _FN is None:
        _log.warning("oc_refresh_shim: Using NO-OP refresh_once()")
        return {"status": "noop", "message": "No refresh function available (shim)"}

    variants = _build_variants(args, kwargs)

    # If target itself is declared async, try calling it first with variants.
    if _FN_is_coro:
        last_err: Optional[BaseException] = None
        for tag, a, k in variants:
            try:
                return await _call_async(_FN, a, k)
            except TypeError as e:
                last_err = e
                continue
            except Exception as e:
                _log.exception("oc_refresh_shim: async fn call (%s) raised", tag)
                return {"status": "error", "message": str(e)}
        _log.error("oc_refresh_shim: async fn arg-mismatch across variants: %s", last_err)
        return {"status": "error", "message": str(last_err)}

    # For sync targets, run in a thread (so caller can always await us)
    loop = asyncio.get_running_loop()
    last_te: Optional[TypeError] = None
    for tag, a, k in variants:
        try:
            return await loop.run_in_executor(None, lambda: _FN(*a, **k))  # type: ignore[misc]
        except TypeError as e:
            last_te = e
            continue
        except Exception as e:
            _log.exception("oc_refresh_shim: sync fn call (%s) raised", tag)
            return {"status": "error", "message": str(e)}
    _log.error("oc_refresh_shim: sync fn arg-mismatch across variants: %s", last_te)
    return {"status": "error", "message": str(last_te)}

# Optional helper passthrough
def get_snapshot():
    try:
        _m = importlib.import_module("analytics.oc_refresh")
        return getattr(_m, "get_snapshot")()
    except Exception:
        return None
