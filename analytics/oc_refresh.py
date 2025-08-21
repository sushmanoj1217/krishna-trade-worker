import os
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from utils.logger import log
from utils.params import Params
from utils.cache import set_snapshot, get_snapshot as _get_snapshot
from integrations.option_chain_dhan import fetch_levels
from integrations import sheets as sh

@dataclass
class OCSnapshot:
    ts: datetime
    spot: float
    s1: float
    s2: float
    r1: float
    r2: float
    expiry: str
    vix: float | None
    pcr: float | None
    max_pain: float
    bias: str | None
    stale: bool = False

COOLDOWN = {"until": None}

def get_snapshot() -> OCSnapshot | None:
    return _get_snapshot()

async def day_oc_loop():
    """Refresh OC snapshot; write to OC_Live; respect rate-limit cooldown."""
    now = datetime.now(timezone.utc)
    until = COOLDOWN.get("until")
    if until and now < until:
        # don't spam; if no cache, log error
        if not _get_snapshot():
            raise RuntimeError("In cooldown and no OC cache available")
        await asyncio.sleep(1)
        return

    p = Params.from_env()
    try:
        oc = await fetch_levels(p)
        snap = OCSnapshot(
            ts=datetime.now(timezone.utc),
            spot=oc["spot"],
            s1=oc["s1"], s2=oc["s2"], r1=oc["r1"], r2=oc["r2"],
            expiry=oc["expiry"],
            vix=oc.get("vix"),
            pcr=oc.get("pcr"),
            max_pain=oc["max_pain"],
            bias=oc.get("bias_tag"),
            stale=False
        )
        set_snapshot(snap)
        # write to sheet (best-effort)
        try:
            await sh.log_oc_live(snap)
        except Exception as e:
            log.warning(f"OC Live write failed: {e}")
    except fetch_levels.TooManyRequests as e:
        log.warning(f"Dhan 429: {e} → cooldown 30s")
        COOLDOWN["until"] = datetime.now(timezone.utc) + timedelta(seconds=30)
        raise
    except Exception as e:
        log.error(f"OC refresh failed: {e}")
        if not _get_snapshot():
            raise
        # else keep old cache
        return
# ===== Back-compat alias (smart resolver, drop-in) =====
# Goal: Always export `refresh_once` from this module, even if the concrete
# function was renamed during refactors. Prefer well-known names; otherwise
# heuristically pick a plausible refresh function. As a last resort, bind a
# no-op that logs a clear warning so the app doesn't crash on import.

import inspect
import logging

_log = logging.getLogger(__name__)

def _pick_refresh_callable():
    """
    Returns a callable that performs a single OC refresh, or None if not found.
    Preference order:
      1) Known function names we've used historically.
      2) Any callable with "refresh" in its name.
      3) Heuristic fallbacks with keywords: snapshot/tick/levels/oc + refresh-ish.
    """
    # 1) Exact known names (most stable first)
    exact_candidates = [
        "refresh_once",
        "refresh_now",
        "run_once",
        "refresh",
        "do_refresh",
        "refresh_one",
        "do_oc_refresh",
        "refresh_snapshot",
        "oc_refresh",
        "refresh_tick",
        "update_levels",
        "fetch_levels",
    ]
    for name in exact_candidates:
        fn = globals().get(name)
        if callable(fn):
            return fn

    # 2) Any callable with "refresh" in the name
    dynamic = []
    for name, obj in list(globals().items()):
        if callable(obj) and isinstance(name, str):
            n = name.lower()
            if "refresh" in n:
                dynamic.append((name, obj))
    if dynamic:
        # Prefer ones that look single-shot (contain 'once', 'now', 'tick')
        def score(item):
            name = item[0].lower()
            s = 0
            if "once" in name or "now" in name: s += 5
            if "tick" in name: s += 3
            if "snapshot" in name: s += 2
            if "oc" in name or "levels" in name: s += 1
            return -s  # smaller is better for sorting
        dynamic.sort(key=score)
        return dynamic[0][1]

    # 3) Heuristic: keywords mix (snapshot/tick/levels/oc) even if not 'refresh'
    kw = ("snapshot", "tick", "levels", "oc")
    pool = []
    for name, obj in list(globals().items()):
        if callable(obj) and isinstance(name, str):
            n = name.lower()
            if any(k in n for k in kw):
                pool.append((name, obj))
    if pool:
        def score2(item):
            name = item[0].lower()
            s = 0
            if "snapshot" in name: s += 4
            if "tick" in name: s += 3
            if "levels" in name: s += 2
            if "oc" in name: s += 1
            return -s
        pool.sort(key=score2)
        return pool[0][1]

    return None

# ===== Back-compat resolver + safe wrapper for refresh_once =====
import inspect
import logging

_log = logging.getLogger(__name__)

def _required_positional_count(fn):
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
    s = 100
    # Prefer known names
    if n == "refresh_once": s = 0
    elif n == "refresh_now": s = 1
    elif n == "run_once": s = 2
    elif n == "refresh": s = 3
    elif n in ("do_refresh", "refresh_one", "do_oc_refresh"): s = 4
    # Generic refresh-ish
    elif "refresh" in n: s = 5
    # Then OC-related helpers
    elif any(k in n for k in ("snapshot", "tick", "levels", "oc", "fetch_levels", "update_levels")):
        s = 6
    return s

def _pick_refresh_callable():
    """
    Choose the best available single-shot refresh callable.
    Preference by name -> then fewer required args.
    """
    cands = []
    for name, obj in list(globals().items()):
        if callable(obj):
            sc = _score_name(name)
            if sc < 100:
                cands.append((sc, _required_positional_count(obj), name, obj))
    # Also consider any function that contains 'refresh' in name
    for name, obj in list(globals().items()):
        if callable(obj):
            n = name.lower()
            if "refresh" in n and _score_name(name) >= 100:
                cands.append((5, _required_positional_count(obj), name, obj))
    # If nothing matched, consider OC helpers as last resort
    if not cands:
        for name, obj in list(globals().items()):
            if callable(obj):
                n = name.lower()
                if any(k in n for k in ("snapshot", "tick", "levels", "oc", "fetch_levels", "update_levels")):
                    cands.append((6, _required_positional_count(obj), name, obj))

    if not cands:
        return None

    # Sort: lower score first, then fewer required args first
    cands.sort(key=lambda t: (t[0], t[1]))
    return cands[0][3], cands[0][2], cands[0][1]

# If user already defined refresh_once, keep it.
if "refresh_once" in globals() and callable(globals()["refresh_once"]):
    _log.debug("oc_refresh: using existing refresh_once()")
else:
    _picked = _pick_refresh_callable()
    if _picked is None:
        _FN = None
        _FN_name = None
        _FN_req = 0
        _log.error("oc_refresh: NO concrete refresh function found. Binding NO-OP.")
    else:
        _FN, _FN_name, _FN_req = _picked
        _log.info("oc_refresh: selected %s (requires %s positional args)", _FN_name, _FN_req)

    def refresh_once(*args, **kwargs):  # type: ignore[assignment]
        """
        Safe single-shot entry point used by callers.
        Tries to call the chosen function with compatible arguments.
        Never raises TypeError outward; returns a dict with status.
        """
        if _FN is None:
            _log.warning("oc_refresh: Using NO-OP refresh_once()")
            return {"status": "noop", "message": "No refresh function available"}

        # 1) Try zero-arg call
        try:
            return _FN()
        except TypeError as e0:
            # 2) If it needs one param (common: p/params), try None
            try:
                return _FN(None)
            except TypeError as e1:
                # 3) Try empty dict
                try:
                    return _FN({})
                except Exception as e2:
                    _log.exception(
                        "oc_refresh: refresh call failed (fn=%s). "
                        "Errors: zero-arg=%s | None=%s | {}=%s",
                        _FN_name, e0, e1, e2
                    )
                    return {"status": "error", "message": str(e2)}
            except Exception as e:
                _log.exception("oc_refresh: refresh call (None) raised")
                return {"status": "error", "message": str(e)}
        except Exception as e:
            _log.exception("oc_refresh: refresh call raised")
            return {"status": "error", "message": str(e)}

# ===== /Back-compat resolver + safe wrapper =====
