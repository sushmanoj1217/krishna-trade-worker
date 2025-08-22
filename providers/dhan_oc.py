# providers/dhan_oc.py
# ------------------------------------------------------------
# Dhan-backed live Option-Chain provider for oc_refresh.
# Strategy:
#   - First, try to call your existing integrations (e.g. integrations.dhan_oc)
#     with tolerant function discovery. We *trust* your integration to hit Dhan.
#   - Normalize to unified snapshot schema used by the bot.
#   - Add light rate-limit: respect OC_REFRESH_SECS; simple 429 cooldown.
#
# Env required (you already set most of these):
#   OC_SYMBOL                NIFTY|BANKNIFTY|FINNIFTY     (default NIFTY)
#   DHAN_UNDERLYING_SEG      e.g. IDX_I                   (required by your integration)
#   DHAN_UNDERLYING_SCRIP    e.g. 13 (NIFTY)             (or use DHAN_UNDERLYING_SCRIP_MAP)
#   DHAN_UNDERLYING_SCRIP_MAP like "NIFTY=13,BANKNIFTY=25,FINNIFTY=27"
#
# Optional:
#   OC_REFRESH_SECS          default 12
#   DHAN_429_COOLDOWN_SEC    default 30
#
# Public API exposed (for oc_refresh discovery):
#   - async refresh_once() -> {"snapshot": {...}, ...}  (zero-arg)
# ------------------------------------------------------------
from __future__ import annotations

import os, time, importlib, inspect
from typing import Any, Dict, Optional, Tuple

_last_fetch_ts: Optional[int] = None
_last_snapshot: Optional[Dict[str, Any]] = None
_cooldown_until: int = 0

# ------------- small utils -------------
def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    return v.strip() if v and str(v).strip() else default

def _now() -> int:
    return int(time.time())

def _parse_map(s: Optional[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not s: return out
    parts = [p.strip() for p in s.split(",") if p.strip()]
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            out[k.strip().upper()] = v.strip()
    return out

def _get_symbol() -> str:
    sym = (_env("OC_SYMBOL") or "NIFTY").upper()
    if sym not in {"NIFTY","BANKNIFTY","FINNIFTY"}:
        sym = "NIFTY"
    return sym

def _get_security_id(sym: str) -> Optional[str]:
    # Prefer explicit DHAN_UNDERLYING_SCRIP, else map
    sid = _env("DHAN_UNDERLYING_SCRIP")
    if sid: return sid
    mp = _parse_map(_env("DHAN_UNDERLYING_SCRIP_MAP"))
    return mp.get(sym)

def _pick_func(mod, names: list[str]):
    for nm in names:
        if hasattr(mod, nm) and callable(getattr(mod, nm)):
            return getattr(mod, nm), nm
    return None, ""

def _is_snapshot(d: Any) -> bool:
    if not isinstance(d, dict): return False
    k = set(x.lower() for x in d.keys())
    if {"symbol","spot"} <= k and ({"s1","s2","r1","r2"} & k):
        return True
    if {"symbol","expiry","spot"} <= k:
        return True
    if "levels" in k and "spot" in k:
        return True
    return False

def _extract_snapshot(ret: Any) -> Optional[dict]:
    # direct dict?
    if _is_snapshot(ret): return ret
    # dict under common keys
    if isinstance(ret, dict):
        for key in ("snapshot","data","result"):
            v = ret.get(key)
            if _is_snapshot(v): return v  # type: ignore
    # tuple/list: search first dict-looking snapshot
    if isinstance(ret, (tuple, list)):
        for it in ret:
            if _is_snapshot(it): return it
        if ret and isinstance(ret[0], dict) and _is_snapshot(ret[0]):
            return ret[0]
    # object attrs
    for attr in ("snapshot","data","result"):
        try:
            v = getattr(ret, attr)
            if _is_snapshot(v): return v
        except Exception:
            pass
    return None

def _merge_defaults(s: Dict[str, Any]) -> Dict[str, Any]:
    # Ensure mandatory keys with safe defaults
    out = dict(s)
    out.setdefault("symbol", _get_symbol())
    out.setdefault("expiry", out.get("exp") or "")
    out.setdefault("spot", out.get("underlying") or out.get("underlying_value") or out.get("ltp") or 0.0)
    for k in ("s1","s2","r1","r2","pcr","max_pain","ce_oi_delta","pe_oi_delta","mv"):
        out.setdefault(k, None)
    # provider tags
    out["source"] = "provider"
    out.setdefault("ts", _now())
    return out

# ------------- core call into your Dhan integration -------------
_CANDIDATE_MODULES = [
    "integrations.dhan_oc",
    "dhan.oc",
    "dhan_integration.oc",
    "oc.dhan",
]
_CANDIDATE_FUNCS = [
    # most likely:
    "refresh_once", "get_oc_snapshot", "get_snapshot", "fetch_levels", "compute_snapshot",
    # generic:
    "refresh", "run_once", "oc_refresh",
]

async def _invoke_callable(fn, *args, **kwargs):
    res = fn(*args, **kwargs)
    if inspect.isawaitable(res):
        res = await res
    return res

async def _call_dhan_integration() -> Dict[str, Any]:
    sym = _get_symbol()
    sid = _get_security_id(sym)
    seg = _env("DHAN_UNDERLYING_SEG", "IDX_I")

    # Build param packs we will try to pass
    param_variants = [
        (),  # zero-arg
        ({"symbol": sym},),
        ({"symbol": sym, "segment": seg},),
        ({"security_id": sid or "", "segment": seg},),
        ({"symbol": sym, "security_id": sid or "", "segment": seg},),
    ]

    last_err: Optional[Exception] = None
    for mod_name in _CANDIDATE_MODULES:
        try:
            m = importlib.import_module(mod_name)
        except Exception as e:
            last_err = e
            continue
        fn, fname = _pick_func(m, _CANDIDATE_FUNCS)
        if not fn:
            continue

        # Try tolerant parameter packs
        for pack in param_variants:
            try:
                if isinstance(pack, tuple) and len(pack) == 1 and isinstance(pack[0], dict):
                    ret = await _invoke_callable(fn, **pack[0])
                else:
                    ret = await _invoke_callable(fn, *pack)
                snap = _extract_snapshot(ret)
                if isinstance(snap, dict):
                    return _merge_defaults(snap)
            except TypeError:
                # wrong signature; try next
                continue
            except Exception as e:
                # handle 429 cooldown hint
                msg = str(e).lower()
                if "429" in msg or "too many" in msg:
                    raise RuntimeError("DHAN_429") from e
                last_err = e
                # try next variant
                continue

    # If we reached here, no suitable callable succeeded
    if last_err:
        raise last_err
    raise RuntimeError("No usable Dhan integration function found")

# ------------- public API -------------
async def refresh_once() -> Dict[str, Any]:
    """
    Respect OC_REFRESH_SECS cadence; apply 429 cooldown;
    then call your Dhan integration and return unified payload.
    """
    global _last_fetch_ts, _last_snapshot, _cooldown_until

    now = _now()
    if now < _cooldown_until:
        # return cached snapshot if available, else raise
        if _last_snapshot:
            return {"status": "cooldown", "reason": "429_cooldown", "snapshot": _last_snapshot}
        raise RuntimeError(f"429 cooldown; wait {(_cooldown_until-now)}s")

    cadence = int(_env("OC_REFRESH_SECS", "12") or "12")
    if _last_fetch_ts and _last_snapshot and now - _last_fetch_ts < max(3, cadence):
        return {"status": "cached", "reason": "", "snapshot": _last_snapshot}

    try:
        snap = await _call_dhan_integration()
    except RuntimeError as e:
        if str(e) == "DHAN_429":
            cd = int(_env("DHAN_429_COOLDOWN_SEC", "30") or "30")
            _cooldown_until = now + max(10, min(120, cd))
            if _last_snapshot:
                return {"status": "cooldown", "reason": "429_cooldown", "snapshot": _last_snapshot}
            raise
        raise

    _last_fetch_ts = now
    _last_snapshot = snap
    return {"status": "ok", "reason": "", "snapshot": snap}
