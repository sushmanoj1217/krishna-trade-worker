# providers/dhan_oc.py
# ------------------------------------------------------------
# Hard-bound Dhan OC provider:
#   - Calls your integration:  DHAN_PROVIDER_MODULE.DHAN_PROVIDER_FUNC
#   - Your case: async fetch_levels(p: utils.params.Params) -> Dict
#   - Builds a Params-like object from env (duck-typing) + injects Dhan creds
#   - Cadence cache + 429 cooldown
#   - Normalizes snapshot (adds source='provider', ts=now)
#
# Required env (set your real values):
#   OC_SYMBOL=NIFTY|BANKNIFTY|FINNIFTY       (default NIFTY)
#   DHAN_UNDERLYING_SEG=IDX_I
#   DHAN_UNDERLYING_SCRIP=13                 (or DHAN_UNDERLYING_SCRIP_MAP="NIFTY=13,BANKNIFTY=25,FINNIFTY=27")
#   DHAN_CLIENT_ID=YOUR_DHAN_CLIENT_ID       <-- IMPORTANT
#   DHAN_ACCESS_TOKEN=YOUR_ACCESS_TOKEN      <-- IMPORTANT
#
# Bind your integration explicitly:
#   DHAN_PROVIDER_MODULE=integrations.option_chain_dhan
#   DHAN_PROVIDER_FUNC=fetch_levels
#
# Optional:
#   OC_REFRESH_SECS=12
#   DHAN_429_COOLDOWN_SEC=30
# ------------------------------------------------------------
from __future__ import annotations

import os, time, importlib, inspect, logging
from types import SimpleNamespace
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)

_last_fetch_ts: Optional[int] = None
_last_snapshot: Optional[Dict[str, Any]] = None
_cooldown_until: int = 0

# ---------------- utils ----------------
def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    return v.strip() if v and str(v).strip() else default

def _now() -> int:
    return int(time.time())

def _to_float(x):
    try:
        if x in (None, "", "—"): return None
        return float(str(x).replace(",", "").strip())
    except Exception:
        return None

def _parse_map(s: Optional[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not s: return out
    for part in s.split(","):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        out[k.strip().upper()] = v.strip()
    return out

def _get_symbol() -> str:
    sym = (_env("OC_SYMBOL") or "NIFTY").upper()
    return sym if sym in {"NIFTY","BANKNIFTY","FINNIFTY"} else "NIFTY"

def _get_security_id(sym: str) -> Optional[str]:
    sid = _env("DHAN_UNDERLYING_SCRIP")
    if sid: return sid
    mp = _parse_map(_env("DHAN_UNDERLYING_SCRIP_MAP"))
    return mp.get(sym)

# ---------------- Params builder ----------------
def _build_params() -> Any:
    """
    Build a Params-like object. Inject BOTH creds and synonyms:
    - client-id/access-token into p.headers for direct HTTP use
    - and also as attributes (client_id, dhanClientId, access_token, accessToken)
      so your integration can pick whichever it expects.
    """
    sym = _get_symbol()
    seg = _env("DHAN_UNDERLYING_SEG", "IDX_I")
    sid = _get_security_id(sym)
    cadence = int(_env("OC_REFRESH_SECS", "12") or "12")

    client_id  = _env("DHAN_CLIENT_ID") or _env("CLIENT_ID")
    access_tok = _env("DHAN_ACCESS_TOKEN") or _env("ACCESS_TOKEN") or _env("TOKEN")

    # Create Params or a namespace
    try:
        from utils.params import Params  # type: ignore
        try:
            p = Params()
        except Exception:
            try:
                p = Params(symbol=sym, segment=seg, security_id=sid, oc_refresh_secs=cadence)  # type: ignore
            except Exception:
                p = SimpleNamespace()
    except Exception:
        p = SimpleNamespace()

    # Core trade params
    for k, v in {
        "symbol": sym,
        "oc_symbol": sym,
        "underlying_symbol": sym,
        "segment": seg,
        "underlying_segment": seg,
        "security_id": sid,
        "underlying_scrip": sid,
        "scrip": sid,
        "oc_refresh_secs": cadence,
        "refresh_secs": cadence,
    }.items():
        try: setattr(p, k, v)
        except Exception: pass

    # Dhan credentials — provide MANY synonyms + headers
    for k, v in {
        "client_id": client_id,
        "dhan_client_id": client_id,
        "clientId": client_id,
        "dhanClientId": client_id,
        "access_token": access_tok,
        "accessToken": access_tok,
        "token": access_tok,
    }.items():
        try: setattr(p, k, v)
        except Exception: pass

    # Standard headers used by Dhan v2 REST:
    #   'client-id': <client id>, 'access-token': <jwt>
    hdrs = {
        "client-id": client_id or "",
        "access-token": access_tok or "",
        "Content-Type": "application/json",
    }
    try: setattr(p, "headers", hdrs)
    except Exception: pass

    return p

# ---------------- Normalization ----------------
def _looks_like_snapshot(d: Any) -> bool:
    if not isinstance(d, dict): return False
    k = {str(x).lower() for x in d.keys()}
    if {"symbol","expiry","spot"} <= k: return True
    if "spot" in k and ({"s1","s2","r1","r2"} & k): return True
    if "levels" in k and "spot" in k: return True
    return False

def _normalize_snapshot(raw: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = dict(raw)

    lv = out.get("levels")
    if isinstance(lv, dict):
        def pick(*names):
            for n in names:
                if n in lv: return lv[n]
                if isinstance(n, str) and n.upper() in lv: return lv[n.upper()]
            return None
        out.setdefault("s1", _to_float(pick("s1","S1")))
        out.setdefault("s2", _to_float(pick("s2","S2")))
        out.setdefault("r1", _to_float(pick("r1","R1")))
        out.setdefault("r2", _to_float(pick("r2","R2")))

    for key in ("spot","s1","s2","r1","r2","pcr","max_pain","ce_oi_delta","pe_oi_delta"):
        if key in out:
            out[key] = _to_float(out.get(key))

    if not out.get("expiry"):
        exp = out.get("exp") or out.get("expiry_date") or out.get("expy") or ""
        out["expiry"] = exp

    if out.get("mv") is None:
        pcr = out.get("pcr"); mp = out.get("max_pain"); spot = out.get("spot")
        mv = ""
        try:
            score = 0
            if isinstance(pcr, (int,float)):
                score += 1 if float(pcr) >= 1.0 else -1
            if isinstance(mp, (int,float)) and isinstance(spot, (int,float)):
                score += 1 if float(mp) > float(spot) else -1
            if score > 0: mv = "bullish"
            elif score < 0: mv = "bearish"
        except Exception:
            pass
        out["mv"] = mv

    out.setdefault("symbol", _get_symbol())
    out["source"] = "provider"
    out.setdefault("ts", _now())
    return out

# ---------------- Resolver & caller ----------------
def _resolve_callable():
    mod_name = _env("DHAN_PROVIDER_MODULE", "integrations.option_chain_dhan")
    func_name = _env("DHAN_PROVIDER_FUNC", "fetch_levels")
    m = importlib.import_module(mod_name)
    fn = getattr(m, func_name)
    if not callable(fn):
        raise RuntimeError(f"{mod_name}.{func_name} is not callable")
    return fn, f"{mod_name}.{func_name}", inspect.iscoroutinefunction(fn)

async def _invoke(fn, *args, **kwargs):
    res = fn(*args, **kwargs)
    if inspect.isawaitable(res):
        res = await res
    return res

# ---------------- Public API ----------------
async def refresh_once() -> Dict[str, Any]:
    """
    Cadence cache; 429 cooldown; call your Dhan integration with a Params-like object.
    Returns: {"status": "...", "reason": "...", "snapshot": {...}}
    """
    global _last_fetch_ts, _last_snapshot, _cooldown_until

    now = _now()
    if now < _cooldown_until:
        if _last_snapshot:
            return {"status":"cooldown", "reason":"429_cooldown", "snapshot":_last_snapshot}
        raise RuntimeError(f"429 cooldown; wait {(_cooldown_until-now)}s")

    cadence = int(_env("OC_REFRESH_SECS", "12") or "12")
    if _last_fetch_ts and _last_snapshot and now - _last_fetch_ts < max(3, cadence):
        return {"status":"cached", "reason":"", "snapshot":_last_snapshot}

    fn, fqname, is_async = _resolve_callable()
    p = _build_params()

    # quick guard: ensure creds exist
    hdrs = getattr(p, "headers", {})
    if not hdrs.get("client-id") or not hdrs.get("access-token"):
        raise RuntimeError("Dhan creds missing: set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN in env")

    try:
        ret = await _invoke(fn, p)
    except Exception as e:
        msg = str(e).lower()
        if any(tok in msg for tok in ["429", "too many requests", "rate limit"]):
            cd = int(_env("DHAN_429_COOLDOWN_SEC", "30") or "30")
            _cooldown_until = now + max(10, min(120, cd))
            if _last_snapshot:
                return {"status":"cooldown", "reason":"429_cooldown", "snapshot":_last_snapshot}
        # bubble up so oc_refresh can fallback to sheets
        raise

    if isinstance(ret, dict) and ("snapshot" in ret and isinstance(ret["snapshot"], dict)):
        snap = _normalize_snapshot(ret["snapshot"])
    elif isinstance(ret, dict):
        snap = _normalize_snapshot(ret)
    else:
        try:
            snap = _normalize_snapshot(getattr(ret, "snapshot"))
        except Exception:
            raise RuntimeError(f"{fqname} did not return a snapshot-like dict")

    _last_fetch_ts = now
    _last_snapshot = snap
    return {"status":"ok", "reason":"", "snapshot":snap}
