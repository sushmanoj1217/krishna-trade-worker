# providers/dhan_oc.py
# ------------------------------------------------------------
# Direct Dhan Option-Chain provider (no intermediate integration).
# Uses v2 endpoints with headers:
#   'access-token': <JWT>, 'client-id': <Client ID>
#
# Env (required):
#   DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN
#   OC_SYMBOL=NIFTY|BANKNIFTY|FINNIFTY (default NIFTY)
#   DHAN_UNDERLYING_SEG=IDX_I
#   DHAN_UNDERLYING_SCRIP=13   (or DHAN_UNDERLYING_SCRIP_MAP="NIFTY=13,BANKNIFTY=25,FINNIFTY=27")
#
# Optional:
#   OC_REFRESH_SECS=12
#   DHAN_429_COOLDOWN_SEC=30
#
# Public API (discovered by analytics.oc_refresh):
#   async refresh_once() -> {"status","reason","snapshot"}
# Snapshot keys:
#   symbol, expiry, spot, s1,s2,r1,r2, pcr, max_pain,
#   ce_oi_delta, pe_oi_delta, mv, ts, asof, age_sec, source="provider"
# ------------------------------------------------------------
from __future__ import annotations

import os, time, json, logging, math
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger(__name__)

try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore
import urllib.request, urllib.error

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

def _strike_step(sym: str) -> int:
    return 100 if sym == "BANKNIFTY" else 50

def _round_down(x: float, step: int) -> float:
    return math.floor(x / step) * step

def _mv_from(pcr: Optional[float], max_pain: Optional[float], spot: Optional[float],
             ce_d: Optional[float], pe_d: Optional[float]) -> str:
    """Primary: PCR (>=1 bullish) + MP (>spot bullish); Tie-break: OIΔ (PEΔ>CEΔ ⇒ bullish)."""
    score = 0
    try:
        if isinstance(pcr, (int,float)):
            score += 1 if float(pcr) >= 1.0 else -1
    except Exception:
        pass
    try:
        if isinstance(max_pain, (int,float)) and isinstance(spot, (int,float)):
            score += 1 if float(max_pain) > float(spot) else -1
    except Exception:
        pass
    if score > 0: 
        return "bullish"
    if score < 0: 
        return "bearish"
    # tie → use OI deltas if available
    if isinstance(ce_d, (int,float)) and isinstance(pe_d, (int,float)) and ce_d != pe_d:
        return "bullish" if pe_d > ce_d else "bearish"
    return ""

# ---------------- HTTP helpers ----------------
_BASE = "https://api.dhan.co/v2"
_UA = _env("DHAN_UA", "Mozilla/5.0")

def _headers() -> Dict[str,str]:
    cid = _env("DHAN_CLIENT_ID")
    tok = _env("DHAN_ACCESS_TOKEN")
    if not cid or not tok:
        raise RuntimeError("Dhan creds missing: set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN")
    return {
        "Content-Type": "application/json",
        "access-token": tok,
        "client-id": cid,
        "User-Agent": _UA,
    }

def _post(url: str, body: Dict[str, Any]) -> Tuple[int, bytes]:
    h = _headers()
    data = json.dumps(body).encode("utf-8")

    global _cooldown_until
    now = _now()
    if now < _cooldown_until:
        raise RuntimeError(f"429 cooldown active {(_cooldown_until-now)}s")

    if requests is not None:
        for attempt in range(2):
            r = requests.post(url, headers=h, data=data, timeout=12)
            if r.status_code == 429:
                _cooldown_until = _now() + int(_env("DHAN_429_COOLDOWN_SEC","30") or "30")
                raise RuntimeError("429 Too Many Requests")
            if r.status_code >= 400:
                return r.status_code, r.content
            return r.status_code, r.content
    else:
        req = urllib.request.Request(url, data=data, headers=h, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=12) as resp:
                return resp.getcode(), resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 429:
                _cooldown_until = _now() + int(_env("DHAN_429_COOLDOWN_SEC","30") or "30")
                raise RuntimeError("429 Too Many Requests")
            return e.code, e.read()
    return 599, b""

# ---------------- Dhan OC fetch ----------------
def _pick_expiry(sym: str, seg: str, sid: str) -> str:
    code, body = _post(f"{_BASE}/optionchain/expirylist", {
        "UnderlyingScrip": int(sid),
        "UnderlyingSeg": seg
    })
    try:
        js = json.loads(body.decode("utf-8"))
    except Exception:
        js = {"status":"failed","Data":{"999":"JSON decode error"}}
    if code != 200 or js.get("status") != "success":
        raise RuntimeError(f"expirylist failed: HTTP {code} body={js}")
    arr = js.get("data") or []
    if not arr:
        raise RuntimeError("expirylist empty")
    today = time.strftime("%Y-%m-%d", time.gmtime(_now()+19800))
    exp = sorted(arr)[0]
    for e in sorted(arr):
        if e >= today:
            exp = e
            break
    return exp

def _fetch_chain(sym: str, seg: str, sid: str, exp: str) -> Dict[str, Any]:
    code, body = _post(f"{_BASE}/optionchain", {
        "UnderlyingScrip": int(sid),
        "UnderlyingSeg": seg,
        "Expiry": exp
    })
    try:
        js = json.loads(body.decode("utf-8"))
    except Exception:
        js = {"status":"failed","Data":{"999":"JSON decode error"}}
    if code != 200:
        raise RuntimeError(f"optionchain HTTP {code}: {js}")
    if js.get("status") != "success":
        raise RuntimeError(json.dumps(js))
    return js.get("data") or {}

def _compute_from_chain(sym: str, data: Dict[str, Any], exp: str) -> Dict[str, Any]:
    spot = _to_float(data.get("last_price"))
    oc = data.get("oc") or {}
    if not isinstance(oc, dict):
        oc = {}

    tot_ce_oi = tot_pe_oi = 0.0
    tot_ce_prev = tot_pe_prev = 0.0
    strike_sum_map = {}

    for k, v in oc.items():
        try:
            strike = float(k)
        except Exception:
            strike = _to_float(k)
        if not isinstance(v, dict):
            continue
        ce = v.get("ce") or {}
        pe = v.get("pe") or {}
        ce_oi = _to_float(ce.get("oi")) or 0.0
        pe_oi = _to_float(pe.get("oi")) or 0.0
        ce_prev = _to_float(ce.get("previous_oi")) or 0.0
        pe_prev = _to_float(pe.get("previous_oi")) or 0.0

        tot_ce_oi += ce_oi; tot_pe_oi += pe_oi
        tot_ce_prev += ce_prev; tot_pe_prev += pe_prev

        if strike is not None:
            strike_sum_map[float(strike)] = (ce_oi + pe_oi)

    pcr = None
    if tot_ce_oi > 0:
        pcr = tot_pe_oi / tot_ce_oi

    ce_d = None
    pe_d = None
    if tot_ce_prev > 0 or tot_pe_prev > 0:
        ce_d = (tot_ce_oi - tot_ce_prev)
        pe_d = (tot_pe_oi - tot_pe_prev)

    max_pain = None
    if strike_sum_map:
        max_pain = max(strike_sum_map.items(), key=lambda kv: kv[1])[0]

    step = _strike_step(sym)
    center = float(spot) if isinstance(spot,(int,float)) and spot is not None else float(max_pain or 0.0)
    base = _round_down(center, step)
    s1 = base
    s2 = base - 2*step
    r1 = base + step
    r2 = base + 2*step

    mv = _mv_from(pcr, max_pain, spot, ce_d, pe_d)

    ts = _now()
    asof = time.strftime("%Y-%m-%d %H:%M:%S IST", time.gmtime(ts + 19800))

    snap = {
        "symbol": sym,
        "expiry": exp,
        "spot": spot,
        "s1": float(s1), "s2": float(s2), "r1": float(r1), "r2": float(r2),
        "pcr": pcr,
        "max_pain": max_pain,
        "ce_oi_delta": ce_d,
        "pe_oi_delta": pe_d,
        "mv": mv,
        "ts": ts,
        "asof": asof,
        "age_sec": 0,
        "source": "provider",
        "stale": False,
        "stale_reason": [],
    }
    return snap

# ---------------- Public API ----------------
async def refresh_once() -> Dict[str, Any]:
    global _last_fetch_ts, _last_snapshot, _cooldown_until

    now = _now()
    if now < _cooldown_until:
        if _last_snapshot:
            return {"status":"cooldown","reason":"429_cooldown","snapshot":_last_snapshot}
        raise RuntimeError(f"429 cooldown; wait {(_cooldown_until-now)}s")

    cadence = int(_env("OC_REFRESH_SECS","12") or "12")
    if _last_fetch_ts and _last_snapshot and now - _last_fetch_ts < max(3, cadence):
        return {"status":"cached","reason":"","snapshot":_last_snapshot}

    sym = _get_symbol()
    seg = _env("DHAN_UNDERLYING_SEG","IDX_I")
    sid = _get_security_id(sym)
    if not sid:
        raise RuntimeError("Missing DHAN_UNDERLYING_SCRIP (or map) for symbol "+sym)

    exp = _pick_expiry(sym, seg, sid)
    data = _fetch_chain(sym, seg, sid, exp)
    snap = _compute_from_chain(sym, data, exp)

    _last_fetch_ts = now
    _last_snapshot = snap
    return {"status":"ok","reason":"","snapshot":snap}
