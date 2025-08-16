# analytics/oc_refresh.py
# Dhan Option Chain plugin (v2 API) — rate-limit safe
# - POST /v2/optionchain/expirylist  (headers: access-token, client-id)
# - POST /v2/optionchain              (headers: access-token, client-id)
# - Caches nearest expiry (TTL) to cut calls
# - Handles HTTP 429 with exponential backoff (returns last-good snapshot during backoff)
# - Maintains last-good snapshot to avoid None when API is temporarily throttled
# - Telegram 429 alert (throttled) if ALERT_429=on

from __future__ import annotations
import os, time, requests
from typing import Any, Dict, List, Optional, Tuple

BASE_URL = "https://api.dhan.co"

# Module memory
_prev_oi = {"ce": None, "pe": None}
_expiry_cache = {"value": None, "ts": 0.0}
_last_ok = {"snapshot": None, "ts": 0.0}
_backoff = {"secs": 0, "until": 0.0}
_alert_429_until = 0.0

# ---- Tunables via env (seconds) ----
EXPIRY_TTL_SECS = int(os.getenv("DHAN_EXPIRY_TTL_SECS", "1800"))  # 30 min
BACKOFF_STEP = int(os.getenv("DHAN_BACKOFF_STEP_SECS", "60"))     # first backoff
BACKOFF_MAX  = int(os.getenv("DHAN_BACKOFF_MAX_SECS", "300"))     # cap at 5 min

def _headers() -> Dict[str, str]:
    return {
        "access-token": os.getenv("DHAN_ACCESS_TOKEN", ""),
        "client-id": os.getenv("DHAN_CLIENT_ID", ""),
        "content-type": "application/json",
        "accept": "application/json",
    }

def _tg_alert(text: str) -> None:
    if os.getenv("ALERT_429", "off").lower() != "on":
        return
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    uid = (os.getenv("TELEGRAM_USER_ID", "") or "").split(",")[0].strip()
    if not token or not uid:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      json={"chat_id": uid, "text": text}, timeout=8)
    except Exception:
        pass

def _post_json(path: str, payload: Dict[str, Any]) -> Any:
    url = BASE_URL + path
    r = requests.post(url, json=payload, headers=_headers(), timeout=12)
    if r.status_code == 429:
        raise RateLimitError(f"HTTP 429 {url} {r.text[:200]}")
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} {url} {r.text[:200]}")
    try:
        return r.json()
    except Exception:
        raise RuntimeError("Non-JSON response from Dhan")

class RateLimitError(Exception):
    pass

# -------------- parsing helpers --------------
def _to_float(v, default=None):
    try:
        if v is None: return default
        return float(v)
    except Exception:
        return default

def _rows_from_v2_oc_map(oc_map: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(oc_map, dict):
        return rows
    for k_str, legs in oc_map.items():
        try:
            k = float(k_str)
        except Exception:
            continue
        if not isinstance(legs, dict):
            continue
        ce = legs.get("ce") or {}
        pe = legs.get("pe") or {}
        if isinstance(ce, dict):
            rows.append({"optionType": "CE", "strikePrice": k, "openInterest": _to_float(ce.get("oi"), 0.0)})
        if isinstance(pe, dict):
            rows.append({"optionType": "PE", "strikePrice": k, "openInterest": _to_float(pe.get("oi"), 0.0)})
    return rows

def _extract_fields(row: Dict[str, Any]) -> Tuple[str, float, float]:
    t = str(row.get("optionType", "")).upper()
    if "CALL" in t: t = "CE"
    if "PUT"  in t: t = "PE"
    if t not in ("CE","PE"):
        t = "CE" if "C" in t else ("PE" if "P" in t else t)
    strike = _to_float(row.get("strikePrice") or row.get("strike"), 0.0)
    oi     = _to_float(row.get("openInterest") or row.get("oi"), 0.0)
    return t, strike, oi

def _compute_levels(rows: List[Dict[str, Any]]) -> Tuple[float, float, float, float, Dict[float, float], Dict[float, float]]:
    pe_oi: Dict[float, float] = {}
    ce_oi: Dict[float, float] = {}
    for row in rows:
        t, k, oi = _extract_fields(row)
        if not k or not oi: continue
        if t == "PE": pe_oi[k] = pe_oi.get(k, 0.0) + oi
        if t == "CE": ce_oi[k] = ce_oi.get(k, 0.0) + oi

    def _top2(d: Dict[float, float]) -> Tuple[float, float]:
        if not d: return 0.0, 0.0
        top = sorted(d.items(), key=lambda kv: kv[1], reverse=True)
        a = top[0][0] if len(top)>0 else 0.0
        b = top[1][0] if len(top)>1 else 0.0
        return a, b

    s1, s2 = _top2(pe_oi)
    r1, r2 = _top2(ce_oi)
    return s1, s2, r1, r2, pe_oi, ce_oi

def _agg_oi_near_atm(oi_by_k: Dict[float, float], spot: float, band: int=1) -> float:
    if spot <= 0 or not oi_by_k: return 0.0
    strikes = sorted(oi_by_k.keys())
    nearest = min(strikes, key=lambda k: abs(k - spot))
    try:
        idx = strikes.index(nearest)
    except ValueError:
        idx = 0
    total = 0.0
    for j in range(idx-band, idx+band+1):
        if 0 <= j < len(strikes):
            total += oi_by_k[strikes[j]]
    return total

# -------------- public: snapshot --------------
def get_snapshot(cfg) -> Optional[Dict[str, Any]]:
    """
    Return dict:
      symbol, spot, s1, s2, r1, r2, expiry, ce_oi_pct, pe_oi_pct, volume_low
    On 429 or errors: returns last-good snapshot (if any) and sets backoff window.
    """
    symbol = os.getenv("OC_SYMBOL_PRIMARY", getattr(cfg, "symbol", "NIFTY"))
    us_map = {}
    for item in (os.getenv("DHAN_USID_MAP","").split(",") if os.getenv("DHAN_USID_MAP") else []):
        item = item.strip()
        if not item: continue
        try:
            k, v = item.split("=", 1)
            us_map[k.strip()] = v.strip()
        except ValueError:
            pass
    try:
        us_id = int(us_map.get(symbol, "13"))
    except Exception:
        us_id = 13
    seg = "IDX_I"

    now = time.time()
    # Respect backoff window
    if now < _backoff["until"]:
        if _last_ok["snapshot"]:
            return _last_ok["snapshot"]
        # no last-ok; keep falling through to try once (rare)

    try:
        # 1) Expiry with TTL
        expiry = _expiry_cache["value"]
        if not expiry or (now - _expiry_cache["ts"] > EXPIRY_TTL_SECS):
            exp_resp = _post_json("/v2/optionchain/expirylist", {
                "UnderlyingScrip": us_id,
                "UnderlyingSeg": seg,
            })
            arr = exp_resp.get("data")
            if not isinstance(arr, list) or not arr:
                raise RuntimeError("bad expirylist shape")
            expiry = sorted([str(x) for x in arr])[0]
            _expiry_cache.update({"value": expiry, "ts": now})

        # 2) Chain
        chain_resp = _post_json("/v2/optionchain", {
            "UnderlyingScrip": us_id,
            "UnderlyingSeg": seg,
            "Expiry": expiry,
        })
        if not isinstance(chain_resp, dict) or "data" not in chain_resp:
            raise RuntimeError("unexpected chain shape")

        data = chain_resp["data"]
        spot = _to_float(data.get("last_price"), 0.0) or 0.0
        rows = _rows_from_v2_oc_map(data.get("oc") or {})
        s1, s2, r1, r2, pe_by_k, ce_by_k = _compute_levels(rows)

        ce_now = _agg_oi_near_atm(ce_by_k, spot, band=1)
        pe_now = _agg_oi_near_atm(pe_by_k, spot, band=1)

        def _pct(curr, prev):
            if prev is None or prev == 0: return 0.0
            try: return (curr - prev) * 100.0 / prev
            except Exception: return 0.0

        ce_pct = _pct(ce_now, _prev_oi["ce"])
        pe_pct = _pct(pe_now, _prev_oi["pe"])
        _prev_oi["ce"] = ce_now
        _prev_oi["pe"] = pe_now

        volume_low = bool(ce_now < 1e4 and pe_now < 1e4)

        snap = {
            "symbol": symbol,
            "spot": float(spot or 0.0),
            "s1": float(s1 or 0.0),
            "s2": float(s2 or 0.0),
            "r1": float(r1 or 0.0),
            "r2": float(r2 or 0.0),
            "expiry": str(expiry),
            "ce_oi_pct": float(round(ce_pct, 2)),
            "pe_oi_pct": float(round(pe_pct, 2)),
            "volume_low": volume_low,
        }
        _last_ok.update({"snapshot": snap, "ts": now})
        # success -> reset backoff
        _backoff.update({"secs": 0, "until": 0.0})
        return snap

    except RateLimitError as e:
        # Exponential backoff
        prev = _backoff["secs"] or BACKOFF_STEP
        nxt = min(max(prev * 2, BACKOFF_STEP), BACKOFF_MAX)
        until = now + nxt
        _backoff.update({"secs": nxt, "until": until})
        # Throttle TG alert to once per backoff window
        global _alert_429_until
        if now >= _alert_429_until:
            _tg_alert(f"⚠️ Dhan 429 — backing off {int(nxt)}s")
            _alert_429_until = until
        # Serve last-good if we have it
        if _last_ok["snapshot"]:
            return _last_ok["snapshot"]
        print(f"[oc_refresh] {e}", flush=True)
        return None

    except Exception as e:
        print(f"[oc_refresh] Dhan plugin failed: {e}", flush=True)
        # Serve last-good on generic failure too
        if _last_ok["snapshot"]:
            return _last_ok["snapshot"]
        return None
