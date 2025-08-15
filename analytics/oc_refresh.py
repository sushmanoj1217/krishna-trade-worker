# analytics/oc_refresh.py
# Dhan Option Chain plugin (v2 API)
# Returns a normalized snapshot for krishna_main.get_oc_snapshot(cfg)
# Output keys:
#   symbol, spot, s1, s2, r1, r2, expiry, ce_oi_pct, pe_oi_pct, volume_low
#
# Strategy:
# - POST /v2/optionchain/expirylist  (headers: access-token, client-id)
# - POST /v2/optionchain              (headers: access-token, client-id)
# - Parse v2 shape: data.last_price + data.oc{ "<strike>": {ce:{oi:...}, pe:{oi:...}} }
# - Compute S1/S2 (PE OI top-2) and R1/R2 (CE OI top-2)
# - Aggregate near-ATM CE/PE OI to compute intraday % change (module memory)
# - On any error: raise -> main will fallback to Sheets OC_Live (returns None here)

from __future__ import annotations
import os
import json
import requests
from typing import Any, Dict, List, Optional, Tuple

BASE_URL = "https://api.dhan.co"

# Module-level memory for near-ATM OI to compute % change across ticks
_prev_oi = {"ce": None, "pe": None}


# ---------------- HTTP helpers ----------------
def _headers() -> Dict[str, str]:
    """Dhan v2 expects access-token + client-id. Keep accept/json."""
    h = {
        "access-token": os.getenv("DHAN_ACCESS_TOKEN", ""),
        "client-id": os.getenv("DHAN_CLIENT_ID", ""),
        "content-type": "application/json",
        "accept": "application/json",
    }
    # Backward-compat (some setups still keep Bearer); harmless to include:
    auth = os.getenv("DHAN_ACCESS_TOKEN")
    if auth:
        h.setdefault("Authorization", f"Bearer {auth}")
    return h


def _post_json(path: str, payload: Dict[str, Any]) -> Any:
    url = BASE_URL + path
    r = requests.post(url, json=payload, headers=_headers(), timeout=12)
    if r.status_code != 200:
        # surface concise error; main handles fallback
        raise RuntimeError(f"HTTP {r.status_code} {url} {r.text[:200]}")
    try:
        return r.json()
    except Exception:
        raise RuntimeError("Non-JSON response from Dhan")


# ---------------- Parsing helpers ----------------
def _rows_from_v2_oc_map(oc_map: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Flatten v2 oc map into rows like:
      {"optionType":"CE"/"PE","strikePrice":float,"openInterest":float}
    """
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
            oi_ce = _to_float(ce.get("oi"), 0.0)
            rows.append({"optionType": "CE", "strikePrice": k, "openInterest": oi_ce})
        if isinstance(pe, dict):
            oi_pe = _to_float(pe.get("oi"), 0.0)
            rows.append({"optionType": "PE", "strikePrice": k, "openInterest": oi_pe})
    return rows


def _extract_fields(row: Dict[str, Any]) -> Tuple[str, float, float]:
    """Return (type 'CE'/'PE', strike, oi) from a row (robust to field names)."""
    t = str(row.get("optionType", "")).upper()
    if "CALL" in t:
        t = "CE"
    elif "PUT" in t:
        t = "PE"
    elif t not in ("CE", "PE"):
        t = "CE" if "C" in t else ("PE" if "P" in t else t)
    strike = _to_float(row.get("strikePrice") or row.get("strike"), 0.0)
    oi = _to_float(row.get("openInterest") or row.get("oi"), 0.0)
    return t, strike, oi


def _to_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _compute_levels(rows: List[Dict[str, Any]]) -> Tuple[float, float, float, float, Dict[float, float], Dict[float, float]]:
    """
    Compute:
      - s1/s2 = top-2 PE OI strikes  (supports)
      - r1/r2 = top-2 CE OI strikes  (resistances)
      also return per-strike OI maps for CE/PE
    """
    pe_oi: Dict[float, float] = {}
    ce_oi: Dict[float, float] = {}
    for row in rows:
        t, k, oi = _extract_fields(row)
        if not k or not oi:
            continue
        if t == "PE":
            pe_oi[k] = pe_oi.get(k, 0.0) + oi
        elif t == "CE":
            ce_oi[k] = ce_oi.get(k, 0.0) + oi

    def _top2(d: Dict[float, float]) -> Tuple[float, float]:
        if not d:
            return 0.0, 0.0
        top = sorted(d.items(), key=lambda kv: kv[1], reverse=True)
        a = top[0][0] if len(top) > 0 else 0.0
        b = top[1][0] if len(top) > 1 else 0.0
        return a, b

    s1, s2 = _top2(pe_oi)
    r1, r2 = _top2(ce_oi)
    return s1, s2, r1, r2, pe_oi, ce_oi


def _pct_change(curr: float, prev: Optional[float]) -> Optional[float]:
    if prev is None or prev == 0:
        return 0.0
    try:
        return (curr - prev) * 100.0 / prev
    except Exception:
        return None


def _agg_oi_near_atm(oi_by_strike: Dict[float, float], spot: float, band: int = 1) -> float:
    """
    Sum OI around the nearest strike to spot across ±band indices.
    """
    if spot <= 0 or not oi_by_strike:
        return 0.0
    strikes = sorted(oi_by_strike.keys())
    nearest = min(strikes, key=lambda k: abs(k - spot))
    try:
        idx = strikes.index(nearest)
    except ValueError:
        idx = 0
    total = 0.0
    for j in range(idx - band, idx + band + 1):
        if 0 <= j < len(strikes):
            total += oi_by_strike[strikes[j]]
    return total


# ---------------- Public: get_snapshot ----------------
def get_snapshot(cfg) -> Optional[Dict[str, Any]]:
    """
    Returns:
      {
        "symbol": str,
        "spot": float, "s1": float, "s2": float, "r1": float, "r2": float,
        "expiry": "YYYY-MM-DD",
        "ce_oi_pct": float|None,
        "pe_oi_pct": float|None,
        "volume_low": bool
      }
    On any error: prints and returns None (main will fallback to Sheets).
    """
    symbol = os.getenv("OC_SYMBOL_PRIMARY", getattr(cfg, "symbol", "NIFTY"))
    # DHAN_USID_MAP like: "NIFTY=13,BANKNIFTY=12"
    us_map_str = os.getenv("DHAN_USID_MAP", "NIFTY=13")
    us_map: Dict[str, str] = {}
    try:
        for item in us_map_str.split(","):
            if not item.strip():
                continue
            k, v = item.strip().split("=")
            us_map[k.strip()] = v.strip()
    except Exception:
        pass
    try:
        us_id = int(us_map.get(symbol, "13"))
    except Exception:
        us_id = 13

    seg = "IDX_I"  # index options segment

    try:
        # 1) Expiries
        exp_resp = _post_json("/v2/optionchain/expirylist", {
            "UnderlyingScrip": us_id,
            "UnderlyingSeg": seg,
        })
        expiries = exp_resp.get("data")
        if not isinstance(expiries, list) or not expiries:
            raise RuntimeError("bad expirylist shape")
        expiries = [str(x) for x in expiries]
        expiry = sorted(expiries)[0]

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
        oc_map = data.get("oc") or {}

        rows = _rows_from_v2_oc_map(oc_map)
        s1, s2, r1, r2, pe_by_k, ce_by_k = _compute_levels(rows)

        # 3) Near-ATM OI aggregates and % changes
        ce_now = _agg_oi_near_atm(ce_by_k, spot, band=1)
        pe_now = _agg_oi_near_atm(pe_by_k, spot, band=1)

        ce_pct = _pct_change(ce_now, _prev_oi["ce"])
        pe_pct = _pct_change(pe_now, _prev_oi["pe"])
        _prev_oi["ce"] = ce_now
        _prev_oi["pe"] = pe_now

        volume_low = bool(ce_now < 1e4 and pe_now < 1e4)

        return {
            "symbol": symbol,
            "spot": float(spot or 0.0),
            "s1": float(s1 or 0.0),
            "s2": float(s2 or 0.0),
            "r1": float(r1 or 0.0),
            "r2": float(r2 or 0.0),
            "expiry": str(expiry),
            "ce_oi_pct": None if ce_pct is None else float(round(ce_pct, 2)),
            "pe_oi_pct": None if pe_pct is None else float(round(pe_pct, 2)),
            "volume_low": volume_low,
        }

    except Exception as e:
        print(f"[oc_refresh] Dhan plugin failed: {e}", flush=True)
        return None
