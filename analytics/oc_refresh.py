# analytics/oc_refresh.py
# Dhan OC plugin that returns a normalized snapshot for krishna_main.get_oc_snapshot(cfg)
# Output dict keys expected by main:
#   symbol, spot, s1, s2, r1, r2, expiry, ce_oi_pct, pe_oi_pct, volume_low
#
# It tries Dhan API first; on any error it falls back to reading OC_Live via Sheets wrapper
# that main already handles (so returning None here will let main use its sheet fallback).

import os, time, json, math
from typing import Any, Dict, List, Optional, Tuple
import requests

# ---- Dhan API endpoints (adjust if your account uses different paths) ----
BASE_URL = "https://api.dhan.co"
EXPIRY_LIST_PATH = "/v2/optionchain/expirylist"   # ?symbol=NIFTY
CHAIN_PATH       = "/v2/optionchain/chain"        # ?symbol=NIFTY&expiry=YYYY-MM-DD

# Module-level memory for OI deltas (so we can compute % change intraday)
_prev_oi = {"ce": None, "pe": None}

def _headers() -> Dict[str, str]:
    token = os.getenv("DHAN_ACCESS_TOKEN", "")
    cid   = os.getenv("DHAN_CLIENT_ID", "")
    return {
        "Authorization": f"Bearer {token}",
        "x-client-id": cid,
        "accept": "application/json",
        "content-type": "application/json",
    }

def _get_json(url: str, params: Dict[str, Any]) -> Any:
    r = requests.get(url, params=params, headers=_headers(), timeout=10)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} {url} {r.text[:200]}")
    return r.json()

def _pick_nearest_expiry(expiries: List[str]) -> str:
    # expiries like ["2025-08-21", "2025-08-28", ...] — pick the earliest future
    if not expiries:
        raise RuntimeError("no expiries")
    return sorted(expiries)[0]

def _extract_fields(row: Dict[str, Any]) -> Tuple[str, float, float]:
    """
    Try to read a single leg row in multiple possible shapes:
      returns (opt_type 'CE'/'PE', strike, oi)
    """
    # option type
    t = row.get("optionType") or row.get("type") or row.get("side") or row.get("optType") or ""
    t = str(t).upper()
    if t not in ("CE", "CALL") and t not in ("PE", "PUT"):
        # handle flags like 'CALL'/'PUT'
        if "CALL" in t:
            t = "CE"
        elif "PUT" in t:
            t = "PE"
    t = "CE" if ("CE" in t or "CALL" in t) else ("PE" if ("PE" in t or "PUT" in t) else "")

    # strike
    strike = row.get("strikePrice") or row.get("strike") or row.get("sp") or 0
    try:
        strike = float(strike)
    except Exception:
        strike = 0.0

    # open interest
    oi = row.get("openInterest") or row.get("oi") or row.get("oiQty") or 0
    try:
        oi = float(oi)
    except Exception:
        oi = 0.0

    return t, strike, oi

def _compute_levels(chain_rows: List[Dict[str, Any]]) -> Tuple[float, float, float, float, Dict[float, float], Dict[float, float]]:
    """Return (s1, s2, r1, r2, pe_oi_by_strike, ce_oi_by_strike) from a flat list of option rows."""
    pe_oi: Dict[float, float] = {}
    ce_oi: Dict[float, float] = {}
    for row in chain_rows:
        t, k, oi = _extract_fields(row)
        if k <= 0: 
            continue
        if t == "PE":
            pe_oi[k] = pe_oi.get(k, 0.0) + oi
        elif t == "CE":
            ce_oi[k] = ce_oi.get(k, 0.0) + oi

    def _top2(d: Dict[float, float]) -> Tuple[float, float]:
        if not d:
            return 0.0, 0.0
        # sort by OI desc
        sorted_k = sorted(d.items(), key=lambda kv: kv[1], reverse=True)
        first = sorted_k[0][0] if len(sorted_k) > 0 else 0.0
        second = sorted_k[1][0] if len(sorted_k) > 1 else 0.0
        return first, second

    s1, s2 = _top2(pe_oi)  # PE side highs -> supports
    r1, r2 = _top2(ce_oi)  # CE side highs -> resistances
    return s1, s2, r1, r2, pe_oi, ce_oi

def _infer_spot(chain_rows: List[Dict[str, Any]]) -> float:
    # Try underlying fields if present
    for row in chain_rows[:10]:
        for key in ("underlying", "underlyingValue", "underlyingPrice", "underlying_price", "ltpUnderlying"):
            if key in row:
                try:
                    return float(row[key])
                except Exception:
                    pass
    # Else approximate by weighted mid of CE/PE ATM band if strikes exist
    strikes = []
    for row in chain_rows:
        _, k, oi = _extract_fields(row)
        if k > 0 and oi > 0:
            strikes.append(k)
    if not strikes:
        return 0.0
    # naive mid
    return float(sorted(strikes)[len(strikes)//2])

def _agg_oi_near_atm(oi_by_strike: Dict[float, float], spot: float, band: int = 1) -> float:
    # Sum OI of nearest +/- band strikes (band=1 means ATM ±1)
    if spot <= 0 or not oi_by_strike:
        return 0.0
    nearest = min(oi_by_strike.keys(), key=lambda k: abs(k - spot))
    strikes = sorted(oi_by_strike.keys())
    # get neighbors
    try:
        idx = strikes.index(nearest)
    except ValueError:
        # fallback to min diff
        diffs = [(abs(k - spot), i) for i, k in enumerate(strikes)]
        idx = min(diffs)[1]
    total = 0.0
    for j in range(idx - band, idx + band + 1):
        if 0 <= j < len(strikes):
            total += oi_by_strike[strikes[j]]
    return total

def _pct_change(curr: float, prev: Optional[float]) -> Optional[float]:
    if prev is None or prev == 0:
        return 0.0
    try:
        return (curr - prev) * 100.0 / prev
    except Exception:
        return None

def _expiries(symbol: str) -> List[str]:
    data = _get_json(BASE_URL + EXPIRY_LIST_PATH, {"symbol": symbol})
    # try common shapes
    if isinstance(data, dict):
        arr = data.get("data") or data.get("expiries") or data.get("expiryList") or data.get("result")
        if isinstance(arr, list):
            # ensure strings
            return [str(x) for x in arr]
    if isinstance(data, list):
        return [str(x) for x in data]
    raise RuntimeError(f"unexpected expiry list shape: {type(data)}")

def _chain(symbol: str, expiry: str) -> List[Dict[str, Any]]:
    data = _get_json(BASE_URL + CHAIN_PATH, {"symbol": symbol, "expiry": expiry})
    # normalize to flat list of rows
    if isinstance(data, dict):
        rows = data.get("data") or data.get("result") or data.get("records") or []
        if isinstance(rows, dict):
            # sometimes {CE: [...], PE: [...]}
            flat: List[Dict[str, Any]] = []
            for k, v in rows.items():
                if isinstance(v, list):
                    for row in v:
                        if isinstance(row, dict):
                            row.setdefault("optionType", "CE" if k.upper().startswith("C") else "PE")
                            flat.append(row)
            return flat
        if isinstance(rows, list):
            return [r for r in rows if isinstance(r, dict)]
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    raise RuntimeError("unexpected chain shape")

def get_snapshot(cfg) -> Optional[Dict[str, Any]]:
    """
    Return dict:
      symbol, spot, s1, s2, r1, r2, expiry, ce_oi_pct, pe_oi_pct, volume_low
    On error, return None -> main will fallback to Sheets OC_Live.
    """
    symbol = os.getenv("OC_SYMBOL_PRIMARY", getattr(cfg, "symbol", "NIFTY"))
    try:
        expiries = _expiries(symbol)
        expiry = _pick_nearest_expiry(expiries)
        rows = _chain(symbol, expiry)

        s1, s2, r1, r2, pe_oi_by_k, ce_oi_by_k = _compute_levels(rows)
        spot = _infer_spot(rows)

        # Aggregate OI near ATM ±1 strikes
        ce_oi_now = _agg_oi_near_atm(ce_oi_by_k, spot, band=1)
        pe_oi_now = _agg_oi_near_atm(pe_oi_by_k, spot, band=1)

        ce_pct = _pct_change(ce_oi_now, _prev_oi["ce"])
        pe_pct = _pct_change(pe_oi_now, _prev_oi["pe"])
        _prev_oi["ce"] = ce_oi_now
        _prev_oi["pe"] = pe_oi_now

        # volume_low heuristic (optional): if both OI tiny near ATM
        volume_low = bool(ce_oi_now < 1e4 and pe_oi_now < 1e4)

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
