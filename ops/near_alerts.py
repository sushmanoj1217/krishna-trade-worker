# ops/near_alerts.py
# Sends Telegram alerts when spot is NEAR or CROSSING S/R zones (directional buffer):
# Supports zone: [S - band, S] ; Cross if spot <= (S - band)
# Resistances zone: [R, R + band] ; Cross if spot >= (R + band)
# Includes: market view tag, PCR/VIX, and whether trade taken or skipped + why.

from __future__ import annotations
import os, time
from typing import Dict, Any, List, Optional, Tuple

from ops.notify import send_telegram
from agents.signal_generator import (
    buffer_points, adj_support, adj_resistance,
    classify_market_view, compute_bias_tag, read_pcr_vix
)

_STATE = {"last": {}}  # cooldown: key=(symbol, tag, kind) -> ts

def _cooldown_secs() -> int:
    v = os.getenv("NEAR_ALERT_COOLDOWN_SECS", "").strip()
    if v.isdigit():
        return max(30, int(v))
    return 300  # default 5 min

def _debug_on() -> bool:
    return os.getenv("NEAR_ALERT_DEBUG", "").strip().lower() in ("1","true","on","yes")

def _on_cooldown(symbol: str, tag: str, kind: str) -> bool:
    key = (symbol, tag, kind)
    last = _STATE["last"].get(key, 0)
    return (time.time() - last) < _cooldown_secs()

def _mark_sent(symbol: str, tag: str, kind: str):
    _STATE["last"][(symbol, tag, kind)] = time.time()

def _fmt(x: Optional[float]) -> str:
    try: return f"{float(x):.2f}"
    except Exception: return str(x)

def _zones(oc: Dict[str,Any]) -> List[Tuple[str,float,float,float,float]]:
    """
    Returns per-level tuple:
      (tag, zone_lo, zone_hi, cross_thr, base_level)
      - Supports: zone_lo = S - band, zone_hi = S, cross_thr = S - band
      - Resistances: zone_lo = R, zone_hi = R + band, cross_thr = R + band
    """
    out: List[Tuple[str,float,float,float,float]] = []
    symbol = (oc.get("symbol") or "NIFTY").upper()
    band = buffer_points(symbol)
    s1,s2,r1,r2 = oc.get("s1"), oc.get("s2"), oc.get("r1"), oc.get("r2")
    def f(x): return None if x is None else float(x)

    if s1 is not None:
        S = f(s1); Sb = adj_support(S, band)
        out.append(("S1", float(Sb), float(S), float(Sb), float(S)))
    if s2 is not None:
        S = f(s2); Sb = adj_support(S, band)
        out.append(("S2", float(Sb), float(S), float(Sb), float(S)))
    if r1 is not None:
        R = f(r1); Rb = adj_resistance(R, band)
        out.append(("R1", float(R), float(Rb), float(Rb), float(R)))
    if r2 is not None:
        R = f(r2); Rb = adj_resistance(R, band)
        out.append(("R2", float(R), float(Rb), float(Rb), float(R)))
    return out

def _why_str(reasons: List[str], dedup_hit: bool, for_tag: Optional[str]) -> str:
    rs = list(reasons)
    if dedup_hit and for_tag:
        rs = ["duplicate level today"]
    if not rs: return "—"
    s = ", ".join(rs)
    return s[:180]

def _send(kind: str, symbol: str, spot, tag: str, zone_lo, zone_hi, cross_thr,
          mv_tag: str, bias_tag: str, pcr: Optional[float], vix: Optional[float],
          context: Dict[str,Any]) -> None:
    if _on_cooldown(symbol, tag, kind):
        return
    header = f"{kind} ⚠️ {symbol} spot={_fmt(spot)}  Level={tag}"
    zone = f"[{_fmt(zone_lo)} … {_fmt(zone_hi)}] (trigger={_fmt(cross_thr)})"
    pcrline = f"PCR={_fmt(pcr) if pcr is not None else 'n/a'}  VIX={_fmt(vix) if vix is not None else 'n/a'}"
    taken = bool(context.get("trade_taken", False))
    trade_tag = context.get("trade_tag")
    trade_side = context.get("trade_side","")
    dedup_hit = bool(context.get("dedup_hit", False))
    reasons = context.get("reasons", [])

    if taken and trade_tag == tag:
        action = f"Action: TAKEN ✅ {trade_side}"
    else:
        action = f"Action: NOT TAKEN ❌ ({_why_str(reasons, dedup_hit, tag)})"

    msg = (
        f"{header}\n"
        f"Zone: {zone}\n"
        f"View: {mv_tag}; {bias_tag}\n"
        f"{pcrline}\n"
        f"{action}"
    )
    if send_telegram(msg):
        _mark_sent(symbol, tag, kind)

def check_and_alert(oc: Dict[str, Any], context: Dict[str, Any]) -> None:
    symbol = (oc.get("symbol") or "NIFTY").upper()
    spot = oc.get("spot")
    if spot is None: return
    try: spot = float(spot)
    except Exception: return

    mv, mv_tag = classify_market_view(oc)
    bias = compute_bias_tag()
    pcr, vix = read_pcr_vix()

    for tag, zlo, zhi, cross_thr, base in _zones(oc):
        is_near = (zlo <= spot <= zhi)
        is_cross = (spot <= cross_thr) if tag.startswith("S") else (spot >= cross_thr)

        if _debug_on():
            print(f"[near_debug] {symbol} {tag}: spot={_fmt(spot)} zone=[{_fmt(zlo)},{_fmt(zhi)}] cross_thr={_fmt(cross_thr)} near={is_near} cross={is_cross}", flush=True)

        # NEAR alert
        if is_near:
            _send("NEAR", symbol, spot, tag, zlo, zhi, cross_thr, mv_tag, bias, pcr, vix, context)

        # CROSS alert (separate cooldown key)
        if is_cross:
            _send("CROSS", symbol, spot, tag, zlo, zhi, cross_thr, mv_tag, bias, pcr, vix, context)
