# ops/near_alerts.py
# Sends Telegram alerts when spot comes NEAR S1/S2/R1/R2 using directional zones:
#   Supports: spot in [S - band, S]
#   Resistances: spot in [R, R + band]
# Includes: OC market view, PCR/VIX, and whether trade taken or why skipped.

from __future__ import annotations
import time
from typing import Dict, Any, List, Optional, Tuple

from ops.notify import send_telegram
from agents.signal_generator import buffer_points, adj_support, adj_resistance, classify_market_view, compute_bias_tag, read_pcr_vix

_STATE = {
    "last": {}  # key=(symbol, level_tag) -> ts
}

def _cooldown_secs() -> int:
    import os
    v = os.getenv("NEAR_ALERT_COOLDOWN_SECS", "").strip()
    if v.isdigit():
        return max(60, int(v))
    return 300  # default 5 min

def _on_cooldown(symbol: str, tag: str) -> bool:
    key = (symbol, tag)
    last = _STATE["last"].get(key, 0)
    return (time.time() - last) < _cooldown_secs()

def _mark_sent(symbol: str, tag: str):
    _STATE["last"][(symbol, tag)] = time.time()

def _fmt(x: Optional[float]) -> str:
    try:
        return f"{float(x):.2f}"
    except Exception:
        return str(x)

def _detect_near_levels(oc: Dict[str, Any]) -> List[Tuple[str, float, float]]:
    """
    Returns list of (level_tag, trigger_level, original_level) that are "near" NOW.
    Supports:  spot in [S - band, S]
    Resistances: spot in [R, R + band]
    """
    out: List[Tuple[str, float, float]] = []
    symbol = (oc.get("symbol") or "NIFTY").upper()
    spot   = oc.get("spot", None)
    if spot is None:
        return out
    band = buffer_points(symbol)
    s1,s2,r1,r2 = oc.get("s1"), oc.get("s2"), oc.get("r1"), oc.get("r2")

    try:
        spot = float(spot)
    except Exception:
        return out

    if s1 is not None:
        s1s = adj_support(s1, band)
        if s1s <= spot <= float(s1): out.append(("S1", float(s1s), float(s1)))
    if s2 is not None:
        s2s = adj_support(s2, band)
        if s2s <= spot <= float(s2): out.append(("S2", float(s2s), float(s2)))
    if r1 is not None:
        r1s = adj_resistance(r1, band)
        if float(r1) <= spot <= r1s: out.append(("R1", float(r1s), float(r1)))
    if r2 is not None:
        r2s = adj_resistance(r2, band)
        if float(r2) <= spot <= r2s: out.append(("R2", float(r2s), float(r2)))

    return out

def _join_reasons(reasons: List[str]) -> str:
    if not reasons: return "—"
    # keep it short
    s = ", ".join(reasons)
    return s[:180]

def check_and_alert(oc: Dict[str, Any], context: Dict[str, Any]) -> None:
    """
    context:
      trade_taken: bool
      trade_side: Optional[str]
      dedup_hit: bool
      reasons: List[str]  (why not traded)
    """
    symbol = (oc.get("symbol") or "NIFTY").upper()
    spot = oc.get("spot")
    mv, mv_tag = classify_market_view(oc)
    bias = compute_bias_tag()
    pcr, vix = read_pcr_vix()

    for tag, trig, orig in _detect_near_levels(oc):
        if _on_cooldown(symbol, tag):
            continue

        band = buffer_points(symbol)
        header = f"NEAR ⚠️ {symbol} spot={_fmt(spot)}  Level={tag}"
        if tag.startswith("S"):
            zone = f"[{_fmt(orig - band)} … { _fmt(orig) }]"
        else:
            zone = f"[{ _fmt(orig) } … { _fmt(orig + band) }]"

        mvline = f"View: {mv_tag}; {bias}"
        pcrline = f"PCR={pcr if pcr is not None else 'n/a'}  VIX={vix if vix is not None else 'n/a'}"
        tline = ""

        trade_taken = bool(context.get("trade_taken", False))
        trade_side  = context.get("trade_side", "")
        reasons     = context.get("reasons", [])
        dedup_hit   = bool(context.get("dedup_hit", False))

        if trade_taken and context.get("trade_tag") == tag:
            tline = f"Action: TAKEN ✅ {trade_side}"
        else:
            why = list(reasons)
            if dedup_hit and context.get("trade_tag") == tag:
                why = ["duplicate level today"]
            tline = f"Action: NOT TAKEN ❌ ({_join_reasons(why)})"

        msg = (
            f"{header}\n"
            f"Zone: {zone}  (trigger={_fmt(trig)})\n"
            f"{mvline}\n"
            f"{pcrline}\n"
            f"{tline}"
        )
        if send_telegram(msg):
            _mark_sent(symbol, tag)
