# agents/signal_generator.py
# Generates signals from OC snapshot using your rules:
# - Directional buffer: supports S* = S - band (spot <= S*), resistances R* = R + band (spot >= R*)
# - 6-condition classifier (Bullish/Strong Bullish/Bearish/Strong Bearish/Sideways/Big Move)
# - PCR/VIX are confirmation-only (bias tag); they DO NOT block entries
# - Returns meta target (RR=2; trail after RR=2) for future executor hooks

from __future__ import annotations
import os
from typing import Dict, Any, Optional, Tuple

# ---------- Buffer config ----------
def _get_float(name: str, default: Optional[float]=None) -> Optional[float]:
    v = os.getenv(name, "")
    try:
        return float(v) if v not in ("", None) else default
    except Exception:
        return default

def buffer_points(symbol: str) -> int:
    """Per-symbol default buffer; global override via ENTRY_BAND_POINTS."""
    v = os.getenv("ENTRY_BAND_POINTS", "").strip()
    if v.isdigit():
        return int(v)
    s = (symbol or "").upper()
    if s == "NIFTY": return 12
    if s == "BANKNIFTY": return 30
    if s == "FINNIFTY": return 15
    return 12

def adj_support(level: float, band: float) -> float:
    return float(level) - float(band)

def adj_resistance(level: float, band: float) -> float:
    return float(level) + float(band)

# ---------- Market view (6 conditions) ----------
def classify_market_view(oc: Dict[str, Any]) -> Tuple[str, str]:
    """
    Returns (market_view, reason_tag)
    Views: bullish, strong_bullish, bearish, strong_bearish, sideways, big_move, unknown
    Based on ce_oi_pct (+up/-down), pe_oi_pct (+up/-down), volume_low flag (optional)
    """
    ce = oc.get("ce_oi_pct", None)
    pe = oc.get("pe_oi_pct", None)
    vol_low = bool(oc.get("volume_low")) if oc.get("volume_low") is not None else False

    if ce is None or pe is None:
        return ("unknown", "mv:unknown")

    up_ce = ce > 0
    up_pe = pe > 0
    down_ce = ce < 0
    down_pe = pe < 0

    if down_ce and up_pe:
        return ("bullish", "mv:bullish (CE↓ PE↑)")
    if down_ce and down_pe:
        return ("big_move", "mv:big_move (CE↓ PE↓)")
    if up_ce and up_pe:
        if vol_low:
            return ("sideways", "mv:sideways (CE↑ PE↑ + low vol)")
        return ("bearish", "mv:bearish (CE↑ PE↑)")
    if up_ce and down_pe:
        return ("strong_bearish", "mv:strong_bearish (CE↑ PE↓)")

    return ("unknown", "mv:unknown")

# ---------- PCR/VIX bias (confirmation-only tag) ----------
def compute_bias_tag() -> str:
    """
    Highest precedence: MARKET_BIAS env (bullish/bearish/neutral).
    Else PCR_VALUE (>1 bullish, <1 bearish, ~=1 neutral).
    VIX_VALUE (optional) is informational.
    """
    mb = (os.getenv("MARKET_BIAS", "").strip().lower())
    if mb in ("bullish", "bearish", "neutral"):
        return f"bias:{mb}"
    pcr = _get_float("PCR_VALUE", None)
    if pcr is None:
        return "bias:neutral"
    if pcr > 1.0: return "bias:bullish"
    if pcr < 1.0: return "bias:bearish"
    return "bias:neutral"

def read_pcr_vix() -> Tuple[Optional[float], Optional[float]]:
    return (_get_float("PCR_VALUE", None), _get_float("VIX_VALUE", None))

# ---------- Main ----------
def datetime_key() -> str:
    import datetime as _dt
    return _dt.datetime.now().strftime("%Y%m%d")

def _fmt(x: Optional[float]) -> str:
    try:
        return f"{float(x):.2f}"
    except Exception:
        return str(x)

def generate_signal_from_oc(oc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Input OC snapshot fields:
      symbol, spot, s1,s2,r1,r2, ce_oi_pct, pe_oi_pct, volume_low
    Output on match:
      {
        'symbol','side'(BUY_CE/BUY_PE),
        'level'(float original S/R), 'level_tag'(S1/S2/R1/R2),
        'trigger_level'(float adjusted S* or R*),
        'reason', 'dedup_key',
        'target_rr': 2.0, 'trail_after_rr': 2.0
      }
    """
    symbol = (oc.get("symbol") or "NIFTY").upper()
    spot   = oc.get("spot", None)
    s1, s2 = oc.get("s1", None), oc.get("s2", None)
    r1, r2 = oc.get("r1", None), oc.get("r2", None)

    if spot is None or (s1 is None and r1 is None):
        return None

    band = buffer_points(symbol)
    mv, mv_tag = classify_market_view(oc)
    bias_tag = compute_bias_tag()

    # ---- Directional buffer thresholds ----
    # CE allowed @ supports when mv supportive (bullish or big_move)
    if s1 is not None:
        s1_star = adj_support(s1, band)  # S1* = S1 - band
        if float(spot) <= s1_star and mv in ("bullish", "big_move"):
            reason = f"{mv_tag}; {bias_tag}; S1*={_fmt(s1_star)} (S1={_fmt(s1)} - {band})"
            dkey = f"{symbol}:S1:BUY_CE:{datetime_key()}"
            return {
                "symbol": symbol,
                "side": "BUY_CE",
                "level": float(s1),
                "level_tag": "S1",
                "trigger_level": float(s1_star),
                "reason": reason,
                "dedup_key": dkey,
                "target_rr": 2.0,
                "trail_after_rr": 2.0,
            }
    if s2 is not None:
        s2_star = adj_support(s2, band)  # S2* = S2 - band
        if float(spot) <= s2_star and mv in ("bullish", "big_move"):
            reason = f"{mv_tag}; {bias_tag}; S2*={_fmt(s2_star)} (S2={_fmt(s2)} - {band})"
            dkey = f"{symbol}:S2:BUY_CE:{datetime_key()}"
            return {
                "symbol": symbol,
                "side": "BUY_CE",
                "level": float(s2),
                "level_tag": "S2",
                "trigger_level": float(s2_star),
                "reason": reason,
                "dedup_key": dkey,
                "target_rr": 2.0,
                "trail_after_rr": 2.0,
            }

    # PE allowed @ resistances when mv supportive (bearish / strong_bearish)
    if r1 is not None:
        r1_star = adj_resistance(r1, band)  # R1* = R1 + band
        if float(spot) >= r1_star and mv in ("bearish", "strong_bearish"):
            reason = f"{mv_tag}; {bias_tag}; R1*={_fmt(r1_star)} (R1={_fmt(r1)} + {band})"
            dkey = f"{symbol}:R1:BUY_PE:{datetime_key()}"
            return {
                "symbol": symbol,
                "side": "BUY_PE",
                "level": float(r1),
                "level_tag": "R1",
                "trigger_level": float(r1_star),
                "reason": reason,
                "dedup_key": dkey,
                "target_rr": 2.0,
                "trail_after_rr": 2.0,
            }
    if r2 is not None:
        r2_star = adj_resistance(r2, band)  # R2* = R2 + band
        if float(spot) >= r2_star and mv in ("bearish", "strong_bearish"):
            reason = f"{mv_tag}; {bias_tag}; R2*={_fmt(r2_star)} (R2={_fmt(r2)} + {band})"
            dkey = f"{symbol}:R2:BUY_PE:{datetime_key()}"
            return {
                "symbol": symbol,
                "side": "BUY_PE",
                "level": float(r2),
                "level_tag": "R2",
                "trigger_level": float(r2_star),
                "reason": reason,
                "dedup_key": dkey,
                "target_rr": 2.0,
                "trail_after_rr": 2.0,
            }

    # no entry
    return None
