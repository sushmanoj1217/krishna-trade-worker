# agents/eligibility_api.py
# ------------------------------------------------------------
# Eligibility wrapper API for /oc_now.
# - Prefers agents.signal_generator.check_eligibility(snapshot) if present.
# - Else fallback C1..C6 with human-readable reasons.
# - C2 now implements "MV 1-of-2 (PCR / MaxPainΔ)" fallback when MV tag missing.
# ------------------------------------------------------------
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, time
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional, Tuple
import math

IST = ZoneInfo("Asia/Kolkata")

def _g(snapshot: Any, key: str, default=None):
    if snapshot is None:
        return default
    if isinstance(snapshot, dict):
        return snapshot.get(key, default)
    if hasattr(snapshot, key):
        return getattr(snapshot, key)
    if key == "symbol":
        return _g(snapshot, "sym", default)
    if key == "expiry":
        return _g(snapshot, "exp", default)
    return default

def _to_float(x, default=None):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return default
        return float(x)
    except Exception:
        return default

def _now_ist() -> datetime:
    return datetime.now(IST)

def _in_window(dt: datetime, start: time, end: time) -> bool:
    t = dt.timetz()
    return (t >= start.replace(tzinfo=IST)) and (t <= end.replace(tzinfo=IST))

DEFAULT_ENTRY_BAND = {"NIFTY": 12.0, "BANKNIFTY": 30.0, "FINNIFTY": 15.0}
NO_TRADE_WINDOWS = [
    (time(9, 15, tzinfo=IST), time(9, 30, tzinfo=IST)),
    (time(14, 45, tzinfo=IST), time(15, 15, tzinfo=IST)),
]
MV_OK_CE = {"bullish", "big_move", "strong_bullish"}
MV_OK_PE = {"bearish", "strong_bearish", "big_down"}

@dataclass
class TriggerInfo:
    side: Optional[str] = None  # "CE"/"PE"
    level_tag: Optional[str] = None  # "S1*"/"S2*"/"R1*"/"R2*"
    price: Optional[float] = None
    reason: str = ""

def _resolve_buffer(symbol: Optional[str], params: Optional[dict]) -> float:
    if params and isinstance(params, dict):
        b = params.get("ENTRY_BAND_POINTS")
        try:
            if isinstance(b, (int, float)):
                return float(b)
        except Exception:
            pass
    sym = (symbol or "").upper()
    return DEFAULT_ENTRY_BAND.get(sym, 12.0)

def _compute_shifted_levels(s1, s2, r1, r2, buf: float) -> Dict[str, Optional[float]]:
    return {
        "S1*": _to_float(None if s1 is None else (s1 - buf)),
        "S2*": _to_float(None if s2 is None else (s2 - buf)),
        "R1*": _to_float(None if r1 is None else (r1 + buf)),
        "R2*": _to_float(None if r2 is None else (r2 + buf)),
    }

def _pick_trigger(spot: Optional[float], shifted: Dict[str, Optional[float]]) -> TriggerInfo:
    if spot is None:
        return TriggerInfo(reason="spot missing")
    ce, pe = [], []
    for tag in ("S1*", "S2*"):
        lv = shifted.get(tag)
        if lv is not None and spot <= lv:
            ce.append((tag, lv, abs(spot - lv)))
    for tag in ("R1*", "R2*"):
        lv = shifted.get(tag)
        if lv is not None and spot >= lv:
            pe.append((tag, lv, abs(spot - lv)))
    if ce and not pe:
        tag, lv, _ = sorted(ce, key=lambda t: t[2])[0]
        return TriggerInfo("CE", tag, lv, "CROSS CE")
    if pe and not ce:
        tag, lv, _ = sorted(pe, key=lambda t: t[2])[0]
        return TriggerInfo("PE", tag, lv, "CROSS PE")
    if ce and pe:
        both = sorted(ce + pe, key=lambda t: t[2])
        tag, lv, _ = both[0]
        side = "CE" if tag.startswith("S") else "PE"
        return TriggerInfo(side, tag, lv, "CROSS both; picked nearest")
    cand = []
    for tag, lv in shifted.items():
        if lv is not None:
            cand.append((tag, lv, abs(spot - lv)))
    if cand:
        tag, lv, _ = sorted(cand, key=lambda t: t[2])[0]
        side = "CE" if tag.startswith("S") else "PE"
        return TriggerInfo(side, tag, lv, "NEAR, not crossed")
    return TriggerInfo(reason="levels missing")

def _mv_gate_ok(side: Optional[str], mv: Optional[str], pcr: Optional[float], max_pain: Optional[float], spot: Optional[float]) -> Tuple[bool, str]:
    m = (mv or "").lower().strip()
    # 1) If explicit MV tag present, use it
    if side == "CE" and m:
        return (m in MV_OK_CE), f"MV={m}"
    if side == "PE" and m:
        return (m in MV_OK_PE), f"MV={m}"
    # 2) Fallback: 1-of-2 (PCR or MaxPain) support
    ok_pcr = ok_mp = None
    if isinstance(pcr, (int, float)):
        ok_pcr = (pcr >= 1.0) if side == "CE" else (pcr <= 1.0)
    if isinstance(max_pain, (int, float)) and isinstance(spot, (int, float)):
        ok_mp = (max_pain >= spot) if side == "CE" else (max_pain <= spot)
    oks = [x for x in (ok_pcr, ok_mp) if x is not None]
    if not oks:
        return False, "MV missing; PCR/MP missing"
    # Any-one true → pass
    parts = []
    parts.append(f"PCR {'✓' if ok_pcr else '×'}" if ok_pcr is not None else "PCR —")
    parts.append(f"MP {'✓' if ok_mp else '×'}" if ok_mp is not None else "MP —")
    return any(oks), " / ".join(parts)

def _oc_pattern_ok(side: Optional[str], ce_oi_delta: Optional[float], pe_oi_delta: Optional[float]) -> Tuple[bool, str]:
    if ce_oi_delta is None or pe_oi_delta is None:
        return False, "OIΔ missing"
    ce_down, ce_up = ce_oi_delta < 0, ce_oi_delta > 0
    pe_down, pe_up = pe_oi_delta < 0, pe_oi_delta > 0
    if side == "CE":
        ok = (ce_down and pe_up) or (ce_down and pe_down)
    elif side == "PE":
        ok = (ce_up and pe_down) or (ce_down and pe_down)
    else:
        ok = False
    pat = f"CEΔ={'+' if ce_up else ('-' if ce_down else '0')}{abs(ce_oi_delta):.0f} / PEΔ={'+' if pe_up else ('-' if pe_down else '0')}{abs(pe_oi_delta):.0f}"
    return ok, pat

def _confirmations_ok(side: Optional[str], pcr: Optional[float], max_pain: Optional[float], spot: Optional[float]) -> Tuple[bool, str]:
    notes = []
    ok_pcr = ok_mp = None
    if isinstance(pcr, (int, float)):
        ok_pcr = (pcr >= 1.0) if side == "CE" else (pcr <= 1.0)
        notes.append(f"PCR={pcr:.2f} {'✓' if ok_pcr else '×'}")
    if isinstance(max_pain, (int, float)) and isinstance(spot, (int, float)):
        ok_mp = (max_pain >= spot) if side == "CE" else (max_pain <= spot)
        notes.append(f"MP={max_pain:.0f} vs spot {spot:.0f} {'✓' if ok_mp else '×'}")
    oks = [x for x in (ok_pcr, ok_mp) if x is not None]
    if not oks:
        return False, "PCR/MP missing"
    return any(oks), " | ".join(notes)

def _system_gates(now: datetime, hold: bool, daily_cap_hit: bool) -> Tuple[bool, str]:
    for s, e in NO_TRADE_WINDOWS:
        if _in_window(now, s, e):
            return True, f"NoTradeWindow({s.strftime('%H:%M')}-{e.strftime('%H:%M')})"
    if hold:
        return True, "HOLD"
    if daily_cap_hit:
        return True, "daily-cap"
    return False, ""

def _dedupe_guard(info: TriggerInfo, already_attempted: bool) -> Tuple[bool, str]:
    if not info.side or not info.level_tag:
        return False, "no key"
    if already_attempted:
        key = f"{_now_ist():%Y%m%d}|{info.side}|{info.level_tag}|{info.price or 'NA'}"
        return True, f"blocked({key})"
    return False, "new"

def _fallback_check(snapshot: Any) -> Dict[str, Any]:
    symbol = str(_g(snapshot, "symbol", "") or "").upper()
    expiry = str(_g(snapshot, "expiry", "") or "")
    spot = _to_float(_g(snapshot, "spot"))
    s1 = _to_float(_g(snapshot, "s1"))
    s2 = _to_float(_g(snapshot, "s2"))
    r1 = _to_float(_g(snapshot, "r1"))
    r2 = _to_float(_g(snapshot, "r2"))
    mv = str(_g(snapshot, "mv", "") or "").lower()
    pcr = _to_float(_g(snapshot, "pcr"))
    max_pain = _to_float(_g(snapshot, "max_pain"))
    ce_oi_delta = _to_float(_g(snapshot, "ce_oi_delta", _g(snapshot, "ce_oi_change")))
    pe_oi_delta = _to_float(_g(snapshot, "pe_oi_delta", _g(snapshot, "pe_oi_change")))
    params = _g(snapshot, "params", {}) or {}
    hold = bool(_g(snapshot, "hold", False))
    daily_cap_hit = bool(_g(snapshot, "daily_cap_hit", False))
    already_attempted = bool(_g(snapshot, "already_attempted", False))

    buf = _resolve_buffer(symbol, params if isinstance(params, dict) else {})
    shifted = _compute_shifted_levels(s1, s2, r1, r2, buf)
    trig = _pick_trigger(spot, shifted)

    checks = []
    c1_ok = trig.side in ("CE", "PE") and trig.level_tag is not None and trig.price is not None
    checks.append({"id": "C1", "ok": c1_ok, "reason": trig.reason or "—"})

    c2_ok, c2_reason = _mv_gate_ok(trig.side, mv, pcr, max_pain, spot)
    checks.append({"id": "C2", "ok": c2_ok, "reason": c2_reason})

    c3_ok, c3_reason = _oc_pattern_ok(trig.side, ce_oi_delta, pe_oi_delta)
    checks.append({"id": "C3", "ok": c3_ok, "reason": c3_reason})

    c4_ok, c4_reason = _confirmations_ok(trig.side, pcr, max_pain, spot)
    checks.append({"id": "C4", "ok": c4_ok, "reason": c4_reason})

    blocked, why = _system_gates(_now_ist(), hold, daily_cap_hit)
    checks.append({"id": "C5", "ok": not blocked, "reason": (why or "—")})

    blocked6, why6 = _dedupe_guard(trig, already_attempted)
    checks.append({"id": "C6", "ok": not blocked6, "reason": (why6 or "—")})

    required = {"C1", "C2", "C3", "C5", "C6"}
    okmap = {c["id"]: bool(c["ok"]) for c in checks}
    eligible = all(okmap.get(cid, False) for cid in required)

    header = {
        "symbol": symbol, "expiry": expiry, "spot": spot,
        "S1": s1, "S2": s2, "R1": r1, "R2": r2,
        "buffers": {"entry_band": buf},
        "shifted": shifted, "mv": mv, "pcr": pcr, "max_pain": max_pain,
    }
    return {
        "header": header,
        "checks": checks,
        "eligible": eligible,
        "side": trig.side,
        "level": trig.level_tag,
        "trigger_price": trig.price,
    }

def check_now(snapshot: Any) -> Dict[str, Any]:
    try:
        import importlib
        sg = importlib.import_module("agents.signal_generator")
        if hasattr(sg, "check_eligibility"):
            return sg.check_eligibility(snapshot)  # type: ignore[misc]
    except Exception:
        pass
    return _fallback_check(snapshot)
