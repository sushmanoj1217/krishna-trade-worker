# analytics/oc_now_render.py
# Paste-ready: drop this file under analytics/ and replace if exists.
# Renders /oc_now snapshot + 6-checks as a single formatted text block (Telegram-style).

from __future__ import annotations
import os
import math
from typing import Any, Dict, Tuple, Optional
from datetime import datetime, time, timezone
try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

from analytics.oc_refresh_shim import get_refresh  # resolves provider refresh callable

# -------------------------
# Config & small utilities
# -------------------------

IST = ZoneInfo("Asia/Kolkata") if ZoneInfo else timezone.utc

DEFAULT_BUFFERS = {
    "NIFTY": 12.0,
    "BANKNIFTY": 30.0,
    "FINNIFTY": 15.0,
}
DEFAULT_ENTRY_BAND = {
    "NIFTY": 3.0,
    "BANKNIFTY": 7.0,
    "FINNIFTY": 4.0,
}
DEFAULT_TARGET_MIN = {
    "NIFTY": 30.0,
    "BANKNIFTY": 70.0,
    "FINNIFTY": 25.0,
}

MV_BULL_SET = {"bullish", "big_move"}
MV_BEAR_SET = {"bearish", "strong_bearish"}

def _fnum(x: Optional[float]) -> str:
    if x is None or isinstance(x, str):
        return str(x)
    if abs(x) >= 100000:
        return f"{int(round(x)):,}"
    if abs(x) >= 1000:
        return f"{x:,.2f}"
    return f"{x:.2f}"

def _fprice(x: Optional[float]) -> str:
    if x is None:
        return "—"
    return f"{x:.2f}"

def _now_ist() -> datetime:
    return datetime.now(tz=IST) if IST else datetime.now(timezone.utc)

def _get_symbol() -> str:
    return (os.environ.get("OC_SYMBOL") or "NIFTY").strip().upper()

def _get_buf(symbol: str) -> float:
    env = os.environ.get("LEVEL_BUFFER")
    if env:
        try:
            return float(env)
        except ValueError:
            pass
    return DEFAULT_BUFFERS.get(symbol, 12.0)

def _get_entry_band(symbol: str) -> float:
    env = os.environ.get("ENTRY_BAND")
    if env:
        try:
            return float(env)
        except ValueError:
            pass
    return DEFAULT_ENTRY_BAND.get(symbol, 3.0)

def _get_target_min(symbol: str) -> float:
    env = os.environ.get("TARGET_MIN_POINTS")
    if env:
        try:
            return float(env)
        except ValueError:
            pass
    return DEFAULT_TARGET_MIN.get(symbol, 30.0)

# ---------------------------------
# MV derivation (fallback if missing)
# ---------------------------------

def _mv_from_pcr(pcr: Optional[float]) -> Optional[str]:
    if pcr is None:
        return None
    # Loosish thresholds so MV rarely stays unknown:
    # PCR ≥ 1.10 → bullish, PCR ≤ 0.90 → bearish
    if pcr >= 1.10:
        return "bullish"
    if pcr <= 0.90:
        return "bearish"
    return None

def _mv_from_mp(spot: Optional[float], mp: Optional[float], sym: str) -> Optional[str]:
    if spot is None or mp is None:
        return None
    # Use symbol buffer as a "neutral zone" around MP
    buf = DEFAULT_BUFFERS.get(sym, 12.0)
    diff = spot - mp
    if diff > buf:
        return "bullish"
    if diff < -buf:
        return "bearish"
    return None

def _derive_mv_if_missing(s: Dict[str, Any], sym: str) -> str:
    mv = (s.get("mv") or "").strip().lower()
    if mv:
        return mv

    pcr = s.get("pcr")
    mp = s.get("mp")
    spot = s.get("spot")

    mv_pcr = _mv_from_pcr(pcr)
    mv_mp  = _mv_from_mp(spot, mp, sym)

    # Prefer agreement; else fall back to PCR if MP inconclusive;
    # else use MP; else unknown.
    if mv_pcr and mv_mp and mv_pcr == mv_mp:
        return mv_pcr
    if mv_pcr and not mv_mp:
        return mv_pcr
    if mv_mp and not mv_pcr:
        return mv_mp
    return ""  # unknown

# ---------------------------------
# Shifted levels & side selection
# ---------------------------------

def _shifted_levels(s: Dict[str, Any], buf: float) -> Dict[str, float]:
    s1 = float(s.get("s1") or 0)
    s2 = float(s.get("s2") or 0)
    r1 = float(s.get("r1") or 0)
    r2 = float(s.get("r2") or 0)
    return {
        "S1*": s1 - buf if s1 else 0.0,
        "S2*": s2 - buf if s2 else 0.0,
        "R1*": r1 + buf if r1 else 0.0,
        "R2*": r2 + buf if r2 else 0.0,
    }

def _prefer_side(mv: Optional[str], spot: float, sh: Dict[str, float]) -> Tuple[str, str, float]:
    """
    Decide which side (CE/PE) and which closest trigger to evaluate for C1.
    If MV is known, prefer its side; else choose nearest among S* & R*.
    Returns (side, trigger_label, trigger_price)
    """
    mv = (mv or "").strip().lower()
    ce_trigs = {k: v for k, v in sh.items() if k in ("S1*", "S2*") and v}
    pe_trigs = {k: v for k, v in sh.items() if k in ("R1*", "R2*") and v}

    def _nearest(trigs: Dict[str, float]) -> Tuple[str, float]:
        if not trigs:
            return ("", 0.0)
        k = min(trigs, key=lambda name: abs(spot - trigs[name]))
        return (k, trigs[k])

    if mv in MV_BULL_SET and ce_trigs:
        k, v = _nearest(ce_trigs); return ("CE", k, v)
    if mv in MV_BEAR_SET and pe_trigs:
        k, v = _nearest(pe_trigs); return ("PE", k, v)

    # fallback: nearest overall
    k_all = min(
        ((k, v) for k, v in sh.items() if v),
        key=lambda kv: abs(spot - kv[1]),
        default=("", 0.0),
    )
    k, v = k_all
    side = "CE" if k.startswith("S") else "PE"
    return (side, k, v)

# -----------------------
# Checks (C1 ... C6)
# -----------------------

def _c1_level(side: str, spot: float, trig_name: str, trig_price: float, entry_band: float) -> Tuple[bool, str]:
    if trig_price == 0.0 or math.isfinite(trig_price) is False:
        return (False, "no valid trigger")

    dist = spot - trig_price
    if side == "CE":
        # want price to dip to <= trigger; band allows prefill
        if spot <= trig_price:
            return (True, "CROSS")
        if abs(dist) <= entry_band:
            return (False, "NEAR")
        return (False, f"FAR @ {trig_name} ({_fprice(trig_price)})")
    else:  # PE
        # want price to rise to >= trigger
        if spot >= trig_price:
            return (True, "CROSS")
        if abs(dist) <= entry_band:
            return (False, "NEAR")
        return (False, f"FAR @ {trig_name} ({_fprice(trig_price)})")

def _sign(v: Optional[float]) -> Optional[int]:
    if v is None:
        return None
    if v > 0: return +1
    if v < 0: return -1
    return 0

def _c2_mv(side: str, mv: Optional[str]) -> Tuple[bool, str]:
    mv_l = (mv or "").strip().lower()
    if side == "CE":
        ok = mv_l in MV_BULL_SET
    else:
        ok = mv_l in MV_BEAR_SET
    if mv is None or mv_l == "":
        return (False, "MV unknown")
    return (ok, f"MV={mv_l}{' OK' if ok else ' block'}")

def _c3_oi_pattern(side: str, ce_d: Optional[float], pe_d: Optional[float]) -> Tuple[bool, str]:
    s_ce = _sign(ce_d); s_pe = _sign(pe_d)
    def _tag(n: Optional[int]) -> str:
        return '+' if n == 1 else ('-' if n == -1 else '0')

    if s_ce is None or s_pe is None:
        return (False, "OIΔ missing")

    # CE rules (bullish): (CE↓ & PE↑) OR (CE↓ & PE↓) OR (CE↔/↓ & PE↑)
    if side == "CE":
        if (s_ce == -1 and s_pe == +1) or (s_ce == -1 and s_pe == -1) or ((s_ce in (0, -1)) and s_pe == +1):
            return (True, f"CEΔ={_tag(s_ce)} / PEΔ={_tag(s_pe)}")
        return (False, f"CEΔ={_tag(s_ce)} / PEΔ={_tag(s_pe)}")

    # PE rules (bearish): (CE↑ & PE↓) OR (CE↓ & PE↓) OR (CE↑ & PE↔/↓)
    if (s_ce == +1 and s_pe == -1) or (s_ce == -1 and s_pe == -1) or (s_ce == +1 and s_pe in (0, -1)):
        return (True, f"CEΔ={_tag(s_ce)} / PEΔ={_tag(s_pe)}")
    return (False, f"CEΔ={_tag(s_ce)} / PEΔ={_tag(s_pe)}")

def _c4_timing_fresh(age_sec: Optional[float]) -> Tuple[bool, str]:
    now = _now_ist()
    hhmm = now.time()
    in_open_block = time(9, 15) <= hhmm < time(9, 30)
    in_close_block = time(14, 45) <= hhmm < time(15, 15)
    fresh_ok = (age_sec is not None) and (age_sec <= float(os.environ.get("OC_FRESH_MAX_SEC", "90")))
    if in_open_block or in_close_block:
        return (False, "no-trade window")
    if not fresh_ok:
        return (False, f"stale {int(age_sec or 9999)}s>={os.environ.get('OC_FRESH_MAX_SEC','90')}s")
    return (True, f"time OK, fresh {int(age_sec)}s≤{os.environ.get('OC_FRESH_MAX_SEC','90')}s")

def _c5_hygiene() -> Tuple[bool, str]:
    # HOLD via env; caps/dedupe assumed enforced elsewhere (trade_loop).
    hold = (os.environ.get("HOLD") or os.environ.get("SYSTEM_HOLD") or "").strip().lower() in ("1", "true", "yes", "y")
    if hold:
        return (False, "HOLD")
    return (True, "OK")

def _c6_space(side: str, trig_price: float, levels: Dict[str, float], target_min: float) -> Tuple[bool, str]:
    # Space from trigger to the opposite first barrier (S1* vs R1; R1* vs S1)
    if trig_price == 0.0:
        return (False, "no trigger")
    s1 = levels.get("s1") or 0.0
    r1 = levels.get("r1") or 0.0
    if side == "CE":
        if r1 <= 0: return (False, "no R1")
        space = abs(r1 - trig_price)
    else:
        if s1 <= 0: return (False, "no S1")
        space = abs(trig_price - s1)
    ok = space >= target_min
    return (ok, f"space {int(round(space))} ≥ target {int(round(target_min))}" if ok else f"space {int(round(space))} < target {int(round(target_min))}")

# -----------------------
# Render
# -----------------------

def _render_text(s: Dict[str, Any]) -> str:
    symbol = (s.get("symbol") or _get_symbol()).upper()
    buf = _get_buf(symbol)
    entry_band = _get_entry_band(symbol)
    target_min = _get_target_min(symbol)

    spot = float(s.get("spot") or 0)
    s1 = float(s.get("s1") or 0)
    s2 = float(s.get("s2") or 0)
    r1 = float(s.get("r1") or 0)
    r2 = float(s.get("r2") or 0)
    # derive MV if missing
    mv = _derive_mv_if_missing(s, symbol)
    pcr = s.get("pcr")
    mp = s.get("mp")
    ce_d = s.get("ce_oi_delta")
    pe_d = s.get("pe_oi_delta")
    source = s.get("source") or "provider"
    asof = s.get("asof")
    age_sec = s.get("age_sec")
    expiry = s.get("expiry")

    shifted = _shifted_levels(s, buf)
    side, trig_name, trig_price = _prefer_side(mv, spot, shifted)

    c1_ok, c1_reason = _c1_level(side, spot, trig_name, trig_price, entry_band)
    c2_ok, c2_reason = _c2_mv(side, mv)
    c3_ok, c3_reason = _c3_oi_pattern(side, ce_d, pe_d)
    c4_ok, c4_reason = _c4_timing_fresh(age_sec)
    c5_ok, c5_reason = _c5_hygiene()
    c6_ok, c6_reason = _c6_space(side, trig_price, {"s1": s1, "r1": r1}, target_min)

    # Header
    lines = []
    lines.append("OC Snapshot")
    lines.append(f"Symbol: {symbol}  |  Exp: {expiry or '—'}  |  Spot: {_fprice(spot)}")
    lines.append(
        f"Levels: S1 {_fprice(s1)}  S2 {_fprice(s2)}  R1 {_fprice(r1)}  R2 {_fprice(r2)}"
    )
    lines.append(
        f"Shifted: S1 `{_fprice(shifted['S1*'])}`  S2 {_fprice(shifted['S2*'])}  R1 `{_fprice(shifted['R1*'])}`  R2 {_fprice(shifted['R2*'])}"
    )
    mv_txt = (mv or "—")
    lines.append(
        f"Buffer: {int(buf) if float(buf).is_integer() else buf}  |  MV: {mv_txt}  |  PCR: {_fnum(pcr) if pcr not in (None, '') else '—'}  |  MP: {_fprice(mp) if mp else '—'}"
    )
    age_txt = f"{int(age_sec)}s" if age_sec is not None else "—"
    if asof:
        lines.append(f"Source: {source}  |  As-of: {asof}  |  Age: {age_txt}")
    else:
        lines.append(f"Source: {source}  |  Age: {age_txt}")

    # Checks
    lines.append("")
    lines.append("Checks")
    lines.append(f"- C1: {'✅' if c1_ok else '❌'} {c1_reason}")
    lines.append(f"- C2: {'✅' if c2_ok else '❌'} {c2_reason}")
    if ce_d is None or pe_d is None:
        lines.append(f"- C3: ❌ OIΔ missing")
    else:
        lines.append(f"- C3: {'✅' if c3_ok else '❌'} {c3_reason}  (raw CEΔ={_fnum(ce_d)}, PEΔ={_fnum(pe_d)})")
    lines.append(f"- C4: {'✅' if c4_ok else '❌'} {c4_reason}")
    lines.append(f"- C5: {'✅' if c5_ok else '❌'} {c5_reason}")
    lines.append(f"- C6: {'✅' if c6_ok else '❌'} {c6_reason}")

    # Summary
    lines.append("")
    if all([c1_ok, c2_ok, c3_ok, c4_ok, c5_ok, c6_ok]):
        lines.append(f"Summary: ✅ Eligible — {side} @ {trig_name} ({_fprice(trig_price)})")
    else:
        failed = []
        if not c1_ok: failed.append("C1")
        if not c2_ok: failed.append("C2")
        if not c3_ok: failed.append("C3")
        if not c4_ok: failed.append("C4")
        if not c5_ok: failed.append("C5")
        if not c6_ok: failed.append("C6")
        lines.append(f"Summary: ❌ Not eligible — failed: {', '.join(failed) if failed else '—'}")

    return "\n".join(lines)

# -----------------------
# Public coroutine
# -----------------------

async def render_now() -> str:
    """
    Pull a fresh snapshot via provider (get_refresh) and render text.
    """
    refresh_once = get_refresh()
    snap = await refresh_once({})
    # ensure minimal keys
    for k in ("symbol", "s1", "s2", "r1", "r2", "spot"):
        snap.setdefault(k, None)
    return _render_text(snap)

# -----------------------
# CLI entry
# -----------------------

if __name__ == "__main__":
    import asyncio
    print(asyncio.run(render_now()))
