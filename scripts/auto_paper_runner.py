#!/usr/bin/env python3
from __future__ import annotations
import os, sys, time, math, asyncio
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List
from datetime import datetime
from zoneinfo import ZoneInfo

# PATH bootstrap so project imports work when run as a file
_THIS = os.path.abspath(__file__)
_SCRIPTS = os.path.dirname(_THIS)
_ROOT = os.path.dirname(_SCRIPTS)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from analytics.oc_refresh_shim import get_refresh  # resolves DHAN provider
from utils.sheets_writer import append_signal, append_trade_open, recent_signal_exists

IST = ZoneInfo("Asia/Kolkata")

# ---- Params (env or defaults) ----
@dataclass
class Params:
    LEVEL_BUFFER: float = float(os.environ.get("LEVEL_BUFFER", 12))
    ENTRY_BAND: float = float(os.environ.get("ENTRY_BAND", 3))
    TARGET_MIN_POINTS: float = float(os.environ.get("TARGET_MIN_POINTS", 30))
    SYMBOL: str = os.environ.get("OC_SYMBOL", "NIFTY")
    LOTS: int = int(os.environ.get("PAPER_QTY_LOTS", "1"))

def fmt(x: Optional[float]) -> str:
    return "—" if x is None else f"{x:,.2f}"

def shifted(s1: float, s2: float, r1: float, r2: float, buf: float) -> Tuple[float,float,float,float]:
    return (s1 - buf, s2 - buf, r1 + buf, r2 + buf)

def within_band(spot: float, trg: float, band: float) -> bool:
    return abs(spot - trg) <= band

def choose_level(spot: float, s1s: float, s2s: float, r1s: float, r2s: float, band: float):
    # returns (hit:bool, side:str|None, level_label:str|None, trigger:float|None, reason:str)
    options = [
        ("CE","S1*", s1s, abs(spot - s1s)),
        ("CE","S2*", s2s, abs(spot - s2s)),
        ("PE","R1*", r1s, abs(spot - r1s)),
        ("PE","R2*", r2s, abs(spot - r2s)),
    ]
    options.sort(key=lambda x: x[3])
    for side, label, trg, _ in options:
        if within_band(spot, trg, band):
            return True, side, label, trg, f"NEAR/CROSS @ {label} {fmt(trg)}"
    side_n, label_n, trg_n, d = options[0]
    return False, None, None, None, f"FAR @ {label_n} {fmt(trg_n)} (Δ≈{fmt(d)})"

ALLOWED_MV_CE = {"bullish", "big_move"}
ALLOWED_MV_PE = {"bearish", "strong_bearish"}

def coerce_mv(snap: Dict[str, Any]) -> str:
    mv = (snap.get("mv") or "").strip().lower()
    if mv in {"bullish","big_move","bearish","strong_bearish"}:
        return mv
    # simple fallback via PCR / MP
    pcr = snap.get("pcr")
    if isinstance(pcr, (int,float)):
        if pcr >= 1.05: return "bullish"
        if pcr <= 0.95: return "bearish"
    mp = snap.get("mp"); spot = snap.get("spot")
    if isinstance(mp,(int,float)) and isinstance(spot,(int,float)):
        return "bullish" if spot < mp else "bearish"
    return "—"

def c2_mv_ok(mv: str, side: Optional[str]) -> Tuple[bool,str]:
    if not side: return False, "no side (C1 fail)"
    ok = (mv in ALLOWED_MV_CE) if side=="CE" else (mv in ALLOWED_MV_PE)
    return (ok, f"MV={mv} OK" if ok else f"MV={mv} block")

def c3_oi_ok(ce_delta: Optional[float], pe_delta: Optional[float], side: Optional[str]) -> Tuple[bool,str]:
    if side is None: return False, "no side (C1 fail)"
    if ce_delta is None or pe_delta is None: return False, "OIΔ missing"
    c_up, c_dn, c_eq = ce_delta>0, ce_delta<0, ce_delta==0
    p_up, p_dn, p_eq = pe_delta>0, pe_delta<0, pe_delta==0
    if side=="CE":
        if (c_dn or c_eq) and p_up: return True, "CEΔ↓/~ & PEΔ↑"
        if (c_dn or c_eq) and (p_dn or p_eq): return True, "CEΔ↓/~ & PEΔ↓/~"
        if (c_eq or c_dn) and p_up: return True, "CEΔ~/↓ & PEΔ↑"
        return False, f"CEΔ={'+' if c_up else ('0' if c_eq else '-') } / PEΔ={'+' if p_up else ('0' if p_eq else '-') }"
    else:
        if c_up and (p_dn or p_eq): return True, "CEΔ↑ & PEΔ↓/~"
        if (c_dn or c_eq) and (p_dn or p_eq): return True, "CEΔ↓/~ & PEΔ↓/~"
        if c_up and (p_eq or p_dn): return True, "CEΔ↑ & PEΔ~/↓"
        return False, f"CEΔ={'+' if c_up else ('0' if c_eq else '-') } / PEΔ={'+' if p_up else ('0' if p_eq else '-') }"

def c4_timefresh(age_sec: Optional[float], now: datetime) -> Tuple[bool,str]:
    h, m = now.hour, now.minute
    in_915_930   = (h == 9 and 15 <= m < 30)
    in_1445_1515 = ((h == 14 and m >= 45) or (h == 15 and m < 15))
    if in_915_930 or in_1445_1515: return False, "no-trade window"
    if isinstance(age_sec,(int,float)) and age_sec > 90: return False, f"stale {int(age_sec)}s>90s"
    return True, f"fresh {int(age_sec or 0)}s"

async def eval_once(p: Params) -> Tuple[Dict[str, Any], Dict[str, Any] | None, str]:
    refresh = get_refresh()
    snap = await refresh({})  # provider snapshot
    # Validate fields
    for k in ("spot","s1","s2","r1","r2"):
        if k not in snap or snap[k] is None:
            return snap, None, "incomplete snapshot"

    s1s,s2s,r1s,r2s = shifted(snap["s1"], snap["s2"], snap["r1"], snap["r2"], p.LEVEL_BUFFER)
    hit, side, level_label, trig, c1_r = choose_level(snap["spot"], s1s, s2s, r1s, r2s, p.ENTRY_BAND)
    mv = coerce_mv(snap)
    c2_ok, c2_r = c2_mv_ok(mv, side)
    c3_ok, c3_r = c3_oi_ok(snap.get("ce_oi_delta"), snap.get("pe_oi_delta"), side)
    c4_ok, c4_r = c4_timefresh(snap.get("age_sec"), datetime.now(IST))

    # Hygiene (caps/dedupe/liq) — minimal here: handled below via signal dedupe
    c5_ok, c5_r = True, "OK"

    # Space to next level (RR room)
    if side == "CE" and trig is not None:
        space = (snap["r1"] - trig)
    elif side == "PE" and trig is not None:
        space = (trig - snap["s1"])
    else:
        space = None
    c6_ok = (space is not None and space >= p.TARGET_MIN_POINTS)
    c6_r = (f"space {int(space)} ≥ target {int(p.TARGET_MIN_POINTS)}" if c6_ok else
            ("no side/trigger" if trig is None else f"space {int(space or 0)} < target {int(p.TARGET_MIN_POINTS)}"))

    ok = hit and c2_ok and c3_ok and c4_ok and c5_ok and c6_ok

    # Build human summary
    lines = []
    lines.append("OC Snapshot")
    lines.append(f"Symbol: {snap.get('symbol','?')}  |  Exp: {snap.get('expiry','?')}  |  Spot: {fmt(snap.get('spot'))}")
    lines.append(f"Levels: S1 {fmt(snap.get('s1'))}  S2 {fmt(snap.get('s2'))}  R1 {fmt(snap.get('r1'))}  R2 {fmt(snap.get('r2'))}")
    lines.append(f"Shifted: S1 `{fmt(s1s)}`  S2 {fmt(s2s)}  R1 `{fmt(r1s)}`  R2 {fmt(r2s)}")
    lines.append(f"Buffer: {int(p.LEVEL_BUFFER)}  |  MV: {mv}  |  PCR: {fmt(snap.get('pcr'))}  |  MP: {fmt(snap.get('mp'))}")
    lines.append(f"Source: {snap.get('source','?')}  |  As-of: {snap.get('asof','—')}  |  Age: {int(snap.get('age_sec') or 0)}s")
    lines.append("")
    lines.append("Checks")
    lines.append(f"- C1: {'✅' if hit else '❌'} {c1_r}")
    lines.append(f"- C2: {'✅' if c2_ok else '❌'} {c2_r}")
    lines.append(f"- C3: {'✅' if c3_ok else '❌'} {c3_r}")
    lines.append(f"- C4: {'✅' if c4_ok else '❌'} {c4_r}")
    lines.append(f"- C5: ✅ {c5_r}")
    lines.append(f"- C6: {'✅' if c6_ok else '❌'} {c6_r}")
    lines.append("")
    if ok and side and trig is not None:
        lines.append(f"Summary: ✅ Eligible — {side} @ {fmt(trig)} ({level_label})")
    else:
        fails = []
        if not hit: fails.append("C1")
        if not c2_ok: fails.append("C2")
        if not c3_ok: fails.append("C3")
        if not c4_ok: fails.append("C4")
        if not c6_ok: fails.append("C6")
        lines.append(f"Summary: ❌ Not eligible — failed: {', '.join(fails) if fails else '—'}")

    text = "\n".join(lines)

    # If eligible, prepare payloads for Sheets
    if ok and side and trig is not None and level_label:
        signal = {
            "Symbol": snap.get("symbol","NIFTY"),
            "Expiry": snap.get("expiry",""),
            "Side": side,
            "Level": level_label,
            "TriggerPrice": trig,
            "Spot": snap.get("spot"),
            "MV": mv,
            "PCR": snap.get("pcr"),
            "MP": snap.get("mp"),
            "CE_OI_Delta": snap.get("ce_oi_delta"),
            "PE_OI_Delta": snap.get("pe_oi_delta"),
            "Source": snap.get("source"),
            "AsOf": snap.get("asof"),
            "AgeSec": snap.get("age_sec"),
            "Eligibility": "YES",
            "Reason": "C1..C6 OK",
            "Mode": "paper",
        }
        trade = {
            "Symbol": signal["Symbol"],
            "Expiry": signal["Expiry"],
            "Side": side,
            "Level": level_label,
            "EntryPrice": trig,
            "SpotAtEntry": snap.get("spot"),
            "QtyLots": os.environ.get("PAPER_QTY_LOTS", "1"),
            "Mode": "paper",
        }
        return snap, {"signal": signal, "trade": trade}, text

    return snap, None, text

async def once_and_maybe_append():
    p = Params()
    snap, payloads, text = await eval_once(p)
    print(text, flush=True)
    if payloads:
        # dedupe key
        key = {
            "Symbol": payloads["signal"]["Symbol"],
            "Expiry": payloads["signal"]["Expiry"],
            "Side": payloads["signal"]["Side"],
            "Level": payloads["signal"]["Level"],
            "TriggerPrice": payloads["signal"]["TriggerPrice"],
            "Mode": "paper",
        }
        if recent_signal_exists(key):
            print("Deduped: similar paper signal already exists in recent rows.", flush=True)
            return
        append_signal(payloads["signal"])
        append_trade_open(payloads["trade"])
        print("Appended: Signals + Trades (paper OPEN).", flush=True)

async def loop_main(interval_sec: int):
    while True:
        try:
            await once_and_maybe_append()
        except Exception as e:
            print("[ERR]", e, flush=True)
        await asyncio.sleep(max(1, interval_sec))

def parse_args(argv: List[str]) -> Tuple[bool,int]:
    if "--once" in argv:
        return True, 0
    if "--loop" in argv:
        i = argv.index("--loop")
        iv = int(argv[i+1]) if i+1 < len(argv) else 18
        return False, iv
    return True, 0

if __name__ == "__main__":
    once, iv = parse_args(sys.argv[1:])
    if once:
        sys.exit(asyncio.run(once_and_maybe_append()))
    else:
        asyncio.run(loop_main(iv))
