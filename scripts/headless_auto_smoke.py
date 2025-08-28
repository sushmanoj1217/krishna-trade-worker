#!/usr/bin/env python3
"""
Headless Auto Smoke Test
- Live DHAN OC snapshot fetch (via analytics.oc_refresh_shim.get_refresh)
- Applies approved C1..C6 entry rules (CE at S1*/S2*, PE at R1*/R2*)
- Prints a clean, human-readable summary
- No Telegram, no order placement (safe)

Run (any ONE of these styles):
  # A) Module mode (best)
  python -m scripts.headless_auto_smoke --once
  python -m scripts.headless_auto_smoke --loop 18

  # B) With PYTHONPATH set
  export PYTHONPATH=/opt/render/project/src
  python scripts/headless_auto_smoke.py --once

  # C) Direct run (this file has sys.path bootstrap so it also works)
  python scripts/headless_auto_smoke.py --once
"""

from __future__ import annotations
import os, sys, math, time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List
from zoneinfo import ZoneInfo
from datetime import datetime

# ---- PATH BOOTSTRAP: add project root so 'analytics', 'utils', etc. resolve ----
_THIS = os.path.abspath(__file__)
_SCRIPTS_DIR = os.path.dirname(_THIS)                 # .../src/scripts
_ROOT = os.path.dirname(_SCRIPTS_DIR)                 # .../src
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

# --- project deps (existing) ---
try:
    from utils.params import Params
except Exception:
    # Minimal fallback: env-only params
    @dataclass
    class Params:
        LEVEL_BUFFER: float = float(os.environ.get("LEVEL_BUFFER", 12))
        ENTRY_BAND: float = float(os.environ.get("ENTRY_BAND", 3))
        TARGET_MIN_POINTS: float = float(os.environ.get("TARGET_MIN_POINTS", 30))
        MV_REV_CONFIRM: float = float(os.environ.get("MV_REV_CONFIRM", 2))
        SYMBOL: str = os.environ.get("OC_SYMBOL", "NIFTY")
        @classmethod
        def from_env(cls): return cls()
else:
    # Prefer real Params loader (reads sheet overrides too)
    Params = Params

# resolve DHAN provider via shim
try:
    from analytics.oc_refresh_shim import get_refresh
except ModuleNotFoundError as e:
    raise SystemExit(
        "[FATAL] Can't import 'analytics'.\n"
        f"  cwd={os.getcwd()}\n"
        f"  this={_THIS}\n"
        f"  sys.path[0]={sys.path[0]}\n"
        "Fixes:\n"
        "  - Run as module:  python -m scripts.headless_auto_smoke --once\n"
        "  - Or set PYTHONPATH: export PYTHONPATH=/opt/render/project/src\n"
    ) from e

IST = ZoneInfo("Asia/Kolkata")

ALLOWED_MV_CE = {"bullish", "big_move"}
ALLOWED_MV_PE = {"bearish", "strong_bearish"}

def fmt_pts(x: Optional[float]) -> str:
    if x is None: return "—"
    return f"{x:,.2f}"

def derive_mv_if_missing(snap: Dict[str, Any]) -> str:
    """Tie-break: use PCR & MaxPain distance when 'mv' missing/neutral."""
    mv = (snap.get("mv") or "").strip().lower()
    if mv in {"bullish","big_move","bearish","strong_bearish"}:
        return mv
    pcr = snap.get("pcr")
    mp  = snap.get("mp")
    spot = snap.get("spot")
    # Conservative PCR thresholds:
    if isinstance(pcr, (int, float)):
        if pcr >= 1.05: return "bullish"
        if pcr <= 0.95: return "bearish"
    if isinstance(spot, (int, float)) and isinstance(mp, (int, float)):
        return "bullish" if spot < mp else "bearish"
    return "—"

def shifted_levels(s1: float, s2: float, r1: float, r2: float, buf: float) -> Tuple[float,float,float,float]:
    return (s1 - buf, s2 - buf, r1 + buf, r2 + buf)

def within_band(spot: float, trg: float, band: float) -> bool:
    return abs(spot - trg) <= band

def c1_level_trigger(spot: float, s1s: float, s2s: float, r1s: float, r2s: float, band: float) -> Tuple[bool, str, Optional[str], Optional[float]]:
    """
    Returns: (ok, reason, side, trigger_price)
    side: 'CE' for S1*/S2*, 'PE' for R1*/R2*
    """
    # Prioritize nearer trigger
    dists = [
        ("CE", s1s, abs(spot - s1s)),
        ("CE", s2s, abs(spot - s2s)),
        ("PE", r1s, abs(spot - r1s)),
        ("PE", r2s, abs(spot - r2s)),
    ]
    dists.sort(key=lambda x: x[2])
    for side, trg, _ in dists:
        if within_band(spot, trg, band):
            return True, f"NEAR/CROSS @ {side} {fmt_pts(trg)}", side, trg
    side_n, trg_n, d = dists[0]
    return False, f"FAR @ {side_n} {fmt_pts(trg_n)} (Δ≈{fmt_pts(d)})", None, None

def c2_mv_gate(mv: str, side: Optional[str]) -> Tuple[bool, str]:
    if not side:
        return False, "no side (C1 fail)"
    mvn = (mv or "—").lower()
    if side == "CE":
        ok = mvn in ALLOWED_MV_CE
    else:
        ok = mvn in ALLOWED_MV_PE
    if ok: return True, f"MV={mvn} OK"
    return False, f"MV={mvn} block"

def c3_oi_pattern(ce_delta: Optional[float], pe_delta: Optional[float], side: Optional[str]) -> Tuple[bool, str]:
    if side is None:
        return False, "no side (C1 fail)"
    if ce_delta is None or pe_delta is None:
        return False, "OIΔ missing"
    # Normalize signs
    c_up  = ce_delta  > 0
    c_dn  = ce_delta  < 0
    c_eq  = ce_delta == 0
    p_up  = pe_delta  > 0
    p_dn  = pe_delta  < 0
    p_eq  = pe_delta == 0

    if side == "CE":  # bullish patterns
        if (c_dn or c_eq) and p_up: return True, f"CEΔ={int(ce_delta):,}↓ / PEΔ={int(pe_delta):,}↑".replace(",", " ")
        if (c_dn or c_eq) and (p_dn or p_eq): return True, f"CEΔ={int(ce_delta):,}↓ / PEΔ={int(pe_delta):,}↓".replace(",", " ")
        if (c_eq or c_dn) and p_up: return True, f"CEΔ~ / PEΔ↑"
        return False, f"CEΔ={'+' if c_up else ('0' if c_eq else '-') } / PEΔ={'+' if p_up else ('0' if p_eq else '-') }  (raw CEΔ={ce_delta:,.0f}, PEΔ={pe_delta:,.0f})".replace(",", " ")
    else:            # PE (bearish patterns)
        if c_up and (p_dn or p_eq): return True, f"CEΔ={int(ce_delta):,}↑ / PEΔ={int(pe_delta):,}↓".replace(",", " ")
        if (c_dn or c_eq) and (p_dn or p_eq): return True, f"CEΔ={int(ce_delta):,}↓ / PEΔ={int(pe_delta):,}↓".replace(",", " ")
        if c_up and (p_eq or p_dn): return True, f"CEΔ↑ / PEΔ~|↓"
        return False, f"CEΔ={'+' if c_up else ('0' if c_eq else '-') } / PEΔ={'+' if p_up else ('0' if p_eq else '-') }  (raw CEΔ={ce_delta:,.0f}, PEΔ={pe_delta:,.0f})".replace(",", " ")

def c4_time_and_fresh(age_sec: Optional[float], now: datetime) -> Tuple[bool, str]:
    # No-trade windows: 09:15–09:30, 14:45–15:15
    h, m = now.hour, now.minute
    in_915_930   = (h == 9 and 15 <= m < 30)
    in_1445_1515 = ((h == 14 and m >= 45) or (h == 15 and m < 15))
    if in_915_930 or in_1445_1515:
        return False, "no-trade window"
    if isinstance(age_sec, (int,float)) and age_sec > 90:
        return False, f"stale {int(age_sec)}s>90s"
    return True, f"time OK, fresh {int(age_sec or 0)}s≤90s"

def c5_hygiene_default() -> Tuple[bool, str]:
    # Smoke mode: assume OK (real system already enforces caps/dedupe/spread)
    return True, "OK"

def c6_space(side: Optional[str], trig: Optional[float], s1: float, s2: float, r1: float, r2: float, target_min: float) -> Tuple[bool, str]:
    if side is None or trig is None:
        return False, "no side/trigger (C1 fail)"
    if side == "CE":
        space = (r1 - trig) if (r1 is not None and trig is not None) else None
    else:
        space = (trig - s1) if (s1 is not None and trig is not None) else None
    if space is None:
        return False, "space n/a"
    ok = space >= target_min
    return ok, f"space {int(space)} ≥ target {int(target_min)}" if ok else f"space {int(space)} < target {int(target_min)}"

def render_snapshot_block(s: Dict[str, Any], buf: float) -> str:
    parts = []
    parts.append("OC Snapshot")
    parts.append(f"Symbol: {s.get('symbol','?')}  |  Exp: {s.get('expiry','?')}  |  Spot: {fmt_pts(s.get('spot'))}")
    parts.append(f"Levels: S1 {fmt_pts(s.get('s1'))}  S2 {fmt_pts(s.get('s2'))}  R1 {fmt_pts(s.get('r1'))}  R2 {fmt_pts(s.get('r2'))}")
    s1s, s2s, r1s, r2s = shifted_levels(s['s1'], s['s2'], s['r1'], s['r2'], buf)
    parts.append(f"Shifted: S1 `{fmt_pts(s1s)}`  S2 {fmt_pts(s2s)}  R1 `{fmt_pts(r1s)}`  R2 {fmt_pts(r2s)}")
    parts.append(f"Buffer: {int(buf)}  |  MV: {s.get('mv','—')}  |  PCR: {fmt_pts(s.get('pcr'))}  |  MP: {fmt_pts(s.get('mp'))}")
    parts.append(f"Source: {s.get('source','?')}  |  As-of: {s.get('asof','—')}  |  Age: {int(s.get('age_sec') or 0)}s")
    return "\n".join(parts)

async def once() -> int:
    # Params
    try:
        p = Params.from_env()
    except Exception:
        p = Params.from_env()  # fallback dataclass
    refresh = get_refresh()
    snap = await refresh(p if hasattr(p, "__dict__") else {})  # provider snapshot

    needed = ["spot","s1","s2","r1","r2"]
    if any(k not in snap or snap[k] is None for k in needed):
        print("Snapshot incomplete:", {k: snap.get(k) for k in needed})
        return 2

    # derive MV if missing/neutral
    mv = derive_mv_if_missing(snap)
    snap["mv"] = mv

    # render top
    print(render_snapshot_block(snap, float(getattr(p, "LEVEL_BUFFER", 12.0))))
    print()

    # C1..C6
    spot = float(snap["spot"])
    s1, s2, r1, r2 = float(snap["s1"]), float(snap["s2"]), float(snap["r1"]), float(snap["r2"])
    buf = float(getattr(p, "LEVEL_BUFFER", 12.0))
    band = float(getattr(p, "ENTRY_BAND", 3.0))
    target_min = float(getattr(p, "TARGET_MIN_POINTS", 30.0))

    s1s, s2s, r1s, r2s = shifted_levels(s1,s2,r1,r2,buf)
    c1_ok, c1_reason, side, trig = c1_level_trigger(spot, s1s, s2s, r1s, r2s, band)
    c2_ok, c2_reason = c2_mv_gate(mv, side)
    c3_ok, c3_reason = c3_oi_pattern(snap.get("ce_oi_delta"), snap.get("pe_oi_delta"), side)
    now = datetime.now(IST)
    c4_ok, c4_reason = c4_time_and_fresh(snap.get("age_sec"), now)
    c5_ok, c5_reason = c5_hygiene_default()
    c6_ok, c6_reason = c6_space(side, trig, s1, s2, r1, r2, target_min)

    checks = [
        ("C1", c1_ok, c1_reason),
        ("C2", c2_ok, c2_reason),
        ("C3", c3_ok, c3_reason),
        ("C4", c4_ok, c4_reason),
        ("C5", c5_ok, c5_reason),
        ("C6", c6_ok, c6_reason),
    ]

    print("Checks")
    for cid, ok, rsn in checks:
        mark = "✅" if ok else "❌"
        print(f"- {cid}: {mark} {rsn}")

    all_ok = all(ok for _, ok, _ in checks)
    print()
    if all_ok and side and trig is not None:
        print(f"Summary: ✅ Eligible — {side} @ {fmt_pts(trig)}")
        return 0
    else:
        fails = [cid for cid, ok, _ in checks if not ok]
        if not fails:
            print("Summary: ❔ No decision")
        else:
            print(f"Summary: ❌ Not eligible — failed: {', '.join(fails)}")
        return 1

async def loop_main(interval: int):
    import asyncio, time
    while True:
        try:
            rc = await once()
        except Exception as e:
            print("[ERR]", e, file=sys.stderr)
            rc = 3
        await asyncio.sleep(max(1, interval) + (time.time_ns() % 4))

def parse_args(argv: List[str]) -> Tuple[bool, int]:
    once_only = "--once" in argv
    if "--loop" in argv:
        try:
            idx = argv.index("--loop")
            iv = int(argv[idx+1]) if idx+1 < len(argv) else 18
        except Exception:
            iv = 18
        return False, iv
    if once_only:
        return True, 0
    return True, 0

if __name__ == "__main__":
    import asyncio
    once_only, iv = parse_args(sys.argv[1:])
    if once_only:
        sys.exit(asyncio.run(once()))
    else:
        asyncio.run(loop_main(iv))
