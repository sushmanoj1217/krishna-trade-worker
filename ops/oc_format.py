# ops/oc_format.py
from __future__ import annotations

from agents.logger import get_latest_status_map

def _fmt(v, digits=2):
    try:
        if v is None or v == "":
            return "-"
        return f"{float(v):.{digits}f}"
    except Exception:
        return str(v)

def format_oc_reply(snapshot: dict) -> str:
    """
    snapshot expects:
      symbol, spot, S1,S2,R1,R2, S1*,S2*,R1*,R2*, MV
    PCR/VIX are read from Status sheet (latest) if not provided.
    """
    ctx = get_latest_status_map()
    pcr = snapshot.get("PCR") or ctx.get("PCR", "-")
    vix = snapshot.get("VIX") or ctx.get("VIX", "-")

    sym = snapshot.get("symbol", "NIFTY")
    spot = _fmt(snapshot.get("spot"))

    s1 = _fmt(snapshot.get("S1")); s1s = _fmt(snapshot.get("S1*"))
    s2 = _fmt(snapshot.get("S2")); s2s = _fmt(snapshot.get("S2*"))
    r1 = _fmt(snapshot.get("R1")); r1s = _fmt(snapshot.get("R1*"))
    r2 = _fmt(snapshot.get("R2")); r2s = _fmt(snapshot.get("R2*"))
    mv = snapshot.get("MV", "-")

    lines = [
        f"📈 {sym} @ {spot} | MV: {mv}",
        f"S: S1 {s1} (S1* {s1s}) • S2 {s2} (S2* {s2s})",
        f"R: R1 {r1} (R1* {r1s}) • R2 {r2} (R2* {r2s})",
        f"Context → PCR: {pcr} • VIX: {vix}",
    ]
    return "\n".join(lines)
