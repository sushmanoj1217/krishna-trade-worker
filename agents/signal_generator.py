from __future__ import annotations
from typing import Dict, Any
from utils.params import Params
from utils.rr import rr_feasible
from analytics.oc_refresh import get_snapshot
from utils.ids import new_signal_id
from integrations import sheets as sh

def _mv_bits(pcr: float | None, spot: float, mp: float, p: Params) -> dict:
    out = {"pcr_ok": None, "mp_ok": None, "basis": ""}
    if pcr is not None:
        out["pcr_ok"] = (pcr >= p.pcr_bull_high) or (pcr <= p.pcr_bear_low)
    out["mp_ok"] = (spot >= mp + p.mp_support_dist) or (spot <= mp - p.mp_support_dist)
    if out["pcr_ok"]:
        out["basis"] = "PCR band"
    if out["mp_ok"]:
        out["basis"] = (out["basis"] + "+MP" if out["basis"] else "MP dist")
    return out

def _oc_pattern_bits(oi: dict) -> dict:
    # very light placeholder; just note that we have oi maps
    ce = oi.get("ce", {})
    pe = oi.get("pe", {})
    basis = "oi_maps" if ce and pe else ""
    return {"basis": basis}

def build_checks_for_snapshot(snap, p: Params) -> Dict[str, Any]:
    """
    Return dict: C1..C6 (bool/None), mv_bits, oc_bits, eligible (bool)
    """
    c = {f"C{i}": None for i in range(1, 7)}
    # C1 TriggerCross (placeholder false)
    c["C1"] = False
    # C2 FlowBias@Trigger (placeholder False)
    c["C2"] = False
    # C3 WallSupport(ΣΔOI) (placeholder False)
    c["C3"] = False
    # C4 Momentum(3–5m) (placeholder False)
    c["C4"] = False
    # C5 RR feasible: we check a dummy CE at S1*
    c["C5"] = rr_feasible("CE", snap.s1, snap.s1 - p.buffer_points, p)
    # C6 SystemGates: (no-trade windows, events) — assume True for now
    c["C6"] = True

    mv = _mv_bits(snap.pcr, snap.spot, snap.max_pain, p)
    ocb = _oc_pattern_bits(getattr(snap, "oi", {}) if hasattr(snap, "oi") else {})

    eligible = all(v is True for v in (c["C1"], c["C2"], c["C3"], c["C4"], c["C5"], c["C6"])) and (mv["pcr_ok"] or mv["mp_ok"])
    return {**c, "mv_bits": mv, "oc_bits": ocb, "eligible": eligible}

async def signal_loop_once():
    snap = get_snapshot()
    if not snap:
        return
    p = Params.from_env()
    checks = build_checks_for_snapshot(snap, p)
    sig = {
        "signal_id": new_signal_id(),
        "ts": snap.ts.strftime("%Y%m%d-%H%M%S"),
        "side": "CE",
        "trigger": "S1*",
        "c1": checks["C1"], "c2": checks["C2"], "c3": checks["C3"], "c4": checks["C4"], "c5": checks["C5"], "c6": checks["C6"],
        "eligible": checks["eligible"],
        "reason": "auto",
        "mv_pcr_ok": checks["mv_bits"]["pcr_ok"],
        "mv_mp_ok": checks["mv_bits"]["mp_ok"],
        "mv_basis": checks["mv_bits"]["basis"],
        "oc_bull_normal": "", "oc_bull_shortcover": "", "oc_bear_normal": "", "oc_bear_crash": "",
        "oc_pattern_basis": checks["oc_bits"]["basis"],
        "near_cross": "",
        "notes": ""
    }
    try:
        await sh.log_signal_row(sig)
    except Exception as e:
        from utils.logger import log
        log.error(f"Signals append failed: {e}")
