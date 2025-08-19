# analytics/oc_refresh.py
import os
from utils.logger import log
from utils.params import Params
from utils.cache import OCSnapshot, set_snapshot
from integrations import sheets as sh
from integrations.option_chain_dhan import fetch_levels

SYMBOL = os.getenv("OC_SYMBOL", "NIFTY").upper()
MODE = os.getenv("OC_MODE", "sheet").lower()

def _buffer_for_symbol(params: Params) -> int:
    return params.buffer_points()

def _mv_flags_and_basis(spot, pcr, max_pain, mp_dist_needed, p: Params) -> dict:
    """
    Compute MV 1-of-2 booleans for CE and PE sides + basis strings.
    CE ok if (PCR >= bull_high) OR (spot >= max_pain + dist)
    PE ok if (PCR <= bear_low)  OR (spot <= max_pain - dist)
    """
    bull_h = p.pcr_bull_high()
    bear_l = p.pcr_bear_low()
    ce_ok = False
    pe_ok = False
    ce_basis = []
    pe_basis = []

    if pcr is not None:
        if pcr >= bull_h:
            ce_ok = True
            ce_basis.append(f"PCR {pcr} ≥ {bull_h}")
        if pcr <= bear_l:
            pe_ok = True
            pe_basis.append(f"PCR {pcr} ≤ {bear_l}")

    if spot is not None and max_pain is not None:
        delta = round(spot - max_pain, 2)
        if spot >= max_pain + mp_dist_needed:
            ce_ok = True
            ce_basis.append(f"MP Δ +{delta} ≥ {mp_dist_needed}")
        if spot <= max_pain - mp_dist_needed:
            pe_ok = True
            pe_basis.append(f"MP Δ {delta} ≤ -{mp_dist_needed}")

    return {
        "ce_ok": ce_ok,
        "pe_ok": pe_ok,
        "ce_basis": " | ".join(ce_basis) if ce_basis else "—",
        "pe_basis": " | ".join(pe_basis) if pe_basis else "—",
    }

def refresh_once() -> OCSnapshot | None:
    p = Params()
    try:
        if MODE == "dhan":
            data = fetch_levels()
        else:
            row = sh.last_row("OC_Live")
            if not row:
                log.warning("OC_Live empty in sheet")
                return None
            data = {
                "spot": float(row.get("spot", 0) or 0),
                "s1": float(row.get("s1", 0) or 0),
                "s2": float(row.get("s2", 0) or 0),
                "r1": float(row.get("r1", 0) or 0),
                "r2": float(row.get("r2", 0) or 0),
                "pcr": float(row.get("pcr", 0) or 0),
                "max_pain": float(row.get("max_pain", 0) or 0),
                "expiry": row.get("expiry", ""),
            }

        b = _buffer_for_symbol(p)
        s1s = (data["s1"] - b) if data.get("s1") else None
        s2s = (data["s2"] - b) if data.get("s2") else None
        r1s = (data["r1"] + b) if data.get("r1") else None
        r2s = (data["r2"] + b) if data.get("r2") else None

        mp = data.get("max_pain")
        spot = data.get("spot")
        mpd = (spot - mp) if (mp is not None and spot is not None) else None

        # MV flags (for 1-of-2 gate)
        mp_dist_needed = p.mp_support_dist()
        mv = _mv_flags_and_basis(spot, data.get("pcr"), mp, mp_dist_needed, p)

        # bias tags (light)
        bias = None
        if mp is not None and mpd is not None:
            if spot >= mp + mp_dist_needed:
                bias = "mv_bull_mp"
            elif spot <= mp - mp_dist_needed:
                bias = "mv_bear_mp"

        snap = OCSnapshot(
            spot=spot,
            s1=data.get("s1"),
            s2=data.get("s2"),
            r1=data.get("r1"),
            r2=data.get("r2"),
            expiry=data.get("expiry", ""),
            vix=None,
            pcr=data.get("pcr"),
            max_pain=mp,
            max_pain_dist=mpd,
            bias_tag=bias,
            stale=False,
            extras={
                "s1s": s1s, "s2s": s2s, "r1s": r1s, "r2s": r2s,
                "buffer": b,
                "mv": {
                    "ce_ok": mv["ce_ok"], "pe_ok": mv["pe_ok"],
                    "ce_basis": mv["ce_basis"], "pe_basis": mv["pe_basis"],
                    "pcr_hi": p.pcr_bull_high(), "pcr_lo": p.pcr_bear_low(),
                    "mp_dist_need": mp_dist_needed,
                },
            },
        )
        set_snapshot(snap)
        return snap

    except Exception as e:
        log.error(f"OC refresh failed: {e}")
        return None
