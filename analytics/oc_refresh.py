# analytics/oc_refresh.py
import os
from math import inf
from utils.logger import log
from utils.params import Params
from utils.cache import OCSnapshot, set_snapshot, get_snapshot
from integrations import sheets as sh
from integrations.option_chain_dhan import fetch_levels

SYMBOL = os.getenv("OC_SYMBOL", "NIFTY").upper()
MODE = os.getenv("OC_MODE", "sheet").lower()

def _buffer_for_symbol(params: Params) -> int:
    return params.buffer_points()

def _mv_flags_and_basis(spot, pcr, max_pain, mp_dist_needed, p: Params) -> dict:
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

def _nearest_strike(strikes: list[float], level: float | None) -> float | None:
    if level is None or not strikes:
        return None
    return min(strikes, key=lambda s: abs(s - level))

def _sum_window(oi_map: dict[float, dict], center: float, width: int, leg: str) -> int:
    # width = number of strikes on each side
    keys = sorted(oi_map.keys())
    if center not in keys:
        # choose nearest available
        center = _nearest_strike(keys, center)
        if center is None:
            return 0
    idx = keys.index(center)
    lo = max(0, idx - width)
    hi = min(len(keys), idx + width + 1)
    total = 0
    for k in keys[lo:hi]:
        total += int((oi_map.get(k) or {}).get(leg, 0))
    return total

def _delta_window(cur: dict[float, dict], prev: dict[float, dict], center: float, width: int, leg: str) -> int:
    keys = sorted(set(cur.keys()) | set(prev.keys()))
    if center not in keys:
        center = _nearest_strike(keys, center)
        if center is None:
            return 0
    idx = keys.index(center)
    lo = max(0, idx - width)
    hi = min(len(keys), idx + width + 1)
    d = 0
    for k in keys[lo:hi]:
        cv = int((cur.get(k) or {}).get(leg, 0))
        pv = int((prev.get(k) or {}).get(leg, 0))
        d += (cv - pv)
    return d

def _oc_pattern(cur_oi: dict[float, dict],
                prev_oi: dict[float, dict] | None,
                s1s: float | None, s2s: float | None,
                r1s: float | None, r2s: float | None,
                p: Params) -> dict:
    """
    Compute OC-Pattern 1-of-2 near the most relevant trigger per side.
      CE (Bullish):
        - bull_normal:     ΔCE < 0  & ΔPE > 0
        - bull_shortcover: ΔCE < 0  & ΔPE < 0  (stronger)
      PE (Bearish):
        - bear_normal:     ΔCE > 0  & ΔPE > 0
        - bear_crash:      ΔCE > 0  & ΔPE < 0  (stronger)
    Significance: |Δ| must exceed max( abs_min, pct_min * prev_window_sum )
    """
    if not cur_oi or not prev_oi:
        return {
            "ce_ok": False, "pe_ok": False,
            "ce_type": None, "pe_type": None,
            "basis_ce": "prev OI unavailable", "basis_pe": "prev OI unavailable"
        }

    strikes = sorted(cur_oi.keys())
    w = max(0, p.oi_cluster_strikes())
    pct_min = max(0.0, p.oi_delta_pct_min())
    abs_min_ce = max(0, p.oi_delta_min_ce())
    abs_min_pe = max(0, p.oi_delta_min_pe())

    # pick trigger-centric strike per side (prefer crossed, else nearer of two)
    cand_ce = _nearest_strike(strikes, level=min([x for x in [s1s, s2s] if x is not None], default=None))
    cand_pe = _nearest_strike(strikes, level=min([x for x in [r1s, r2s] if x is not None], default=None))

    # If nothing to lock onto, use center by spot-ish window (bigger window)
    if cand_ce is None:
        # fallback: center of strikes
        cand_ce = strikes[len(strikes)//2]
        w = max(w, p.oi_window_min())
    if cand_pe is None:
        cand_pe = strikes[len(strikes)//2]
        w = max(w, p.oi_window_min())

    # compute deltas & prev sums
    d_ce_at_ce = _delta_window(cur_oi, prev_oi, cand_ce, w, "ce")
    d_pe_at_ce = _delta_window(cur_oi, prev_oi, cand_ce, w, "pe")
    prev_ce_sum = _sum_window(prev_oi, cand_ce, w, "ce")
    prev_pe_sum = _sum_window(prev_oi, cand_ce, w, "pe")

    d_ce_at_pe = _delta_window(cur_oi, prev_oi, cand_pe, w, "ce")
    d_pe_at_pe = _delta_window(cur_oi, prev_oi, cand_pe, w, "pe")
    prev_ce_sum_pe = _sum_window(prev_oi, cand_pe, w, "ce")
    prev_pe_sum_pe = _sum_window(prev_oi, cand_pe, w, "pe")

    # significance thresholds
    th_ce = max(abs_min_ce, int(prev_ce_sum * pct_min))
    th_pe = max(abs_min_pe, int(prev_pe_sum * pct_min))
    th_ce_pe = max(abs_min_ce, int(prev_ce_sum_pe * pct_min))
    th_pe_pe = max(abs_min_pe, int(prev_pe_sum_pe * pct_min))

    # CE side classification
    ce_ok = False
    ce_type = None
    if abs(d_ce_at_ce) >= th_ce and abs(d_pe_at_ce) >= th_pe:
        if d_ce_at_ce < 0 and d_pe_at_ce > 0:
            ce_ok, ce_type = True, "bull_normal"
        elif d_ce_at_ce < 0 and d_pe_at_ce < 0:
            ce_ok, ce_type = True, "bull_shortcover"

    # PE side classification
    pe_ok = False
    pe_type = None
    if abs(d_ce_at_pe) >= th_ce_pe and abs(d_pe_at_pe) >= th_pe_pe:
        if d_ce_at_pe > 0 and d_pe_at_pe > 0:
            pe_ok, pe_type = True, "bear_normal"
        elif d_ce_at_pe > 0 and d_pe_at_pe < 0:
            pe_ok, pe_type = True, "bear_crash"

    basis_ce = f"ΔCE={d_ce_at_ce} ΔPE={d_pe_at_ce} (thCE≥{th_ce}, thPE≥{th_pe}) @strike≈{cand_ce} ±{w}"
    basis_pe = f"ΔCE={d_ce_at_pe} ΔPE={d_pe_at_pe} (thCE≥{th_ce_pe}, thPE≥{th_pe_pe}) @strike≈{cand_pe} ±{w}"

    return {
        "ce_ok": ce_ok, "pe_ok": pe_ok,
        "ce_type": ce_type, "pe_type": pe_type,
        "basis_ce": basis_ce, "basis_pe": basis_pe,
        "strike_ce": cand_ce, "strike_pe": cand_pe, "window": w,
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
                "oc_oi": {}, "strike_step": None,
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

        # OC-Pattern (needs previous snapshot OI)
        prev = get_snapshot()
        cur_oi = data.get("oc_oi") or {}
        prev_oi = (prev.extras.get("oc_oi") if (prev and prev.extras) else None)
        ocp = _oc_pattern(cur_oi, prev_oi, s1s, s2s, r1s, r2s, p)

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
                "ocp": ocp,
                "oc_oi": cur_oi,
                "strike_step": data.get("strike_step"),
            },
        )
        set_snapshot(snap)
        return snap

    except Exception as e:
        log.error(f"OC refresh failed: {e}")
        return None
