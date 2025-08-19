# analytics/oc_refresh.py
from __future__ import annotations
import os
from utils.logger import log
from utils.params import Params
from utils.cache import OCSnapshot, set_snapshot, get_snapshot
from integrations import sheets as sh
from integrations.option_chain_dhan import fetch_levels
from integrations.news_feed import hold_active

SYMBOL = os.getenv("OC_SYMBOL", "NIFTY").upper()
MODE = os.getenv("OC_MODE", "dhan").lower()

def _buffer_for_symbol(params: Params) -> int:
    return params.buffer_points()

def _mv_flags_and_basis(spot, pcr, max_pain, mp_dist_needed, p: Params) -> dict:
    bull_h = p.pcr_bull_high()
    bear_l = p.pcr_bear_low()
    ce_ok = pe_ok = False
    ce_basis = []; pe_basis = []
    if pcr is not None:
        if pcr >= bull_h: ce_ok = True; ce_basis.append(f"PCR {pcr} ≥ {bull_h}")
        if pcr <= bear_l: pe_ok = True; pe_basis.append(f"PCR {pcr} ≤ {bear_l}")
    if spot is not None and max_pain is not None:
        delta = round(spot - max_pain, 2)
        if spot >= max_pain + mp_dist_needed: ce_ok = True; ce_basis.append(f"MP Δ +{delta} ≥ {mp_dist_needed}")
        if spot <= max_pain - mp_dist_needed: pe_ok = True; pe_basis.append(f"MP Δ {delta} ≤ -{mp_dist_needed}")
    return {
        "ce_ok": ce_ok, "pe_ok": pe_ok,
        "ce_basis": " | ".join(ce_basis) if ce_basis else "—",
        "pe_basis": " | ".join(pe_basis) if pe_basis else "—",
        "pcr_hi": bull_h, "pcr_lo": bear_l, "mp_need": mp_dist_needed,
    }

def _nearest_strike(strikes: list[float], level: float | None) -> float | None:
    if level is None or not strikes: return None
    return min(strikes, key=lambda s: abs(s - level))

def _sum_window(oi_map: dict[float, dict], center: float, width: int, leg: str) -> int:
    keys = sorted(oi_map.keys())
    if center not in keys:
        center = _nearest_strike(keys, center) or (keys[len(keys)//2] if keys else None)
    if center is None: return 0
    idx = keys.index(center)
    lo = max(0, idx - width); hi = min(len(keys), idx + width + 1)
    return sum(int((oi_map.get(k) or {}).get(leg, 0)) for k in keys[lo:hi])

def _delta_window(cur: dict[float, dict], prev: dict[float, dict], center: float, width: int, leg: str) -> int:
    keys = sorted(set(cur.keys()) | set(prev.keys()))
    if center not in keys:
        center = _nearest_strike(keys, center) or (keys[len(keys)//2] if keys else None)
    if center is None: return 0
    idx = keys.index(center)
    lo = max(0, idx - width); hi = min(len(keys), idx + width + 1)
    d = 0
    for k in keys[lo:hi]:
        cv = int((cur.get(k) or {}).get(leg, 0))
        pv = int((prev.get(k) or {}).get(leg, 0))
        d += (cv - pv)
    return d

def _oc_pattern(cur_oi: dict[float, dict], prev_oi: dict[float, dict] | None,
                s1s, s2s, r1s, r2s, p: Params) -> dict:
    if not cur_oi or not prev_oi:
        return {"ce_ok": False, "pe_ok": False, "ce_type": None, "pe_type": None,
                "basis_ce": "prev OI unavailable", "basis_pe": "prev OI unavailable"}
    strikes = sorted(cur_oi.keys())
    w = max(0, p.oi_cluster_strikes())
    # choose strike near triggers
    def pick(*levels):
        lev = min([x for x in levels if x is not None], default=None)
        return _nearest_strike(strikes, lev) or (strikes[len(strikes)//2] if strikes else None)
    ce_ctr = pick(s1s, s2s)
    pe_ctr = pick(r1s, r2s)
    prev_ce_sum = _sum_window(prev_oi, ce_ctr, w, "ce")
    prev_pe_sum = _sum_window(prev_oi, ce_ctr, w, "pe")
    prev_ce_sum_pe = _sum_window(prev_oi, pe_ctr, w, "ce")
    prev_pe_sum_pe = _sum_window(prev_oi, pe_ctr, w, "pe")
    d_ce_at_ce = _delta_window(cur_oi, prev_oi, ce_ctr, w, "ce")
    d_pe_at_ce = _delta_window(cur_oi, prev_oi, ce_ctr, w, "pe")
    d_ce_at_pe = _delta_window(cur_oi, prev_oi, pe_ctr, w, "ce")
    d_pe_at_pe = _delta_window(cur_oi, prev_oi, pe_ctr, w, "pe")
    th_ce = max(p.oi_delta_min_ce(), int(prev_ce_sum * p.oi_delta_pct_min()))
    th_pe = max(p.oi_delta_min_pe(), int(prev_pe_sum * p.oi_delta_pct_min()))
    th_ce_pe = max(p.oi_delta_min_ce(), int(prev_ce_sum_pe * p.oi_delta_pct_min()))
    th_pe_pe = max(p.oi_delta_min_pe(), int(prev_pe_sum_pe * p.oi_delta_pct_min()))
    ce_ok = pe_ok = False; ce_type = pe_type = None
    if abs(d_ce_at_ce) >= th_ce and abs(d_pe_at_ce) >= th_pe:
        if d_ce_at_ce < 0 and d_pe_at_ce > 0: ce_ok, ce_type = True, "bull_normal"
        elif d_ce_at_ce < 0 and d_pe_at_ce < 0: ce_ok, ce_type = True, "bull_shortcover"
    if abs(d_ce_at_pe) >= th_ce_pe and abs(d_pe_at_pe) >= th_pe_pe:
        if d_ce_at_pe > 0 and d_pe_at_pe > 0: pe_ok, pe_type = True, "bear_normal"
        elif d_ce_at_pe > 0 and d_pe_at_pe < 0: pe_ok, pe_type = True, "bear_crash"
    return {
        "ce_ok": ce_ok, "pe_ok": pe_ok,
        "ce_type": ce_type, "pe_type": pe_type,
        "basis_ce": f"ΔCE={d_ce_at_ce} ΔPE={d_pe_at_ce} (thCE≥{th_ce}, thPE≥{th_pe}) @≈{ce_ctr} ±{w}",
        "basis_pe": f"ΔCE={d_ce_at_pe} ΔPE={d_pe_at_pe} (thCE≥{th_ce_pe}, thPE≥{th_pe_pe}) @≈{pe_ctr} ±{w}",
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
                "spot": float(row.get("spot") or 0),
                "s1": float(row.get("s1") or 0),
                "s2": float(row.get("s2") or 0),
                "r1": float(row.get("r1") or 0),
                "r2": float(row.get("r2") or 0),
                "pcr": float(row.get("pcr") or 0),
                "max_pain": float(row.get("max_pain") or 0),
                "expiry": row.get("expiry") or "",
                "oc_oi": {}, "strike_step": None,
            }

        b = p.buffer_points()
        s1s = (data["s1"] - b) if data.get("s1") else None
        s2s = (data["s2"] - b) if data.get("s2") else None
        r1s = (data["r1"] + b) if data.get("r1") else None
        r2s = (data["r2"] + b) if data.get("r2") else None

        mp = data.get("max_pain")
        spot = data.get("spot")
        mpd = (spot - mp) if (mp is not None and spot is not None) else None
        mv = _mv_flags_and_basis(spot, data.get("pcr"), mp, p.mp_support_dist(), p)

        prev = get_snapshot()
        cur_oi = data.get("oc_oi") or {}
        prev_oi = (prev.extras.get("oc_oi") if (prev and prev.extras) else None)
        ocp = _oc_pattern(cur_oi, prev_oi, s1s, s2s, r1s, r2s, p)

        # bias tag
        bias = None
        if mp is not None and mpd is not None:
            if spot >= mp + p.mp_support_dist(): bias = "mv_bull_mp"
            elif spot <= mp - p.mp_support_dist(): bias = "mv_bear_mp"

        snap = OCSnapshot(
            spot=spot, s1=data.get("s1"), s2=data.get("s2"), r1=data.get("r1"), r2=data.get("r2"),
            expiry=data.get("expiry", ""), vix=None, pcr=data.get("pcr"),
            max_pain=mp, max_pain_dist=mpd, bias_tag=bias, stale=False,
            extras={
                "s1s": s1s, "s2s": s2s, "r1s": r1s, "r2s": r2s,
                "buffer": b, "mv": mv, "ocp": ocp, "oc_oi": cur_oi,
                "strike_step": data.get("strike_step"),
            },
        )
        set_snapshot(snap)

        # Write OC_Live row (SoR)
        pcr_bucket = "bull" if (data.get("pcr") and data.get("pcr") >= p.pcr_bull_high()) else \
                     ("bear" if (data.get("pcr") is not None and data.get("pcr") <= p.pcr_bear_low()) else "")
        sh.append_row("OC_Live", [
            sh.now_str(), spot, data.get("s1"), data.get("s2"), data.get("r1"), data.get("r2"),
            data.get("expiry"), "", None, data.get("pcr"), pcr_bucket, mp, mpd, bias, False
        ])
        return snap

    except Exception as e:
        log.error(f"OC refresh failed: {e}")
        # Mark stale in OC_Live
        try:
            sh.append_row("OC_Live", [sh.now_str(), None, None, None, None, None, "", "", None, None, "", None, None, "stale", True])
        except Exception:
            pass
        return None
