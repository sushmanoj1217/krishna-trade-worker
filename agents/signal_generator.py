# agents/signal_generator.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple, List
import time, os
from statistics import linear_regression

from utils.logger import log
from utils.params import Params
from utils.cache import get_snapshot
from utils.rr import rr_feasible
from utils.time_windows import is_no_trade_now
from integrations.news_feed import hold_active
from integrations import sheets as sh
from utils.ids import signal_id
from utils.state import set_last_signal

QTY_PER_TRADE = int(os.getenv("QTY_PER_TRADE", "15"))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "10"))
MAX_EXPOSURE_PER_TRADE = float(os.getenv("MAX_EXPOSURE_PER_TRADE", "3000"))

@dataclass
class Signal:
    id: str
    side: str      # CE|PE
    trigger: str   # S1*|S2*|R1*|R2*
    eligible: bool
    reason: str
    basis: Dict[str, Any]

_seen_ts: Dict[str, float] = {}
_level_once: Dict[str, bool] = {}   # per-day one-attempt-per-level
_COOLDOWN_SEC = 300.0

def _today_key() -> str:
    return time.strftime("%Y%m%d")

def _level_key(side: str, trig: str, lvl: float) -> str:
    return f"{_today_key()}|{side}|{trig}|{round(lvl)}"

def _sig_hash(side: str, trig: str, lvl: float) -> str:
    return f"{side}:{trig}:{round(lvl)}"

def _crossed(tag: str, spot: Optional[float], lvl: Optional[float]) -> Tuple[bool, str]:
    if spot is None or lvl is None: return False, "no_level"
    if tag in ("S1*", "S2*"):
        return (spot <= lvl), ("CROSS" if spot <= lvl else ("NEAR" if (lvl - spot) <= 6 else "FAR"))
    else:
        return (spot >= lvl), ("CROSS" if spot >= lvl else ("NEAR" if (spot - lvl) <= 6 else "FAR"))

def _momentum_ok(side: str, minutes: int = 5) -> Tuple[bool, str]:
    """
    Momentum check over last 3–5 min: slope of spot via simple linear regression.
    For CE: slope > 0 (up), for PE: slope < 0 (down). Allow tiny tolerance.
    """
    rows = sh.get_oc_live_last_minutes(minutes=max(3, minutes))
    if len(rows) < 3:
        return True, "insufficient data"
    xs = list(range(len(rows)))
    try:
        ys = [float(r.get("spot") or 0.0) for r in rows]
        # simple regression: slope = sum((x-x̄)(y-ȳ))/sum((x-x̄)^2)
        xbar = sum(xs)/len(xs); ybar = sum(ys)/len(ys)
        num = sum((x - xbar)*(y - ybar) for x,y in zip(xs,ys))
        den = sum((x - xbar)**2 for x in xs) or 1.0
        slope = num / den
    except Exception:
        slope = ys[-1] - ys[0]
    tol = 0.0
    if side == "CE":
        return (slope > tol), f"slope={slope:.2f}"
    else:
        return (slope < -tol), f"slope={slope:.2f}"

def _wall_support_ok(side: str, center: float, cur_oi: Dict[float, Dict[str,int]], prev_oi: Optional[Dict[float, Dict[str,int]]], p: Params) -> Tuple[bool, str]:
    """
    ΣΔOI around trigger ±w strikes. For CE near supports: want PE OI adding and/or CE OI cutting.
    For PE near resistances: want CE OI adding and/or PE OI cutting.
    """
    if not cur_oi or not prev_oi:
        return True, "no prev OI"
    strikes = sorted(set(cur_oi.keys()) | set(prev_oi.keys()))
    if not strikes: 
        return True, "no strikes"
    # find nearest strike to center
    center = min(strikes, key=lambda s: abs(s - center))
    w = max(0, p.oi_cluster_strikes())
    idx = strikes.index(center)
    lo = max(0, idx - w); hi = min(len(strikes), idx + w + 1)
    d_ce = 0; d_pe = 0
    prev_ce_sum = 0; prev_pe_sum = 0
    for k in strikes[lo:hi]:
        cv = cur_oi.get(k) or {}
        pv = prev_oi.get(k) or {}
        d_ce += int(cv.get("ce",0)) - int(pv.get("ce",0))
        d_pe += int(cv.get("pe",0)) - int(pv.get("pe",0))
        prev_ce_sum += int(pv.get("ce",0))
        prev_pe_sum += int(pv.get("pe",0))
    th_ce = max(p.oi_delta_min_ce(), int(prev_ce_sum * p.oi_delta_pct_min()))
    th_pe = max(p.oi_delta_min_pe(), int(prev_pe_sum * p.oi_delta_pct_min()))
    if side == "CE":
        ok = ((d_pe >= th_pe) or (-d_ce >= th_ce))
        basis = f"ΣΔPE={d_pe} (th≥{th_pe}) ; ΣΔCE={d_ce} (need cut ≥{th_ce})"
    else:
        ok = ((d_ce >= th_ce) or (-d_pe >= th_pe))
        basis = f"ΣΔCE={d_ce} (th≥{th_ce}) ; ΣΔPE={d_pe} (need cut ≥{th_pe})"
    return ok, basis

def run_once() -> Optional[Signal]:
    snap = get_snapshot()
    if not snap:
        return None
    p = Params()

    s1s, s2s = snap.extras.get("s1s"), snap.extras.get("s2s")
    r1s, r2s = snap.extras.get("r1s"), snap.extras.get("r2s")
    candidates: List[Tuple[str,str,float]] = []
    if s1s: candidates.append(("CE", "S1*", s1s))
    if s2s: candidates.append(("CE", "S2*", s2s))
    if r1s: candidates.append(("PE", "R1*", r1s))
    if r2s: candidates.append(("PE", "R2*", r2s))

    mv = (snap.extras or {}).get("mv", {})
    ocp = (snap.extras or {}).get("ocp", {})
    cur_oi = (snap.extras or {}).get("oc_oi", {})
    prev = get_snapshot()
    prev_oi = (prev.extras.get("oc_oi") if (prev and prev.extras) else None)

    now = time.time()
    for side, trig, lvl in candidates:
        crossed, nearfar = _crossed(trig, snap.spot, lvl)
        if not crossed:
            continue

        # one-attempt-per-level (per day)
        lkey = _level_key(side, trig, lvl)
        if _level_once.get(lkey, False):
            log.info(f"Level attempt guard: already tried {lkey}")
            continue

        sig_hash = _sig_hash(side, trig, lvl)
        if sig_hash in _seen_ts and now - _seen_ts[sig_hash] < _COOLDOWN_SEC:
            log.info(f"Duplicate signal blocked {sig_hash}")
            continue

        buf = int(snap.extras.get("buffer", p.buffer_points()))
        sl = lvl - buf if side == "CE" else lvl + buf
        rr_ok, risk, tp = rr_feasible(lvl, sl, p.min_target_points())

        # exposure proxy
        approx_premium = max(5.0, min(300.0, abs((snap.spot or lvl) - lvl)))
        exposure = approx_premium * QTY_PER_TRADE

        # C1..C6
        c1 = True; reason1 = f"TriggerCross {nearfar}"
        bull = (snap.bias_tag or "").startswith("mv_bull")
        bear = (snap.bias_tag or "").startswith("mv_bear")
        c2 = (bull if side == "CE" else bear); reason2 = f"FlowBias {'bull' if bull else ('bear' if bear else 'flat')}"
        c3, reason3 = _wall_support_ok(side, lvl, cur_oi, prev_oi, p)   # real
        c4, reason4 = _momentum_ok(side, minutes=5)                     # real
        c5 = rr_ok; reason5 = f"RR feasible risk={round(risk,2)} tp={round(tp,2)}"
        hold_on, hold_reason = hold_active()
        caps_ok = (sh.count_today_trades() < MAX_TRADES_PER_DAY) and (exposure <= MAX_EXPOSURE_PER_TRADE)
        sys_bits = []
        if is_no_trade_now(): sys_bits.append("NoTradeWindow")
        if hold_on: sys_bits.append(hold_reason or "HOLD")
        if sh.count_today_trades() >= MAX_TRADES_PER_DAY: sys_bits.append("DayCap")
        if exposure > MAX_EXPOSURE_PER_TRADE: sys_bits.append("ExposureCap")
        c6 = (not is_no_trade_now()) and (not hold_on) and caps_ok
        reason6 = "SystemGates " + (",".join(sys_bits) if sys_bits else "OK")

        # MV & OC-pattern gates
        if side == "CE":
            mv_ok = bool(mv.get("ce_ok")); mv_basis = mv.get("ce_basis", "—")
            oc_ok = bool(ocp.get("ce_ok")); oc_basis = f"{ocp.get('ce_type','-')}; {ocp.get('basis_ce','—')}"
        else:
            mv_ok = bool(mv.get("pe_ok")); mv_basis = mv.get("pe_basis", "—")
            oc_ok = bool(ocp.get("pe_ok")); oc_basis = f"{ocp.get('pe_type','-')}; {ocp.get('basis_pe','—')}"

        six_ok = all([c1,c2,c3,c4,c5,c6])
        eligible = six_ok and mv_ok and oc_ok

        s = Signal(
            id=signal_id(), side=side, trigger=trig, eligible=eligible,
            reason=f"C1..C6={six_ok}; MV={mv_ok}; OC={oc_ok}",
            basis={
                "entry": lvl, "sl": sl, "tp": tp, "risk": risk,
                "c_reasons": [reason1, reason2, reason3, reason4, reason5, reason6],
                "mv_basis": mv_basis, "oc_basis": oc_basis,
                "nearfar": nearfar
            },
        )

        # Log (Signals)
        try:
            sh.log_signal_row([
                s.id, time.strftime("%Y-%m-%d %H:%M:%S"),
                s.side, s.trigger,
                str(c1), str(c2), str(c3), str(c4), str(c5), str(c6),
                str(s.eligible), s.reason,
                mv_ok, mv_basis, oc_ok, oc_basis, s.basis["nearfar"], ""
            ])
        except Exception as e:
            log.error(f"Signals append failed: {e}")

        # Pipe to trade loop
        set_last_signal({
            "id": s.id, "side": s.side, "trigger": s.trigger,
            "entry": lvl, "sl": sl, "tp": tp, "eligible": s.eligible
        })

        log.info(f"Signal {s.id} {s.side} {s.trigger} eligible={s.eligible} "
                 f"entry={lvl} sl={sl} tp={tp} | MV: {mv_basis} | OC: {oc_basis} | {reason6}")

        _seen_ts[sig_hash] = now
        _level_once[lkey] = True
        return s

    return None
