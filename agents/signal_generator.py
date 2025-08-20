# agents/signal_generator.py
from agents.signal_emit import emit_signal_row_from_dict, emit_signal_row_from_list
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple, List
import time, os

from utils.logger import log
from utils.params import Params
from utils.cache import get_snapshot
from utils.rr import rr_feasible
from utils.time_windows import is_no_trade_now
from integrations.news_feed import hold_active
from integrations import sheets as sh
from utils.ids import signal_id
from utils.state import set_last_signal, approvals_required, queue_for_approval

QTY_PER_TRADE = int(os.getenv("QTY_PER_TRADE", "15"))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "10"))
MAX_EXPOSURE_PER_TRADE = float(os.getenv("MAX_EXPOSURE_PER_TRADE", "3000"))

@dataclass
class Signal:
    id: str
    side: str
    trigger: str
    eligible: bool
    reason: str
    basis: Dict[str, Any]
    dedupe_hash: str

_seen_ts: Dict[str, float] = {}
_level_once: Dict[str, bool] = {}   # per-day one-attempt-per-level
_COOLDOWN_SEC = 300.0

def _today_key() -> str:
    return time.strftime("%Y%m%d")

def _level_key(side: str, trig: str, lvl: float) -> str:
    return f"{_today_key()}|{side}|{trig}|{round(lvl)}"

def _sig_hash(side: str, trig: str, lvl: float) -> str:
    return f"{side}:{trig}:{round(lvl)}"

def _dedupe_hash(side: str, trig: str, lvl: float) -> str:
    return f"{_today_key()}:{side}:{trig}:{round(lvl)}"

def _crossed(tag: str, spot: Optional[float], lvl: Optional[float]) -> Tuple[bool, str]:
    if spot is None or lvl is None: return False, "no_level"
    if tag in ("S1*", "S2*"):
        return (spot <= lvl), ("CROSS" if spot <= lvl else ("NEAR" if (lvl - spot) <= 6 else "FAR"))
    else:
        return (spot >= lvl), ("CROSS" if spot >= lvl else ("NEAR" if (spot - lvl) <= 6 else "FAR"))

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
    prev_oi = (snap.extras or {}).get("oc_oi_prev", None)  # may be absent

    now = time.time()
    today_dups = sh.get_today_signal_dedupes()

    for side, trig, lvl in candidates:
        crossed, nearfar = _crossed(trig, snap.spot, lvl)
        if not crossed:
            continue

        lkey = _level_key(side, trig, lvl)
        if _level_once.get(lkey, False):
            log.info(f"Level attempt guard: already tried {lkey}")
            continue

        sig_hash = _sig_hash(side, trig, lvl)
        if sig_hash in _seen_ts and now - _seen_ts[sig_hash] < _COOLDOWN_SEC:
            log.info(f"Duplicate signal blocked {sig_hash}")
            continue

        d_hash = _dedupe_hash(side, trig, lvl)
        if d_hash in today_dups:
            log.info(f"Persistent dedupe blocked {d_hash}")
            continue

        buf = int(snap.extras.get("buffer", p.buffer_points()))
        sl = lvl - buf if side == "CE" else lvl + buf
        rr_ok, risk, tp = rr_feasible(lvl, sl, p.min_target_points())

        approx_premium = max(5.0, min(300.0, abs((snap.spot or lvl) - lvl)))
        exposure = approx_premium * QTY_PER_TRADE

        c1 = True; reason1 = f"TriggerCross {nearfar}"
        bull = (snap.bias_tag or "").startswith("mv_bull")
        bear = (snap.bias_tag or "").startswith("mv_bear")
        c2 = (bull if side == "CE" else bear); reason2 = f"FlowBias {'bull' if bull else ('bear' if bear else 'flat')}"
        # keep placeholders here; walls/momentum are evaluated in oc_refresh OC-pattern/mv blocks already
        c3, reason3 = True, "WallSupport via OC-pattern"
        c4, reason4 = True, "Momentum via MV"
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
            dedupe_hash=d_hash
        )

        try:
            sh.log_signal_row([
                s.id, time.strftime("%Y-%m-%d %H:%M:%S"),
                s.side, s.trigger,
                str(c1), str(c2), str(c3), str(c4), str(c5), str(c6),
                str(s.eligible), s.reason,
                mv_ok, mv_basis, oc_ok, oc_basis, s.basis["nearfar"], "",
                s.dedupe_hash
            ])
        except Exception as e:
            log.error(f"Signals append failed: {e}")

        if approvals_required():
            queue_for_approval({
                "id": s.id, "side": s.side, "trigger": s.trigger,
                "entry": lvl, "sl": sl, "tp": tp, "eligible": s.eligible,
                "dedupe_hash": s.dedupe_hash
            })
            log.info(f"Signal {s.id} queued for approval {s.side} {s.trigger}")
        else:
            set_last_signal({
                "id": s.id, "side": s.side, "trigger": s.trigger,
                "entry": lvl, "sl": sl, "tp": tp, "eligible": s.eligible,
                "dedupe_hash": s.dedupe_hash
            })
            log.info(f"Signal {s.id} pipe → trade: {s.side} {s.trigger} eligible={s.eligible}")

        _seen_ts[sig_hash] = now
        _level_once[lkey] = True
        return s
    return None
