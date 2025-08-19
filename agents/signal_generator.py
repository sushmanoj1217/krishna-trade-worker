from dataclasses import dataclass
from typing import Dict, Any
from utils.logger import log
from utils.params import Params
from utils.cache import get_snapshot
from utils.rr import rr_feasible
from utils.time_windows import is_no_trade_now
from integrations import sheets as sh
from utils.ids import signal_id

@dataclass
class Signal:
    id: str
    side: str  # CE|PE
    trigger: str  # S1*|S2*|R1*|R2*
    eligible: bool
    reason: str
    basis: Dict[str, Any]

_seen_hashes = set()

def _hash(side: str, trigger: str, price: float) -> str:
    return f"{side}:{trigger}:{round(price)}"

def run_once() -> Signal | None:
    snap = get_snapshot()
    if not snap:
        log.info("No OC snapshot yet")
        return None
    p = Params()

    def crossed(tag: str, level: float | None):
        if not level:
            return False
        if tag in ("S1*", "S2*"):
            return snap.spot <= level  # support cross
        else:
            return snap.spot >= level  # resistance cross

    s1s, s2s = snap.extras.get("s1s"), snap.extras.get("s2s")
    r1s, r2s = snap.extras.get("r1s"), snap.extras.get("r2s")

    candidates = []
    if s1s: candidates.append(("CE", "S1*", s1s))
    if s2s: candidates.append(("CE", "S2*", s2s))
    if r1s: candidates.append(("PE", "R1*", r1s))
    if r2s: candidates.append(("PE", "R2*", r2s))

    for side, trig, lvl in candidates:
        if not crossed(trig, lvl):
            continue

        c1 = True
        c2 = (side == "CE" and (snap.bias_tag or "").startswith("mv_bull")) or \
             (side == "PE" and (snap.bias_tag or "").startswith("mv_bear"))
        c3 = True if lvl else False
        c4 = True  # momentum placeholder
        sl = lvl - snap.extras.get("buffer", 12) if side == "CE" else lvl + snap.extras.get("buffer", 12)
        rr_ok, risk, tp = rr_feasible(lvl, sl, p.min_target_points())
        c5 = rr_ok
        c6 = not is_no_trade_now()

        all_ok = all([c1, c2, c3, c4, c5, c6])

        sig_hash = _hash(side, trig, lvl)
        if sig_hash in _seen_hashes:
            log.info(f"Duplicate signal blocked {sig_hash}")
            continue

        s = Signal(
            id=signal_id(), side=side, trigger=trig, eligible=all_ok,
            reason=";".join([f"C1={c1}", f"C2={c2}", f"C3={c3}", f"C4={c4}", f"C5={c5}", f"C6={c6}"]),
            basis={"entry": lvl, "sl": sl, "tp": tp, "risk": risk}
        )

        try:
            sh.append_row("Signals", [s.id, s.side, s.trigger, lvl, c1, c2, c3, c4, c5, c6, s.eligible, s.reason])
        except Exception as e:
            log.error(f"Signals append failed: {e}")

        _seen_hashes.add(sig_hash)
        return s
    return None
