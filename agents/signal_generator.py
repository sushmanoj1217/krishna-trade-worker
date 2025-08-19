# agents/signal_generator.py
from dataclasses import dataclass
from typing import Dict, Any
import time
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

_seen: Dict[str, float] = {}
_DUP_LOG_COOLDOWN = 300.0  # seconds

def _hash(side: str, trigger: str, price: float) -> str:
    return f"{side}:{trigger}:{round(price)}"

def run_once() -> Signal | None:
    snap = get_snapshot()
    if not snap:
        log.debug("No OC snapshot yet")
        return None
    p = Params()

    def crossed(tag: str, level: float | None):
        if not level:
            return False
        return snap.spot <= level if tag in ("S1*", "S2*") else snap.spot >= level

    s1s, s2s = snap.extras.get("s1s"), snap.extras.get("s2s")
    r1s, r2s = snap.extras.get("r1s"), snap.extras.get("r2s")
    candidates = []
    if s1s: candidates.append(("CE", "S1*", s1s))
    if s2s: candidates.append(("CE", "S2*", s2s))
    if r1s: candidates.append(("PE", "R1*", r1s))
    if r2s: candidates.append(("PE", "R2*", r2s))

    mv = (snap.extras or {}).get("mv", {})  # {ce_ok, pe_ok, ce_basis, pe_basis, ...}

    now = time.time()
    for side, trig, lvl in candidates:
        if not crossed(trig, lvl):
            continue

        sig_hash = _hash(side, trig, lvl)
        if sig_hash in _seen and now - _seen[sig_hash] < _DUP_LOG_COOLDOWN:
            continue

        # --- 6-Checks (simplified placeholders still ok) ---
        c1 = True  # TriggerCross already satisfied
        c2 = True  # FlowBias@Trigger (placeholder; MV gate handles macro bias)
        c3 = True  # WallSupport(ΣΔOI) TODO in OC-pattern task
        c4 = True  # Momentum(3–5m)  TODO
        # RR feasibility
        sl = lvl - snap.extras.get("buffer", 12) if side == "CE" else lvl + snap.extras.get("buffer", 12)
        rr_ok, risk, tp = rr_feasible(lvl, sl, p.min_target_points())
        c5 = rr_ok
        c6 = not is_no_trade_now()

        six_ok = all([c1, c2, c3, c4, c5, c6])

        # --- MV 1-of-2 (directional) ---
        if side == "CE":
            mv_ok = bool(mv.get("ce_ok"))
            mv_basis = mv.get("ce_basis", "—")
        else:
            mv_ok = bool(mv.get("pe_ok"))
            mv_basis = mv.get("pe_basis", "—")

        eligible = six_ok and mv_ok

        s = Signal(
            id=signal_id(),
            side=side,
            trigger=trig,
            eligible=eligible,
            reason=f"6/6={six_ok}; MV={mv_ok}",
            basis={"entry": lvl, "sl": sl, "tp": tp, "risk": risk, "mv_basis": mv_basis},
        )

        log.info(f"Signal {s.id} {s.side} {s.trigger} eligible={s.eligible} "
                 f"entry={lvl} sl={sl} tp={tp} | MV: {mv_basis}")

        try:
            sh.append_row("Signals", [
                s.id, time.strftime("%Y-%m-%d %H:%M:%S"),
                s.side, s.trigger,
                True, True, True, True, c5, c6,  # C1..C6 flags (placeholders for C1..C4=True)
                s.eligible, s.reason,
                mv_ok, mv_basis
            ])
        except Exception as e:
            log.error(f"Signals append failed: {e}")

        _seen[sig_hash] = now
        return s

    return None
