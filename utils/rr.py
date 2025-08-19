from typing import Tuple

def rr_feasible(entry: float, stop: float, min_target_points: float) -> Tuple[bool, float, float]:
    risk = abs(entry - stop)
    tp = entry + 2 * risk if entry > stop else entry - 2 * risk
    rr_ok = risk > 0 and abs(tp - entry) >= max(min_target_points, 1)
    return rr_ok, risk, tp
