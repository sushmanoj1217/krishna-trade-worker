
import os
from core.state import AppState

def can_take_trade(side, ltp, cfg, params, state: AppState) -> bool:
    if state.daily_trades >= cfg.max_trades_per_day:
        print("[risk] blocked: max trades reached")
        return False
    if state.daily_pnl <= -cfg.daily_loss_limit:
        print("[risk] blocked: daily loss limit hit")
        return False
    return ltp is not None and ltp > 0

def compute_qty(ltp: float) -> int:
    qty = int(os.getenv("QTY_PER_TRADE", "0") or "0")
    if qty > 0:
        return qty
    max_expo = int(os.getenv("MAX_EXPOSURE_PER_TRADE", "3000") or "3000")
    if ltp <= 0:
        return 0
    q = max_expo // int(ltp if ltp>0 else 1)
    return max(1, int(q))

def bump_daily(state: AppState, pnl: float):
    state.daily_pnl += pnl
    state.daily_trades += 1
