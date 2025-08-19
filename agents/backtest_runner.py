# agents/backtest_runner.py
"""
Backtest v2 (lightweight):
- Use OC_Live history (last ~60 days)
- When PCR>=bull_hi or MP Δ beyond threshold => assume directional long/short opportunity
- Apply simple RR & 1:2 trail model to simulate P&L
- Write summary to Performance tab
"""
import os, statistics
from utils.logger import log
from utils.params import Params
from integrations import sheets as sh

def run():
    p = Params()
    rows = sh.get_oc_live_history(days=60)
    if not rows:
        log.info("Backtest: no OC_Live history")
        return

    wins = 0; losses = 0; pnls = []
    bull_hi = p.pcr_bull_high()
    bear_lo = p.pcr_bear_low()
    mpd_need = p.mp_support_dist()
    tgt = p.min_target_points()

    last_spot = None
    for r in rows:
        try:
            spot = float(r.get("spot") or 0.0)
            pcr = float(r.get("pcr") or 0.0)
            mpd = float(r.get("max_pain_dist") or 0.0)
        except Exception:
            continue
        if not spot: 
            continue

        go_long = (pcr >= bull_hi) or (mpd >= mpd_need)
        go_short = (pcr <= bear_lo) or (mpd <= -mpd_need)

        if go_long and not go_short:
            # CE sim: entry at spot, SL at (spot - buffer), TP at spot + tgt
            sl = spot - max(5, tgt/2)
            tp = spot + tgt
            # naive outcome: if next spot (or drift) crosses tp first
            if last_spot is not None and last_spot <= tp:
                wins += 1; pnls.append(tgt)
            else:
                losses += 1; pnls.append(-(tgt/2))
        elif go_short and not go_long:
            sl = spot + max(5, tgt/2)
            tp = spot - tgt
            if last_spot is not None and last_spot >= tp:
                wins += 1; pnls.append(tgt)
            else:
                losses += 1; pnls.append(-(tgt/2))

        last_spot = spot

    if pnls:
        wr = wins / max(1, wins + losses) * 100.0
        avg = statistics.mean(pnls)
        # max drawdown (running sum based)
        run_sum = 0.0; peak = 0.0; max_dd = 0.0
        for x in pnls:
            run_sum += x
            peak = max(peak, run_sum)
            max_dd = min(max_dd, run_sum - peak)
        sh.update_performance({
            "win_rate": round(wr, 2),
            "avg_pl": round(avg, 2),
            "drawdown": round(max_dd, 2),
            "version": os.getenv("APP_VERSION", "dev"),
        })
        log.info(f"Backtest v2: WR={wr:.1f}% avg={avg:.2f} dd={max_dd:.2f} (n={len(pnls)})")
    else:
        log.info("Backtest v2: no signals formed")
