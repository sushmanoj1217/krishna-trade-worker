"""
2-month batch backtest (placeholder):
- Reads historical OC_Live (last ~2 months rows)
- Simulates trigger crosses & MV/OC gates using recorded fields
- Writes summary row to Performance tab (version from ENV).
"""
import os, statistics
from utils.logger import log
from integrations import sheets as sh

def run():
    rows = sh.get_oc_live_history(days=60)
    if not rows:
        log.info("Backtest: no OC_Live history")
        return
    # naive sim: count NEAR/CROSS vs PCR/MP bands to estimate opportunities
    wins = 0; losses = 0; pnls = []
    for r in rows:
        pcr = float(r.get("pcr") or 0)
        mpd = float(r.get("max_pain_dist") or 0)
        # toy rule
        if pcr >= 1.1 or mpd >= 25:
            wins += 1; pnls.append(15)
        elif pcr <= 0.9 or mpd <= -25:
            wins += 1; pnls.append(15)
        else:
            losses += 1; pnls.append(-10)
    wr = wins / max(1, (wins+losses))
    avg = statistics.mean(pnls) if pnls else 0.0
    dd = min(0, min([sum(pnls[:i]) for i in range(1, len(pnls)+1)])) if pnls else 0.0
    sh.update_performance({
        "win_rate": round(wr*100, 2),
        "avg_pl": round(avg, 2),
        "drawdown": round(dd, 2),
        "version": os.getenv("APP_VERSION", "dev"),
    })
    log.info(f"Backtest: WR={wr*100:.1f}% avg={avg} dd={dd}")
