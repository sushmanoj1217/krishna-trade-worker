import os, asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from utils.logger import log
from utils.cache import get_snapshot
from integrations import sheets as sh

IST = ZoneInfo("Asia/Kolkata")
TRAIL_R_MULT = float(os.getenv("TRAIL_R_MULT", "2.0"))  # trail after 1:2
TRAIL_LOCK_R = float(os.getenv("TRAIL_LOCK_R", "1.0"))  # lock 1R on trail

async def trail_tick():
    """Manage SL/TP/flip exits on open trades."""
    snap = get_snapshot()
    if not snap:
        return
    mark = float(snap.spot or 0.0)
    mv = (snap.extras or {}).get("mv", {}) if snap.extras else {}
    ce_ok, pe_ok = bool(mv.get("ce_ok")), bool(mv.get("pe_ok"))

    for t in sh.get_open_trades():
        side = t["side"]
        buy = float(t["buy_ltp"])
        sl = float(t["sl"])
        tp = float(t["tp"])
        r = abs(buy - sl) if side == "CE" else abs(sl - buy)
        # paper premium proxy:
        price = mark  # using spot as proxy for simplicity

        # TP/SL checks
        hit_tp = (price >= tp) if side == "CE" else (price <= tp)
        hit_sl = (price <= sl) if side == "CE" else (price >= sl)

        if hit_tp:
            pnl = (tp - buy) if side == "CE" else (buy - tp)
            sh.close_trade(t["trade_id"], exit_ltp=tp, result="tp", pnl=round(pnl, 2))
            log.info(f"TP EXIT {t['trade_id']} @ {tp} pnl={pnl}")
            continue
        if hit_sl:
            pnl = (sl - buy) if side == "CE" else (buy - sl)
            sh.close_trade(t["trade_id"], exit_ltp=sl, result="sl", pnl=round(pnl, 2))
            log.info(f"SL EXIT {t['trade_id']} @ {sl} pnl={pnl}")
            continue

        # Trailing from 1:2
        move_trail = False
        if side == "CE":
            if (price - buy) >= TRAIL_R_MULT * r:
                new_sl = max(sl, buy + TRAIL_LOCK_R * r)
                if new_sl > sl:
                    sl = new_sl; move_trail = True
        else:
            if (buy - price) >= TRAIL_R_MULT * r:
                new_sl = min(sl, buy - TRAIL_LOCK_R * r)
                if new_sl < sl:
                    sl = new_sl; move_trail = True
        if move_trail:
            sh.update_trade_sl(t["trade_id"], sl)
            log.info(f"TRAIL {t['trade_id']} SL→{sl} (after {TRAIL_R_MULT}R)")

        # MV reversal exit (flip)
        if side == "CE" and pe_ok:
            pnl = (price - buy)
            sh.close_trade(t["trade_id"], exit_ltp=price, result="mv_flip", pnl=round(pnl, 2))
            log.info(f"MV FLIP EXIT {t['trade_id']} CE→PE @ {price} pnl={pnl}")
            continue
        if side == "PE" and ce_ok:
            pnl = (buy - price)
            sh.close_trade(t["trade_id"], exit_ltp=price, result="mv_flip", pnl=round(pnl, 2))
            log.info(f"MV FLIP EXIT {t['trade_id']} PE→CE @ {price} pnl={pnl}")
            continue
