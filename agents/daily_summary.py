# path: agents/daily_summary.py
from datetime import datetime
from tzlocal import get_localzone

from integrations import telegram

def _today() -> str:
    return datetime.now(get_localzone()).strftime("%Y-%m-%d")

def compute_today(sheet):
    # Prefer Performance tab; fallback to Trades aggregation
    day = _today()
    row = None
    try:
        perfs = sheet.read_all("Performance") or []
        for r in reversed(perfs):
            if str(r.get("date","")).strip() == day:
                row = r; break
    except Exception:
        row = None

    if row:
        return {
            "date": day,
            "win_rate": row.get("win_rate"),
            "avg_pnl": row.get("avg_pnl"),
            "total_trades": row.get("total_trades"),
            "daily_pnl": row.get("daily_pnl"),
            "notes": row.get("notes",""),
        }

    # fallback from Trades
    wins = 0; losses = 0; pnl = 0.0; n = 0
    try:
        trs = sheet.read_all("Trades") or []
        for r in trs:
            ts = str(r.get("ts_buy",""))
            if ts.startswith(day):
                n += 1
                try:
                    v = float(r.get("pnl") or 0); pnl += v
                    if v >= 0: wins += 1
                    else: losses += 1
                except Exception:
                    pass
    except Exception:
        pass
    win_rate = f"{(wins*100/max(1, n)):.1f}%" if n else "0%"
    avg = f"{(pnl/max(1,n)):.2f}"
    return {"date": day, "win_rate": win_rate, "avg_pnl": avg, "total_trades": n, "daily_pnl": f"{pnl:.2f}", "notes": ""}

def push_telegram(sheet, cfg):
    s = compute_today(sheet)
    msg = (f"Daily Summary ({s['date']}) [{cfg.symbol}]\n"
           f"Trades={s['total_trades']}  Win%={s['win_rate']}\n"
           f"AvgPnL={s['avg_pnl']}  DayPnL={s['daily_pnl']}\n"
           f"{s.get('notes','')}")
    telegram.send(msg.strip())
