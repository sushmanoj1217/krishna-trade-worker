# path: ops/commands.py
from datetime import datetime
from tzlocal import get_localzone

from integrations import telegram

ALLOW_MANUAL = False  # hard block manual controls

def _today() -> str:
    return datetime.now(get_localzone()).strftime("%Y-%m-%d")

def _sum_today_pnl(sheet) -> tuple[float, int]:
    pnl = 0.0; n = 0
    try:
        rows = sheet.read_all("Trades") or []
        for r in rows:
            ts = str(r.get("ts_buy",""))
            if ts.startswith(_today()):
                try:
                    v = float(r.get("pnl") or 0)
                    pnl += v; n += 1
                except Exception:
                    pass
    except Exception:
        pass
    return pnl, n

def _last_levels(state) -> str:
    lv = state.last_levels or {}
    if not lv: return "no levels yet"
    s = lv.get("symbol","")
    return f"{s} spot={lv.get('spot')} s1={lv.get('s1')} r1={lv.get('r1')}"

def handle(text: str, sender_id: str, cfg, state, sheet):
    t = (text or "").strip().lower()
    if not t.startswith("/"):
        # Manual overrides rejected (audit)
        if ALLOW_MANUAL:
            telegram.send("Manual commands currently disabled by policy.")
        return

    if t in ("/start", "/help"):
        telegram.send("Commands: /status, /last, /perf_today")
        return

    if t == "/status":
        pnl, n = _sum_today_pnl(sheet)
        msg = (f"Status {cfg.symbol}\n"
               f"OpenTrades={len(state.open_trades)}\n"
               f"Levels: { _last_levels(state) }\n"
               f"Today Trades Logged={n}, PnL={pnl:.2f}")
        telegram.send(msg); return

    if t == "/last":
        telegram.send(f"Last Levels -> { _last_levels(state) }"); return

    if t == "/perf_today":
        try:
            rows = sheet.read_all("Performance") or []
            today = _today()
            row = None
            for r in reversed(rows):
                if str(r.get("date","")).strip() == today:
                    row = r; break
            if row:
                msg = (f"Performance {today}\n"
                       f"win_rate={row.get('win_rate')} avg_pnl={row.get('avg_pnl')} "
                       f"total_trades={row.get('total_trades')} daily_pnl={row.get('daily_pnl')}")
            else:
                pnl, n = _sum_today_pnl(sheet)
                msg = f"Performance {today}: trades={n}, pnl={pnl:.2f}"
            telegram.send(msg)
        except Exception:
            pnl, n = _sum_today_pnl(sheet)
            telegram.send(f"Performance { _today() }: trades={n}, pnl={pnl:.2f}")
        return

    # Unknown
    telegram.send("Unknown command. Try /help")
