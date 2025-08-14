# path: ops/commands.py
from datetime import datetime
from tzlocal import get_localzone
from integrations import telegram

ALLOW_MANUAL = False

def _today(): return datetime.now(get_localzone()).strftime("%Y-%m-%d")

def _sum_today_pnl(sheet) -> tuple[float, int]:
    pnl = 0.0; n = 0
    try:
        rows = sheet.read_all("Trades") or []
        for r in rows:
            ts = str(r.get("ts_buy",""))
            if ts.startswith(_today()):
                try: pnl += float(r.get("pnl") or 0); n += 1
                except: pass
    except Exception: pass
    return pnl, n

def _last_levels(state) -> str:
    lv = state.last_levels or {}
    if not lv: return "no levels yet"
    s = lv.get("symbol","")
    return f"{s} spot={lv.get('spot')} s1={lv.get('s1')} r1={lv.get('r1')}"

def _events_today(sheet):
    try:
        rows = sheet.read_all("Events") or []
    except Exception:
        return "no events (read error)"
    day = _today()
    out = []
    for r in rows:
        if str(r.get("date","")).strip() == day:
            out.append(f"{r.get('time_ist','')} {r.get('name','')} [{r.get('severity','')}] {r.get('effect','')}"
                       f" {r.get('window_start','')}-{r.get('window_end','')}")
    return "\n".join(out) if out else "no events today"

def handle(text: str, sender_id: str, cfg, state, sheet):
    t = (text or "").strip().lower()
    if not t.startswith("/"):
        if ALLOW_MANUAL:
            telegram.send("Manual commands disabled.")
        return

    if t in ("/start","/help"):
        telegram.send("Commands: /status, /last, /perf_today, /open_trades, /health, /events_today"); return

    if t == "/status":
        pnl, n = _sum_today_pnl(sheet)
        telegram.send(f"Status {cfg.symbol}\nOpenTrades={len(state.open_trades)}\nLevels: {_last_levels(state)}\nToday Trades={n}, PnL={pnl:.2f}"); return

    if t == "/last":
        telegram.send(f"Last Levels -> {_last_levels(state)}"); return

    if t == "/perf_today":
        try:
            rows = sheet.read_all("Performance") or []
            row = None
            for r in reversed(rows):
                if str(r.get("date","")).strip() == _today():
                    row = r; break
            if row:
                telegram.send(f"Performance {_today()}\nwin_rate={row.get('win_rate')} avg_pnl={row.get('avg_pnl')} total_trades={row.get('total_trades')} daily_pnl={row.get('daily_pnl')}")
            else:
                pnl, n = _sum_today_pnl(sheet)
                telegram.send(f"Performance {_today()}: trades={n}, pnl={pnl:.2f}")
        except Exception:
            pnl, n = _sum_today_pnl(sheet)
            telegram.send(f"Performance {_today()}: trades={n}, pnl={pnl:.2f}")
        return

    if t == "/open_trades":
        if not state.open_trades:
            telegram.send("No open trades"); return
        lines = []
        for tid, tr in state.open_trades.items():
            lines.append(f"{tid[-6:]} {tr['symbol']} {tr['side']} qty={tr['qty']} @ {tr['buy_ltp']} SL={tr['sl']} TP={tr['tp']}")
        telegram.send("Open Trades:\n" + "\n".join(lines[:20])); return

    if t == "/health":
        telegram.send("Health OK: schedulers running, OC & sheet active."); return

    if t == "/events_today":
        telegram.send(_events_today(sheet)); return

    telegram.send("Unknown command. Try /help")
