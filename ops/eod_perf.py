# ops/eod_perf.py
# EOD Performance writer (offline-safe)
# - Trades tab read karke aaj ka summary banata hai
# - Performance tab me ek row append karta hai
# - (optional) Telegram par daily summary bhejta hai
#
# Headers we assume (consistent with logger.ensure_all_headers):
# Trades:
#   ["ts_open","symbol","side","qty","entry","sl","tp","ts_close","exit_price","pnl","reason_open","reason_close","trade_id"]
# Performance:
#   ["date","symbol","trades","wins","losses","win_rate","avg_pnl","gross_pnl","net_pnl","max_dd","version","notes"]

from __future__ import annotations
import os, json, math, statistics, requests
from datetime import datetime
from typing import Dict, Any, List, Optional

from agents import logger  # uses logger.log_performance if available

IST_TZ = os.getenv("TZ", "Asia/Kolkata")

def _today_ist_date() -> str:
    # render dynos usually in UTC; we just use naive local date string
    return datetime.now().date().isoformat()

def _to_float(x, default=0.0) -> float:
    try:
        if x in ("", None): return default
        return float(str(x).replace(",", ""))
    except Exception:
        return default

def _col_index(headers: List[str], name: str) -> int:
    try:
        return headers.index(name)
    except ValueError:
        return -1

def _read_trades_today(sheet, symbol: str) -> List[Dict[str, Any]]:
    ws = sheet.ss.worksheet("Trades")
    rows = ws.get_all_values()
    if not rows or len(rows) < 2:
        return []
    headers = rows[0]
    i_ts   = _col_index(headers, "ts_open")
    i_sym  = _col_index(headers, "symbol")
    i_pnl  = _col_index(headers, "pnl")
    today = _today_ist_date()
    out = []
    for r in rows[1:]:
        if i_sym >= 0 and len(r) > i_sym and r[i_sym] and r[i_sym] != symbol:
            continue
        # match date by prefix of ts_open (YYYY-MM-DD ...)
        ts_ok = True
        if i_ts >= 0 and len(r) > i_ts and r[i_ts]:
            ts_ok = str(r[i_ts]).strip().startswith(today)
        if not ts_ok:
            continue
        pnl = _to_float(r[i_pnl] if (i_pnl >= 0 and len(r) > i_pnl) else 0.0, 0.0)
        out.append({"row": r, "pnl": pnl})
    return out

def _compute_perf(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(trades)
    pnls = [t["pnl"] for t in trades]
    wins = sum(1 for p in pnls if p > 0)
    losses = sum(1 for p in pnls if p < 0)
    gross = sum(pnls) if pnls else 0.0
    avg = statistics.mean(pnls) if pnls else 0.0

    # crude max drawdown from cumulative PnL sequence
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for p in pnls:
        cum += p
        peak = max(peak, cum)
        dd = peak - cum
        max_dd = max(max_dd, dd)

    win_rate = round(wins * 100.0 / n, 2) if n > 0 else 0.0
    return {
        "trades": n,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "avg_pnl": round(avg, 2),
        "gross_pnl": round(gross, 2),
        "net_pnl": round(gross, 2),   # fees/slip not modeled here
        "max_dd": round(max_dd, 2),
    }

def write_eod(sheet, cfg, symbol: Optional[str] = None) -> Dict[str, Any]:
    """Reads today's Trades → appends a Performance row. Returns the perf dict."""
    symbol = symbol or getattr(cfg, "symbol", os.getenv("OC_SYMBOL_PRIMARY", "NIFTY"))
    trades = _read_trades_today(sheet, symbol)
    perf = _compute_perf(trades)

    row = {
        "date": _today_ist_date(),
        "symbol": symbol,
        **perf,
        "version": getattr(cfg, "git_sha", os.getenv("GIT_SHA", ""))[:10],
        "notes": "auto",
    }

    # Prefer logger API if present
    try:
        logger.log_performance(sheet, row)
    except Exception:
        # Fallback append
        try:
            ws = sheet.ss.worksheet("Performance")
            ws.append_row([
                row["date"], row["symbol"], row["trades"], row["wins"], row["losses"],
                row["win_rate"], row["avg_pnl"], row["gross_pnl"], row["net_pnl"],
                row["max_dd"], row["version"], row["notes"]
            ])
        except Exception as e:
            print(f"[eod_perf] append failed: {e}", flush=True)
    return perf

def send_daily_summary(perf: Dict[str, Any], cfg) -> None:
    """Optional Telegram summary at ~15:35 IST."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    uids = (os.getenv("TELEGRAM_USER_ID", "") or "").split(",")
    if not token or not uids or not uids[0].strip():
        return
    chat_id = uids[0].strip()
    text = (
        f"Daily Summary ✅ {getattr(cfg,'symbol','NIFTY')} ({_today_ist_date()})\n"
        f"Trades={perf.get('trades',0)} | Wins={perf.get('wins',0)} | "
        f"Losses={perf.get('losses',0)} | WinRate={perf.get('win_rate',0)}%\n"
        f"AvgPnL={perf.get('avg_pnl',0)} | Gross={perf.get('gross_pnl',0)} | "
        f"Net={perf.get('net_pnl',0)} | MaxDD={perf.get('max_dd',0)}"
    )
    try:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      json={"chat_id": chat_id, "text": text}, timeout=12)
    except Exception:
        pass
