# agents/trade_executor.py
from __future__ import annotations
import os, time, requests
from typing import Dict, Any, Optional, List
from agents import logger
from agents import circuit

def _env_int(name: str, default: int) -> int:
    try: return int(os.getenv(name, str(default)))
    except: return default

def _tg_send(text: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat = (os.getenv("TELEGRAM_USER_ID", "") or "").split(",")[0].strip()
    if not token or not chat: return
    try:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      json={"chat_id": chat, "text": text}, timeout=10)
    except Exception:
        pass

# ---------- OPEN ----------
def open_trade(sheet, sig: Dict[str, Any]) -> Optional[str]:
    side   = sig.get("side", "")
    symbol = sig.get("symbol", "NIFTY")
    level  = sig.get("level")
    reason = sig.get("reason", "")
    dkey   = sig.get("dedup_key", "")  # use as trade_id
    qty    = _env_int("QTY_PER_TRADE", 50)

    try:
        logger.log_trade(sheet, {
            "ts_open": "",
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "entry": "",
            "sl": "",
            "tp": "",
            "ts_close": "",
            "exit_price": "",
            "pnl": "",
            "reason_open": f"{reason} @ {level}",
            "reason_close": "",
            "trade_id": dkey or "",
        })
        _tg_send(f"OPEN ▶️ {symbol} {side} | {reason} | level={level} | qty={qty}")
        return dkey or ""
    except Exception as e:
        _tg_send(f"OPEN ❌ failed: {e}")
        return None

# ---------- CLOSE ----------
def close_trade(sheet, trade_id: str, exit_price: str = "", reason_close: str = "manual") -> bool:
    ok = logger.update_trade_by_id(sheet, trade_id, {
        "ts_close": "", "exit_price": exit_price, "reason_close": reason_close
    })
    if ok:
        if "SL" in reason_close.upper():
            circuit.notify_sl_hit()
        _tg_send(f"CLOSE ⏹️ trade_id={trade_id} reason={reason_close} price={exit_price or '-'}")
    return ok

# ---------- Utility: close all open trades for today (time-exit) ----------
def close_open_trades_time_exit(sheet, symbol: str = "NIFTY") -> int:
    ws = sheet.ss.worksheet("Trades")
    rows = ws.get_all_values()
    if not rows or len(rows) < 2: return 0
    hdr = rows[0]
    idx = {h:i for i,h in enumerate(hdr)}
    def col(name): return idx.get(name, -1)
    i_ts_open, i_sym, i_ts_close, i_tid = col("ts_open"), col("symbol"), col("ts_close"), col("trade_id")
    n = 0
    for r_i in range(1, len(rows)):
        r = rows[r_i]
        if i_sym>=0 and len(r)>i_sym and r[i_sym] and r[i_sym] != symbol: continue
        # today filter
        if i_ts_open>=0 and len(r)>i_ts_open and r[i_ts_open]:
            if str(r[i_ts_open]).split(" ")[0] != time.strftime("%Y-%m-%d"):
                continue
        # open?
        if i_ts_close>=0 and len(r)>i_ts_close and r[i_ts_close]:
            continue
        tid = r[i_tid] if i_tid>=0 and len(r)>i_tid else ""
        if not tid: continue
        ok = logger.update_trade_by_id(sheet, tid, {"ts_close":"", "reason_close":"time_exit"})
        if ok: n += 1
    if n:
        _tg_send(f"TIME EXIT 🔔 closed {n} open trades")
    return n
