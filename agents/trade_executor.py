# agents/trade_executor.py
# Minimal paper-trade executor (OPEN only). CLOSE/SL/TP ko later wire karenge.
# krishna_main.py auto-import karega: from agents.trade_executor import open_trade

from __future__ import annotations
import os, time, requests
from typing import Dict, Any, Optional
from agents import logger

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _tg_send(text: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    uids = (os.getenv("TELEGRAM_USER_ID", "") or "").split(",")
    if not token or not uids:
        return
    chat_id = uids[0].strip()
    if not chat_id:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      json={"chat_id": chat_id, "text": text}, timeout=10)
    except Exception:
        pass

def open_trade(sheet, sig: Dict[str, Any]) -> Optional[str]:
    """
    Creates a Trades row for a BUY_CE / BUY_PE signal.
    NOTE: Entry price unknown at this minimal stage; we log structure first.
    Returns trade_id (string) or None on failure.
    """
    side = sig.get("side", "")
    symbol = sig.get("symbol", "NIFTY")
    level = sig.get("level")
    reason = sig.get("reason", "")
    dkey = sig.get("dedup_key", "")  # use as trade_id

    # Quantities (env-driven)
    qty = _env_int("QTY_PER_TRADE", 50)

    # Trades schema:
    # ["ts_open","symbol","side","qty","entry","sl","tp","ts_close",
    #  "exit_price","pnl","reason_open","reason_close","trade_id"]
    try:
        logger.log_trade(sheet, {
            "ts_open": "",                 # auto-fill now
            "symbol": symbol,
            "side": side,                  # BUY_CE / BUY_PE
            "qty": qty,
            "entry": "",                   # (optional) to be filled by price feed later
            "sl": "",                      # to be derived from sl_pct when price available
            "tp": "",                      # to be derived from target_pct when price available
            "ts_close": "",
            "exit_price": "",
            "pnl": "",
            "reason_open": f"{reason} @ {level}",
            "reason_close": "",
            "trade_id": dkey or "",
        })
        _tg_send(f"OPEN ▶️ {symbol} {side} | reason: {reason} | level: {level} | qty={qty}")
        return dkey or ""
    except Exception as e:
        _tg_send(f"OPEN ❌ failed: {e}")
        return None
