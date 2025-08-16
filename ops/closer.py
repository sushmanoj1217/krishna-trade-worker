# ops/closer.py
from __future__ import annotations
from agents.trade_executor import close_open_trades_time_exit

def time_exit_all(sheet, cfg):
    try:
        n = close_open_trades_time_exit(sheet, getattr(cfg, "symbol", "NIFTY"))
        print(f"[closer] time-exit closed={n}", flush=True)
    except Exception as e:
        print(f"[closer] time-exit failed: {e}", flush=True)
