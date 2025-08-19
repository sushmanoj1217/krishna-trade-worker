# utils/time_windows.py
from __future__ import annotations
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

NO_TRADE_1_START = dtime(9, 15)
NO_TRADE_1_END   = dtime(9, 30)
NO_TRADE_2_START = dtime(14, 45)
NO_TRADE_2_END   = dtime(15, 15)

def ist_now() -> datetime:
    return datetime.now(tz=IST)

def is_market_open_now() -> bool:
    t = ist_now().time()
    return t >= dtime(9, 15) and t < dtime(15, 15)

def is_no_trade_now() -> bool:
    t = ist_now().time()
    if NO_TRADE_1_START <= t < NO_TRADE_1_END:
        return True
    if NO_TRADE_2_START <= t < NO_TRADE_2_END:
        return True
    return False
