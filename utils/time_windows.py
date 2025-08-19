from datetime import datetime, time
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

NO_TRADE_WINDOWS = [
    (time(9, 15), time(9, 30)),
    (time(14, 45), time(15, 15)),
]

MARKET_CLOSE = time(15, 15)

def now_ist() -> datetime:
    return datetime.now(IST)

def in_window(t: time, start: time, end: time) -> bool:
    return start <= t <= end

def is_no_trade_now() -> bool:
    t = now_ist().time()
    return any(in_window(t, a, b) for a, b in NO_TRADE_WINDOWS)

def is_market_close_now() -> bool:
    t = now_ist().time()
    return t >= MARKET_CLOSE
