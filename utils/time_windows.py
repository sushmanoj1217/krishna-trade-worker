from datetime import datetime, time, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))

def NOW_IST() -> datetime:
    return datetime.now(IST)

def is_market_open_now() -> bool:
    now = NOW_IST()
    # No-trade: 09:15–09:30 and 14:45–15:15; open 09:15–15:30
    if now.weekday() >= 5:
        return False
    open_t = time(9, 15, tzinfo=IST)
    close_t = time(15, 15, tzinfo=IST)  # we auto-flat 15:15
    if not (open_t <= now.timetz() <= time(15, 30, tzinfo=IST)):
        return False
    if time(9, 15, tzinfo=IST) <= now.timetz() < time(9, 30, tzinfo=IST):
        return False
    if time(14, 45, tzinfo=IST) <= now.timetz() <= time(15, 15, tzinfo=IST):
        return False
    return True

def next_market_close_dt_ist() -> datetime:
    now = NOW_IST()
    return now.replace(hour=15, minute=15, second=0, microsecond=0)

async def sleep_until(dt: datetime):
    import asyncio
    now = datetime.now(dt.tzinfo)
    sec = max(0, (dt - now).total_seconds())
    await asyncio.sleep(sec)
