# path: risk/circuit_breaker.py
import os, time
from integrations import telegram
from agents import logger

# in-memory state (per-process)
_SL_TIMES: list[float] = []
_PAUSE_UNTIL: float = 0.0

def _cfg():
    thr  = int(os.getenv("CIRCB_SL_THRESHOLD", "3") or "3")     # N SLs...
    winm = int(os.getenv("CIRCB_WINDOW_MINS", "15") or "15")    # ...within M minutes
    pause= int(os.getenv("CIRCB_PAUSE_MINS", "20") or "20")     # ...then pause Z minutes
    return thr, winm*60, pause*60

def _now():
    return time.time()

def _trim():
    thr, win_s, _ = _cfg()
    t = _now()
    while _SL_TIMES and (t - _SL_TIMES[0]) > win_s:
        _SL_TIMES.pop(0)

def is_paused() -> tuple[bool, str]:
    t = _now()
    if t < _PAUSE_UNTIL:
        left = int(_PAUSE_UNTIL - t)
        mins = left // 60
        secs = left % 60
        return True, f"circuit pause {mins}m{secs}s left"
    return False, ""

def on_trade_close(evt: dict, sheet, cfg):
    """Call on every trade_close bus event."""
    global _PAUSE_UNTIL
    reason = (evt.get("reason") or "").upper()
    if "SL" not in reason:
        return
    _SL_TIMES.append(_now())
    _trim()

    thr, _, pause_s = _cfg()
    if len(_SL_TIMES) >= thr:
        # trip breaker
        _PAUSE_UNTIL = _now() + pause_s
        _SL_TIMES.clear()
        msg = f"⚠️ Circuit TRIPPED [{cfg.symbol}] — pausing new entries for {pause_s//60}m"
        telegram.send(msg)
        logger.log_status(sheet, {"state":"HOLD", "message": msg})

def reset(sheet=None, cfg=None):
    global _PAUSE_UNTIL
    _SL_TIMES.clear()
    _PAUSE_UNTIL = 0.0
    if sheet and cfg:
        logger.log_status(sheet, {"state":"OK", "message":"circuit reset"})
