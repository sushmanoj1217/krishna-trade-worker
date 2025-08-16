# agents/circuit.py
from __future__ import annotations
import os, time
from typing import Optional

_state = {"pause_until": 0.0, "sl_hits": []}  # [(ts), ...] recent SL timestamps

def _env_i(name, d): 
    try: return int(os.getenv(name, str(d)))
    except: return d

def should_pause() -> bool:
    return time.time() < _state["pause_until"]

def notify_sl_hit():
    now = time.time()
    win_mins = _env_i("CIRCB_WINDOW_MINS", 15)
    thr = _env_i("CIRCB_SL_THRESHOLD", 3)
    pause = _env_i("CIRCB_PAUSE_MINS", 20)
    # push
    _state["sl_hits"] = [t for t in _state["sl_hits"] if now - t < win_mins * 60] + [now]
    if len(_state["sl_hits"]) >= thr:
        _state["pause_until"] = now + pause * 60
        _state["sl_hits"].clear()

def pause_remaining_secs() -> int:
    rem = int(_state["pause_until"] - time.time())
    return rem if rem > 0 else 0
