# utils/telemetry.py
from __future__ import annotations
import threading, time
from typing import Dict, Any, Optional

_lock = threading.RLock()
_counters: Dict[str, int] = {}
_marks: Dict[str, float] = {}

def inc(key: str, n: int = 1) -> None:
    with _lock:
        _counters[key] = _counters.get(key, 0) + n
        _marks[f"last_{key}"] = time.time()

def mark(key: str) -> None:
    with _lock:
        _marks[key] = time.time()

def get() -> Dict[str, Any]:
    with _lock:
        return {"counters": dict(_counters), "marks": dict(_marks)}

def last_time(key: str) -> Optional[float]:
    with _lock:
        return _marks.get(key)
