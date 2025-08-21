from dataclasses import dataclass
from datetime import datetime

@dataclass
class OCSnapshot:
    ts: datetime
    spot: float
    s1: float
    s2: float
    r1: float
    r2: float
    expiry: str
    vix: float | None
    pcr: float | None
    max_pain: float
    bias: str | None
    stale: bool = False

_CACHE: OCSnapshot | None = None

def set_snapshot(snap: OCSnapshot):
    global _CACHE
    _CACHE = snap

def get_snapshot() -> OCSnapshot | None:
    return _CACHE
