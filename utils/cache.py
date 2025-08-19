from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime

@dataclass
class OCSnapshot:
    spot: float
    s1: float | None
    s2: float | None
    r1: float | None
    r2: float | None
    expiry: str
    vix: float | None = None
    pcr: float | None = None
    max_pain: float | None = None
    max_pain_dist: float | None = None
    bias_tag: str | None = None
    stale: bool = False
    ts: datetime | None = None
    extras: Dict[str, Any] = field(default_factory=dict)

_last_snapshot: Optional[OCSnapshot] = None

def set_snapshot(s: OCSnapshot):
    global _last_snapshot
    _last_snapshot = s

def get_snapshot() -> Optional[OCSnapshot]:
    return _last_snapshot
