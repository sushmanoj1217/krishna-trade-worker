# integrations/news_feed.py
from __future__ import annotations
import os
from integrations import sheets as sh

def hold_active() -> tuple[bool, str]:
    """
    Returns (is_hold, reason)
    Priority:
      1) ENV NEWS_HOLD=on/off
      2) Last Events row containing 'HOLD'
    """
    env = (os.getenv("NEWS_HOLD", "") or "").strip().lower()
    if env in ("1","true","yes","on","hold"):
        return True, "ENV:NEWS_HOLD"
    if env in ("0","false","no","off",""):
        pass
    rows = sh.get_last_event_rows(n=5)
    for r in reversed(rows):
        # [ts, event, status]
        status = (str(r[2]) if len(r) > 2 else "").upper()
        if "HOLD" in status:
            return True, "EVENT:HOLD"
    return False, ""
