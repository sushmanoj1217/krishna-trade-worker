"""
Events/HOLD gate source.
- HOLD can be driven by ENV NEWS_HOLD=on/off, or latest 'Events' sheet row with status='HOLD'
"""
import os
from integrations import sheets as sh

def hold_active() -> bool:
    env = os.getenv("NEWS_HOLD", "").strip().lower()
    if env in ("1","true","yes","on","hold"):
        return True
    if env in ("0","false","no","off",""):
        pass
    # check last Events row
    rows = sh._DB.get("Events") or []
    if rows:
        last = rows[-1]
        status = (last[1] if len(last)>1 else "") or (last[2] if len(last)>2 else "")
        if str(status).strip().upper() == "HOLD":
            return True
    return False
