# path: agents/override_guard.py
import os
from agents import logger

def is_manual_allowed() -> bool:
    return os.getenv("ALLOW_MANUAL","off").lower() == "on"

def audit_attempt(sheet, who: str, what: str):
    logger.log_status(sheet, {"state":"WARN", "message": f"manual_attempt by={who} cmd='{what}'"})
