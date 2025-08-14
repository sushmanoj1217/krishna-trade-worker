# integrations/telegram.py
# Uses TELEGRAM_BOT_TOKEN and TELEGRAM_USER_ID (single or multiple IDs)

import os
import requests

def _parse_recipients(s: str) -> list[str]:
    """
    Accepts comma/space/newline separated IDs.
    Example: "1266551700,-1001234567890  999999999\n888888888"
    """
    if not s:
        return []
    s = s.replace("\n", ",").replace(" ", ",")
    return [p.strip() for p in s.split(",") if p.strip()]

def _cfg():
    token = (os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()
    # Primary: TELEGRAM_USER_ID; Fallback: TELEGRAM_CHAT_ID (for backward-compat)
    raw_ids = (os.getenv("TELEGRAM_USER_ID", "") or os.getenv("TELEGRAM_CHAT_ID", "") or "").strip()
    recipients = _parse_recipients(raw_ids)
    return token, recipients

def send(text: str, disable_preview: bool = True) -> bool:
    """
    Send a plain-text message to one or more chat IDs.
    Returns True if at least one send succeeds.
    """
    token, recipients = _cfg()
    if not token or not recipients or not text:
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    ok_any = False
    payload_base = {
        "text": text[:4000],  # Telegram hard limit ~4096
        "disable_web_page_preview": disable_preview,
    }
    for cid in recipients:
        try:
            payload = dict(payload_base, chat_id=cid)
            r = requests.post(url, json=payload, timeout=10)
            # Treat 200 as success; otherwise just mark failure but continue others
            ok_any = (r.status_code == 200) or ok_any
        except Exception:
            # swallow; we'll just mark as failure and continue
            pass
    return ok_any
