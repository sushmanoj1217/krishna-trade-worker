# integrations/telegram.py
# Priority: ENV -> Fallback (hard-coded below)

import os
import requests

# ---- Fallbacks (your provided creds) ----
TOKEN_FALLBACK = "7987248090:AAGMOE56FV3H_eHFBbVF1PNCUPS_GMwmaeA"
USER_IDS_FALLBACK = ["1266551700"]  # multiple allowed: ["1266551700","-1001234567890"]

def _parse_recipients(s: str) -> list[str]:
    if not s:
        return []
    s = s.replace("\n", ",").replace(" ", ",")
    return [p.strip() for p in s.split(",") if p.strip()]

def _cfg():
    token = (os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip() or TOKEN_FALLBACK
    raw_ids = (os.getenv("TELEGRAM_USER_ID", "") or os.getenv("TELEGRAM_CHAT_ID", "") or "").strip()
    recipients = _parse_recipients(raw_ids) if raw_ids else USER_IDS_FALLBACK
    return token, recipients

def send(text: str, disable_preview: bool = True) -> bool:
    token, recipients = _cfg()
    if not token or not recipients or not text:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    ok_any = False
    base = {"text": text[:4000], "disable_web_page_preview": disable_preview}
    for cid in recipients:
        try:
            r = requests.post(url, json={**base, "chat_id": cid}, timeout=10)
            ok_any = (r.status_code == 200) or ok_any
        except Exception:
            pass
    return ok_any
