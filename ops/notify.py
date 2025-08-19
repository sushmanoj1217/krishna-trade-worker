# ops/notify.py
from __future__ import annotations

import os
import requests
from datetime import datetime

BOT = os.getenv("TELEGRAM_BOT_TOKEN")
USER = os.getenv("TELEGRAM_USER_ID")

def _now():
    return datetime.now().isoformat(sep=" ", timespec="seconds")

def send_telegram(text: str):
    """
    Always prints to stdout. If TELEGRAM_* is configured, also sends to Telegram.
    """
    text = str(text)
    # Always log to Render stdout
    print(f"[{_now()}] [TG] {text}", flush=True)

    if not BOT or not USER:
        return

    try:
        url = f"https://api.telegram.org/bot{BOT}/sendMessage"
        payload = {"chat_id": int(USER), "text": text}
        r = requests.post(url, json=payload, timeout=6)
        r.raise_for_status()
    except Exception as e:
        print(f"[{_now()}] [TG-ERROR] {e}", flush=True)
