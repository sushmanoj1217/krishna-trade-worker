# path: integrations/telegram.py
# Priority: ENV -> Fallback (only if ENV missing)

import os, json, requests, time, pathlib

# --- Fallbacks (used only when envs are empty) ---
TOKEN_FALLBACK = "7987248090:AAGMOE56FV3H_eHFBbVF1PNCUPS_GMwmaeA"
USER_IDS_FALLBACK = ["1266551700"]  # multiple allowed: ["1266551700","-100xyz"]

DATA_DIR = pathlib.Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
OFFSET_FILE = DATA_DIR / "telegram_offset.json"

def _parse_ids(s: str) -> list[str]:
    if not s: return []
    s = s.replace("\n", ",").replace(" ", ",")
    return [p.strip() for p in s.split(",") if p.strip()]

def _cfg():
    token = (os.getenv("TELEGRAM_BOT_TOKEN","").strip() or TOKEN_FALLBACK)
    ids = _parse_ids(os.getenv("TELEGRAM_USER_ID","").strip() or os.getenv("TELEGRAM_CHAT_ID","").strip())
    if not ids: ids = USER_IDS_FALLBACK
    return token, ids

def send(text: str, disable_preview: bool = True) -> bool:
    token, ids = _cfg()
    if not token or not ids or not text: return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    ok = False
    base = {"text": text[:4000], "disable_web_page_preview": disable_preview}
    for cid in ids:
        try:
            r = requests.post(url, json={**base, "chat_id": cid}, timeout=10)
            ok = (r.status_code == 200) or ok
        except Exception:
            pass
    return ok

def _load_offset() -> int:
    if OFFSET_FILE.exists():
        try:
            return int(json.loads(OFFSET_FILE.read_text()).get("offset", 0))
        except Exception:
            return 0
    return 0

def _save_offset(offset: int):
    try:
        OFFSET_FILE.write_text(json.dumps({"offset": int(offset)}))
    except Exception:
        pass

def fetch_updates(timeout_sec: int = 10) -> list[dict]:
    token, _ = _cfg()
    if not token: return []
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    params = {
        "timeout": timeout_sec,
        "allowed_updates": json.dumps(["message"]),
        "offset": _load_offset() + 1
    }
    try:
        r = requests.get(url, params=params, timeout=timeout_sec + 5)
        if r.status_code != 200: return []
        data = r.json() or {}
        updates = data.get("result", []) or []
        if updates:
            _save_offset(max(u.get("update_id", 0) for u in updates))
        return updates
    except Exception:
        return []

def extract_command_text(update: dict) -> tuple[str, int, str]:
    """
    Returns (text, chat_id, sender_id) or ("", 0, "")
    """
    msg = update.get("message") or {}
    text = (msg.get("text") or "").strip()
    chat = msg.get("chat") or {}
    frm  = msg.get("from") or {}
    return text, int(chat.get("id", 0)), str(frm.get("id",""))
