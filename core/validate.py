# path: core/validate.py
import os
from core.version import git_sha

def startup_summary() -> list[str]:
    lines = []
    lines.append(f"OC_MODE={os.getenv('OC_MODE','dhan').lower()}")
    sid = os.getenv("GSHEET_SPREADSHEET_ID","")
    lines.append("GSHEET_SPREADSHEET_ID=" + ("set" if sid else "MISSING"))
    if os.getenv("OC_MODE","dhan").lower()=="dhan":
        lines.append("DHAN_CLIENT_ID=" + ("set" if os.getenv("DHAN_CLIENT_ID","") else "MISSING"))
        lines.append("DHAN_ACCESS_TOKEN=" + ("set" if os.getenv("DHAN_ACCESS_TOKEN","") else "MISSING"))
        if os.getenv("DHAN_USID_MAP",""): lines.append("DHAN_USID_MAP="+os.getenv("DHAN_USID_MAP"))
    if os.getenv("OC_MODE","")=="sheet":
        lines.append("OC_SHEET_CSV_URL="+("set" if os.getenv("OC_SHEET_CSV_URL","") else "MISSING"))
    tlg = ("ready" if (os.getenv("TELEGRAM_BOT_TOKEN","") and (os.getenv("TELEGRAM_USER_ID","") or os.getenv("TELEGRAM_CHAT_ID",""))) else "off")
    lines.append("TELEGRAM="+tlg)
    sha = git_sha()
    if sha: lines.append("GIT_SHA="+sha[:10])
    return lines

def print_startup_summary():
    for ln in startup_summary():
        print("[boot]", ln)
