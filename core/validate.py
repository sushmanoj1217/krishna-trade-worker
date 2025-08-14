# path: core/validate.py
import os

def startup_summary() -> list[str]:
    lines = []
    mode = os.getenv("OC_MODE","dhan").lower()
    lines.append(f"OC_MODE={mode}")

    # Sheets
    sid = os.getenv("GSHEET_SPREADSHEET_ID","")
    lines.append("GSHEET_SPREADSHEET_ID=" + ("set" if sid else "MISSING"))

    # Dhan
    if mode == "dhan":
        tok = os.getenv("DHAN_ACCESS_TOKEN","")
        cid = os.getenv("DHAN_CLIENT_ID","")
        lines.append("DHAN_CLIENT_ID=" + ("set" if cid else "MISSING"))
        lines.append("DHAN_ACCESS_TOKEN=" + ("set" if tok else "MISSING"))
        umap = os.getenv("DHAN_USID_MAP","")
        if umap:
            lines.append(f"DHAN_USID_MAP={umap}")

    # Sheet mode CSV
    if mode == "sheet":
        csv = os.getenv("OC_SHEET_CSV_URL","")
        lines.append("OC_SHEET_CSV_URL=" + ("set" if csv else "MISSING"))

    # Telegram
    tbot = os.getenv("TELEGRAM_BOT_TOKEN","")
    tuid = os.getenv("TELEGRAM_USER_ID","") or os.getenv("TELEGRAM_CHAT_ID","")
    lines.append("TELEGRAM=" + ("ready" if (tbot and tuid) else "off"))

    return lines

def print_startup_summary():
    for ln in startup_summary():
        print("[boot]", ln)
