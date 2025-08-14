# path: agents/event_filter.py
from datetime import datetime, time as dtime
from tzlocal import get_localzone

TAB = "Events"  # date,time_ist,name,severity,effect,window_start,window_end

def _parse_hhmm(s: str) -> dtime | None:
    if not s:
        return None
    s = s.strip()
    try:
        hh, mm = s.split(":")
        return dtime(int(hh), int(mm))
    except Exception:
        return None

def is_blocked_now(sheet, cfg) -> tuple[bool, str]:
    """Returns (blocked?, reason). Blocks when any row for today has effect=disable."""
    try:
        rows = sheet.read_all(cfg.sheet.get("events_tab", TAB))
    except Exception:
        return (False, "")
    if not rows:
        return (False, "")
    tz = get_localzone()
    now = datetime.now(tz)
    today = now.strftime("%Y-%m-%d")

    for r in rows:
        if str(r.get("date", "")).strip() != today:
            continue
        effect = (r.get("effect", "") or "").strip().lower()
        severity = (r.get("severity", "") or "").strip().lower()
        if effect != "disable":
            continue
        ws = _parse_hhmm((r.get("window_start", "") or "").strip())
        we = _parse_hhmm((r.get("window_end", "") or "").strip())

        start_dt = now.replace(hour=ws.hour, minute=ws.minute, second=0, microsecond=0) if ws else now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt   = now.replace(hour=we.hour, minute=we.minute, second=59, microsecond=0) if we else now.replace(hour=23, minute=59, second=59, microsecond=0)
        if start_dt <= now <= end_dt:
            name = (r.get("name", "") or "").strip()
            return (True, f"{severity or 'event'}: {name} {start_dt.time()}–{end_dt.time()}")

    return (False, "")
