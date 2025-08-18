# ops/events_gate.py
from __future__ import annotations
from datetime import datetime
from typing import Tuple, List

_TRUE = {"1","true","yes","on","y","active"}

def _idx(headers: List[str], name: str) -> int:
    name = name.strip().lower()
    try:
        return [h.strip().lower() for h in headers].index(name)
    except ValueError:
        return -1

def _parse_hhmm(s: str) -> Tuple[int,int]:
    s = (s or "").strip()
    if not s or ":" not in s: return (-1, -1)
    try:
        hh, mm = s.split(":",1)
        return int(hh), int(mm)
    except Exception:
        return (-1, -1)

def _now_hhmm() -> Tuple[int,int]:
    now = datetime.now()
    return now.hour, now.minute

def _in_window(win: str) -> bool:
    """
    window string "HH:MM-HH:MM" in local (TZ env e.g., Asia/Kolkata).
    Empty/invalid => treat as ALL-DAY hold.
    Supports wrap? No (assume intra-day).
    """
    win = (win or "").strip()
    if not win or "-" not in win:
        return True  # all-day hold
    lo, hi = win.split("-",1)
    h1,m1 = _parse_hhmm(lo)
    h2,m2 = _parse_hhmm(hi)
    if h1<0 or h2<0:  # bad format => hold all day
        return True
    nh, nm = _now_hhmm()
    after_lo = (nh, nm) >= (h1, m1)
    before_hi = (nh, nm) <= (h2, m2)
    return after_lo and before_hi

def is_hold_now(sheet) -> Tuple[bool, str]:
    """
    Reads Google Sheet 'Events' tab with headers:
      date | type | window | note | active
    Returns (is_hold, reason_text).
    Only checks today's date (YYYY-MM-DD) and active rows.
    """
    try:
        ws = sheet.ss.worksheet("Events")
        rows = ws.get_all_values()
    except Exception:
        return (False, "")

    if not rows or len(rows) < 2:
        return (False, "")

    headers = rows[0]
    id_date   = _idx(headers, "date")
    id_type   = _idx(headers, "type")
    id_window = _idx(headers, "window")
    id_note   = _idx(headers, "note")
    id_active = _idx(headers, "active")

    today = datetime.now().date().isoformat()
    reasons = []

    for r in rows[1:]:
        try:
            dt  = (r[id_date] if id_date>=0 and len(r)>id_date else "").strip()
            act = (r[id_active] if id_active>=0 and len(r)>id_active else "").strip().lower()
            if dt != today or act not in _TRUE:
                continue
            typ = (r[id_type] if id_type>=0 and len(r)>id_type else "").strip()
            win = (r[id_window] if id_window>=0 and len(r)>id_window else "").strip()
            note= (r[id_note] if id_note>=0 and len(r)>id_note else "").strip()
            if _in_window(win):
                reasons.append(f"{typ} {win or '(all-day)'} {note}".strip())
        except Exception:
            continue

    if reasons:
        return (True, "; ".join(reasons)[:300])
    return (False, "")
