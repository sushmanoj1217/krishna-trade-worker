import os, csv, json, urllib.request
from datetime import datetime
from tzlocal import get_localzone

LEVELS_PATH = "data/levels.json"

def fetch_sheet_csv(url: str) -> dict:
    with urllib.request.urlopen(url) as resp:
        data = resp.read().decode("utf-8").splitlines()
    rows = list(csv.DictReader(data))
    if not rows:
        return {}
    row = rows[-1]
    def fget(k):
        v = row.get(k)
        try:
            return float(v) if v not in (None, "",) else None
        except:
            return None
    lv = {
        "spot": fget("spot"),
        "s1": fget("s1"),
        "s2": fget("s2"),
        "r1": fget("r1"),
        "r2": fget("r2"),
        "signal": row.get("signal") or None,
        "expiry": row.get("expiry") or None
    }
    return lv

def update_levels_from_sheet(bus):
    url = os.getenv("OC_SHEET_CSV_URL", "")
    if not url:
        return
    try:
        lv = fetch_sheet_csv(url)
        if not lv:
            return
        lv["ts"] = datetime.now(get_localzone()).isoformat()
        with open(LEVELS_PATH, "w", encoding="utf-8") as f:
            json.dump(lv, f, indent=2)
        bus.emit("levels", lv)
        print(f"[oc] spot={lv.get('spot')} s1={lv.get('s1')} r1={lv.get('r1')}")
    except Exception as e:
        print("[oc] sheet fetch error:", e)

def oc_refresh_tick(bus):
    mode = os.getenv("OC_MODE", "sheet").lower()
    if mode == "sheet":
        update_levels_from_sheet(bus)
    else:
        print("[oc] non-sheet mode not implemented yet")
