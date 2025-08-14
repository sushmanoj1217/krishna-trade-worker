
import os, csv, json, urllib.request
from datetime import datetime
from tzlocal import get_localzone

LEVELS_PATH = "data/levels.json"

def update_levels_from_sheet(bus):
    url = os.getenv("OC_SHEET_CSV_URL", "")
    if not url: return
    with urllib.request.urlopen(url) as resp:
        data = resp.read().decode("utf-8").splitlines()
    rows = list(csv.DictReader(data))
    if not rows: return
    r = rows[-1]
    def f(k):
        v = r.get(k)
        try: return float(v) if v not in (None,"") else None
        except: return None
    lv = {"symbol": os.getenv("OC_SYMBOL_PRIMARY","NIFTY"),
          "spot": f("spot"), "s1": f("s1"), "s2": f("s2"), "r1": f("r1"), "r2": f("r2"),
          "expiry": r.get("expiry") or None, "signal": r.get("signal") or None,
          "ts": datetime.now(get_localzone()).isoformat()}
    bus.emit("levels", lv)
    with open(LEVELS_PATH, "w", encoding="utf-8") as f:
        json.dump({"symbols": {lv['symbol']: lv}, "ts": lv["ts"], "primary": lv["symbol"]}, f, indent=2)
