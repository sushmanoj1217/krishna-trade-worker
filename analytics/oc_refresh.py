# analytics/oc_refresh.py  (केवल relevant हिस्से दिखा रहा हूँ)
import os, json
from datetime import datetime
from tzlocal import get_localzone
from integrations.dhan import DhanClient, _sym_norm

# ...

def update_levels_from_dhan(bus):
    client = DhanClient()
    symbols = _parse_symbols()
    primary = _choose_primary(symbols)

    # ✅ Memory-safe: by default sirf PRIMARY fetch karo
    fetch_all = os.getenv("OC_FETCH_ALL", "off").lower() == "on"
    if not fetch_all:
        symbols = [primary]

    levels_all = {}
    for sym in symbols:
        try:
            usid = client.resolve_underlying_scrip(sym)
            if not usid:
                print(f"[oc] resolve fail: {sym}")
                continue
            exps = client.get_expiries(usid)
            if not exps:
                print(f"[oc] no expiries: {sym}")
                continue
            expiry = exps[0]
            oc = client.get_option_chain(usid, expiry)
            lv = _compute_levels_from_oc(oc)
            lv.update({"ts": datetime.now(get_localzone()).isoformat(), "expiry": expiry, "symbol": sym})
            levels_all[sym] = lv
            bus.emit("levels", lv)
            print(f"[oc] {sym} spot={lv.get('spot')} s1={lv.get('s1')} r1={lv.get('r1')}")
        except Exception as e:
            print(f"[oc] error {sym}: {e}")

    # ... (rest as-is)
