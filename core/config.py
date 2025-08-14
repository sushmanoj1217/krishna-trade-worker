# path: core/config.py
import os, json, pathlib

DATA_DIR = pathlib.Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

class Cfg:
    def __init__(self):
        self.symbol = os.getenv("OC_SYMBOL_PRIMARY","NIFTY")
        self.sheet = {
            "levels_tab": "OC_Live",
            "signals_tab": "Signals",
            "trades_tab": "Trades",
            "perf_tab": "Performance",
            "events_tab": "Events",
            "status_tab": "Status",
        }
        self.oc_refresh_secs_day = int(os.getenv("OC_REFRESH_SECS","10") or "10")
        self.oc_refresh_secs_night = 60

def load_settings() -> Cfg:
    return Cfg()

def _deep_merge(a: dict, b: dict):
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(a.get(k), dict):
            _deep_merge(a[k], v)
        else:
            a[k] = v

def load_strategy_params() -> dict:
    # base defaults
    params = {
        "name": "v1",
        "entry_rules": {
            "entry_band_points": int(os.getenv("ENTRY_BAND_POINTS","5") or "5"),
        },
        "exits": {
            "initial_sl_points": 15,
            "target_rr": int(os.getenv("RR_TARGET","3") or "3"),
        }
    }
    # optional override file (produced by auto_heal)
    ov_path = DATA_DIR / "params_override.json"
    if ov_path.exists():
        try:
            ov = json.loads(ov_path.read_text())
            # apply overrides only where explicit value is given
            cleaned = {k:v for k,v in ov.items() if k != "_diff"}
            _deep_merge(params, cleaned)
        except Exception:
            pass
    return params
