# path: core/config.py
import os, json, pathlib

DATA_DIR = pathlib.Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

class Cfg:
    def __init__(self):
        # ---- Core symbol / sheet ----
        self.symbol = os.getenv("OC_SYMBOL_PRIMARY", "NIFTY")
        self.sheet = {
            "levels_tab": "OC_Live",
            "signals_tab": "Signals",
            "trades_tab": "Trades",
            "perf_tab": "Performance",
            "events_tab": "Events",
            "status_tab": "Status",
        }

        # ---- OC cadence (day/night) ----
        self.oc_refresh_secs_day = int(os.getenv("OC_REFRESH_SECS", "10") or "10")
        self.oc_refresh_secs_night = 60

        # ---- Risk / sizing ----
        self.max_exposure_per_trade = float(os.getenv("MAX_EXPOSURE_PER_TRADE", "3000") or "3000")
        self.daily_loss_limit       = float(os.getenv("DAILY_LOSS_LIMIT", "6000") or "6000")
        self.max_trades_per_day     = int(os.getenv("MAX_TRADES_PER_DAY", "6") or "6")
        self.qty_per_trade          = int(os.getenv("QTY_PER_TRADE", "50") or "50")
        self.point_value            = float(os.getenv("POINT_VALUE", "1") or "1")

        self.rr_target              = float(os.getenv("RR_TARGET", "3") or "3")
        self.entry_band_points      = int(os.getenv("ENTRY_BAND_POINTS", "5") or "5")

def load_settings() -> Cfg:
    return Cfg()

def _deep_merge(a: dict, b: dict):
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(a.get(k), dict):
            _deep_merge(a[k], v)
        else:
            a[k] = v

def load_strategy_params() -> dict:
    """
    Base strategy params + optional overrides from data/params_override.json
    (auto_heal writes this file).
    """
    params = {
        "name": "v1",
        "entry_rules": {
            "entry_band_points": int(os.getenv("ENTRY_BAND_POINTS", "5") or "5"),
        },
        "exits": {
            "initial_sl_points": 15,
            "target_rr": float(os.getenv("RR_TARGET", "3") or "3"),
            # NEW: trailing defaults (avoid KeyError)
            "trailing_enabled": (os.getenv("TRAILING_ENABLED", "on").lower() == "on"),
            "trail_after_points": int(os.getenv("TRAIL_AFTER_POINTS", "15") or "15"),
            "trail_step_points": int(os.getenv("TRAIL_STEP_POINTS", "5") or "5"),
        },
    }

    # optional override file (produced by auto_heal)
    ov_path = DATA_DIR / "params_override.json"
    if ov_path.exists():
        try:
            ov = json.loads(ov_path.read_text())
            cleaned = {k: v for k, v in ov.items() if k != "_diff"}
            _deep_merge(params, cleaned)
        except Exception:
            pass

    return params
