# utils/params.py
import os
from dataclasses import dataclass

# --- Defaults ---
_DEF_BUFFERS = {"NIFTY": 12, "BANKNIFTY": 30, "FINNIFTY": 15}
_DEF_MP_DIST = {"NIFTY": 25, "BANKNIFTY": 60, "FINNIFTY": 30}
_DEF_MIN_TARGET = {"NIFTY": 20, "BANKNIFTY": 40, "FINNIFTY": 25}

# ΔOI detection defaults
_DEF_OI_CLUSTER_STRIKES = 1        # ±N strikes around the trigger strike
_DEF_OI_WINDOW_MIN = 5             # fallback window (if no trigger chosen), ±5 strikes
_DEF_OI_DELTA_MIN_CE = 10000       # absolute OI change min for CE leg
_DEF_OI_DELTA_MIN_PE = 10000       # absolute OI change min for PE leg
_DEF_OI_DELTA_PCT_MIN = 0.30       # relative (vs prev window sum)

def _get_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name)
        return float(v) if v is not None and str(v).strip() != "" else default
    except Exception:
        return default

def _get_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name)
        return int(float(v)) if v is not None and str(v).strip() != "" else default
    except Exception:
        return default

def _parse_symbol_map(raw: str) -> dict[str, int]:
    out: dict[str, int] = {}
    if not raw:
        return out
    parts = [p for p in raw.replace(";", ",").split(",") if p.strip()]
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            k = k.strip().upper()
            v = v.strip()
            if v.replace(".", "", 1).isdigit():
                out[k] = int(float(v))
    return out

@dataclass
class Params:
    """
    Central read of tunables from ENV (and later Sheet overrides).
    """
    symbol: str = os.getenv("OC_SYMBOL", "NIFTY").upper()

    # --- Bands / buffers ---
    def buffer_points(self) -> int:
        direct = os.getenv("ENTRY_BAND_POINTS", "").strip()
        if direct and direct.replace(".", "", 1).isdigit():
            return int(float(direct))
        m = _parse_symbol_map(os.getenv("ENTRY_BAND_POINTS_MAP", ""))
        if m.get(self.symbol):
            return m[self.symbol]
        return _DEF_BUFFERS.get(self.symbol, 12)

    # --- MV thresholds ---
    def pcr_bull_high(self) -> float:
        return _get_float("PCR_BULL_HIGH", 1.10)

    def pcr_bear_low(self) -> float:
        return _get_float("PCR_BEAR_LOW", 0.90)

    def mp_support_dist(self) -> int:
        key = f"MP_SUPPORT_DIST_{self.symbol}"
        if os.getenv(key):
            return _get_int(key, _DEF_MP_DIST.get(self.symbol, 25))
        return _DEF_MP_DIST.get(self.symbol, 25)

    # --- RR / targets ---
    def min_target_points(self) -> int:
        sym = self.symbol
        if sym == "NIFTY":
            return _get_int("MIN_TARGET_POINTS_N", _DEF_MIN_TARGET["NIFTY"])
        if sym == "BANKNIFTY":
            return _get_int("MIN_TARGET_POINTS_B", _DEF_MIN_TARGET["BANKNIFTY"])
        if sym == "FINNIFTY":
            return _get_int("MIN_TARGET_POINTS_F", _DEF_MIN_TARGET["FINNIFTY"])
        return _DEF_MIN_TARGET.get(sym, 20)

    # --- ΔOI thresholds / windowing for OC-Pattern ---
    def oi_cluster_strikes(self) -> int:
        return _get_int("OI_CLUSTER_STRIKES", _DEF_OI_CLUSTER_STRIKES)

    def oi_window_min(self) -> int:
        return _get_int("OI_WINDOW_MIN", _DEF_OI_WINDOW_MIN)

    def oi_delta_min_ce(self) -> int:
        return _get_int("OI_DELTA_MIN_CE", _DEF_OI_DELTA_MIN_CE)

    def oi_delta_min_pe(self) -> int:
        return _get_int("OI_DELTA_MIN_PE", _DEF_OI_DELTA_MIN_PE)

    def oi_delta_pct_min(self) -> float:
        return _get_float("OI_DELTA_PCT_MIN", _DEF_OI_DELTA_PCT_MIN)

    # Dump (debug)
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "buffer_points": self.buffer_points(),
            "pcr_bull_high": self.pcr_bull_high(),
            "pcr_bear_low": self.pcr_bear_low(),
            "mp_support_dist": self.mp_support_dist(),
            "min_target_points": self.min_target_points(),
            "oi_cluster_strikes": self.oi_cluster_strikes(),
            "oi_window_min": self.oi_window_min(),
            "oi_delta_min_ce": self.oi_delta_min_ce(),
            "oi_delta_min_pe": self.oi_delta_min_pe(),
            "oi_delta_pct_min": self.oi_delta_pct_min(),
        }
