# utils/params.py
import os
from dataclasses import dataclass

_DEF_BUFFERS = {"NIFTY": 12, "BANKNIFTY": 30, "FINNIFTY": 15}
_DEF_MP_DIST = {"NIFTY": 25, "BANKNIFTY": 60, "FINNIFTY": 30}
_DEF_MIN_TARGET = {"NIFTY": 20, "BANKNIFTY": 40, "FINNIFTY": 25}

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
    """
    Parse maps like "NIFTY=12,BANKNIFTY=30,FINNIFTY=15"
    """
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
        # direct single override takes precedence
        direct = os.getenv("ENTRY_BAND_POINTS", "").strip()
        if direct and direct.replace(".", "", 1).isdigit():
            return int(float(direct))
        # map override e.g. "NIFTY=12,BANKNIFTY=30"
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
        # allow per-symbol envs: MP_SUPPORT_DIST_NIFTY / _BANKNIFTY / _FINNIFTY
        key = f"MP_SUPPORT_DIST_{self.symbol}"
        if os.getenv(key):
            return _get_int(key, _DEF_MP_DIST.get(self.symbol, 25))
        return _DEF_MP_DIST.get(self.symbol, 25)

    # --- RR / targets ---
    def min_target_points(self) -> int:
        # MIN_TARGET_POINTS_{N,B,F}
        sym = self.symbol
        if sym == "NIFTY":
            return _get_int("MIN_TARGET_POINTS_N", _DEF_MIN_TARGET["NIFTY"])
        if sym == "BANKNIFTY":
            return _get_int("MIN_TARGET_POINTS_B", _DEF_MIN_TARGET["BANKNIFTY"])
        if sym == "FINNIFTY":
            return _get_int("MIN_TARGET_POINTS_F", _DEF_MIN_TARGET["FINNIFTY"])
        return _DEF_MIN_TARGET.get(sym, 20)

    # Placeholder for future Sheet overrides merge
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "buffer_points": self.buffer_points(),
            "pcr_bull_high": self.pcr_bull_high(),
            "pcr_bear_low": self.pcr_bear_low(),
            "mp_support_dist": self.mp_support_dist(),
            "min_target_points": self.min_target_points(),
        }
