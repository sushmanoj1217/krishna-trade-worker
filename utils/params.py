# utils/params.py
from __future__ import annotations
import os
from dataclasses import dataclass
from integrations import sheets as sh

# Defaults
_DEF_BUFFERS = {"NIFTY": 12, "BANKNIFTY": 30, "FINNIFTY": 15}
_DEF_MP_DIST = {"NIFTY": 25, "BANKNIFTY": 60, "FINNIFTY": 30}
_DEF_MIN_TARGET = {"NIFTY": 20, "BANKNIFTY": 40, "FINNIFTY": 25}
_DEF_OI_CLUSTER_STRIKES = 1
_DEF_OI_WINDOW_MIN = 5
_DEF_OI_DELTA_MIN_CE = 10000
_DEF_OI_DELTA_MIN_PE = 10000
_DEF_OI_DELTA_PCT_MIN = 0.30

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

def _override_or(env_value, override_value):
    return override_value if override_value is not None else env_value

@dataclass
class Params:
    symbol: str = os.getenv("OC_SYMBOL", "NIFTY").upper()

    def _ov_map(self) -> dict[str, str]:
        """Read Params_Override tab once per call; return {key: value}."""
        try:
            return sh.get_overrides_map()
        except Exception:
            return {}

    # ---- Bands / buffers ----
    def buffer_points(self) -> int:
        ov = self._ov_map()
        # map override e.g. ENTRY_BAND_POINTS_MAP
        m = _parse_symbol_map(ov.get("ENTRY_BAND_POINTS_MAP", os.getenv("ENTRY_BAND_POINTS_MAP", "")))
        if m.get(self.symbol): return m[self.symbol]
        direct = ov.get("ENTRY_BAND_POINTS", os.getenv("ENTRY_BAND_POINTS", "")).strip()
        if direct and direct.replace(".", "", 1).isdigit():
            return int(float(direct))
        return _DEF_BUFFERS.get(self.symbol, 12)

    # ---- MV thresholds ----
    def pcr_bull_high(self) -> float:
        ov = self._ov_map().get("PCR_BULL_HIGH")
        return float(ov) if ov else _get_float("PCR_BULL_HIGH", 1.10)

    def pcr_bear_low(self) -> float:
        ov = self._ov_map().get("PCR_BEAR_LOW")
        return float(ov) if ov else _get_float("PCR_BEAR_LOW", 0.90)

    def mp_support_dist(self) -> int:
        ov_key = f"MP_SUPPORT_DIST_{self.symbol}"
        ov = self._ov_map().get(ov_key)
        if ov: 
            try: return int(float(ov))
            except: pass
        return _get_int(ov_key, _DEF_MP_DIST.get(self.symbol, 25))

    # ---- RR / targets ----
    def min_target_points(self) -> int:
        ov_map = self._ov_map()
        sym = self.symbol
        if sym == "NIFTY":
            return int(float(ov_map.get("MIN_TARGET_POINTS_N") or _get_int("MIN_TARGET_POINTS_N", _DEF_MIN_TARGET["NIFTY"])))
        if sym == "BANKNIFTY":
            return int(float(ov_map.get("MIN_TARGET_POINTS_B") or _get_int("MIN_TARGET_POINTS_B", _DEF_MIN_TARGET["BANKNIFTY"])))
        if sym == "FINNIFTY":
            return int(float(ov_map.get("MIN_TARGET_POINTS_F") or _get_int("MIN_TARGET_POINTS_F", _DEF_MIN_TARGET["FINNIFTY"])))
        return _DEF_MIN_TARGET.get(sym, 20)

    # ---- ΔOI thresholds / windowing ----
    def oi_cluster_strikes(self) -> int:
        ov = self._ov_map().get("OI_CLUSTER_STRIKES")
        return int(float(ov)) if ov else _get_int("OI_CLUSTER_STRIKES", _DEF_OI_CLUSTER_STRIKES)

    def oi_window_min(self) -> int:
        ov = self._ov_map().get("OI_WINDOW_MIN")
        return int(float(ov)) if ov else _get_int("OI_WINDOW_MIN", _DEF_OI_WINDOW_MIN)

    def oi_delta_min_ce(self) -> int:
        ov = self._ov_map().get("OI_DELTA_MIN_CE")
        return int(float(ov)) if ov else _get_int("OI_DELTA_MIN_CE", _DEF_OI_DELTA_MIN_CE)

    def oi_delta_min_pe(self) -> int:
        ov = self._ov_map().get("OI_DELTA_MIN_PE")
        return int(float(ov)) if ov else _get_int("OI_DELTA_MIN_PE", _DEF_OI_DELTA_MIN_PE)

    def oi_delta_pct_min(self) -> float:
        ov = self._ov_map().get("OI_DELTA_PCT_MIN")
        return float(ov) if ov else _get_float("OI_DELTA_PCT_MIN", _DEF_OI_DELTA_PCT_MIN)

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
