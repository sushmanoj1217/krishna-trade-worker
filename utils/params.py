import os
import json
from dataclasses import dataclass

@dataclass
class Params:
    # Environment-driven defaults
    symbol: str = os.getenv("OC_SYMBOL", "NIFTY")
    buffer_points: int = int(os.getenv("OC_BUFFER_POINTS", "12"))
    qty_per_trade: int = int(os.getenv("QTY_PER_TRADE", "50"))
    max_trades_per_day: int = int(os.getenv("MAX_TRADES_PER_DAY", "6"))
    exposure_cap: int = int(os.getenv("MAX_EXPOSURE_PER_TRADE", "3000"))

    # Sentiment bands
    vix_low: float = float(os.getenv("VIX_LOW", "12"))
    vix_high: float = float(os.getenv("VIX_HIGH", "18"))
    pcr_bull_high: float = float(os.getenv("PCR_BULL_HIGH", "1.10"))
    pcr_bear_low: float = float(os.getenv("PCR_BEAR_LOW", "0.90"))

    # MaxPain distance per symbol
    mp_dist_nifty: int = int(os.getenv("MP_SUPPORT_DIST_NIFTY", "25"))
    mp_dist_banknifty: int = int(os.getenv("MP_SUPPORT_DIST_BANKNIFTY", "60"))
    mp_dist_finnifty: int = int(os.getenv("MP_SUPPORT_DIST_FINNIFTY", "30"))

    # Minimum target points per symbol
    min_target_nifty: int = int(os.getenv("MIN_TARGET_POINTS_N", "20"))
    min_target_banknifty: int = int(os.getenv("MIN_TARGET_POINTS_B", "40"))
    min_target_finnifty: int = int(os.getenv("MIN_TARGET_POINTS_F", "15"))

    def min_target_points(self) -> int:
        s = (self.symbol or "NIFTY").upper()
        return {
            "NIFTY": self.min_target_nifty,
            "BANKNIFTY": self.min_target_banknifty,
            "FINNIFTY": self.min_target_finnifty,
        }.get(s, self.min_target_nifty)

    def mp_support_dist(self) -> int:
        s = (self.symbol or "NIFTY").upper()
        return {
            "NIFTY": self.mp_dist_nifty,
            "BANKNIFTY": self.mp_dist_banknifty,
            "FINNIFTY": self.mp_dist_finnifty,
        }.get(s, self.mp_dist_nifty)

    def apply_overrides(self, override_json: str | None) -> None:
        """Merge JSON overrides into this Params object (keys must exist)."""
        if not override_json:
            return
        try:
            data = json.loads(override_json)
            if not isinstance(data, dict):
                return
            for k, v in data.items():
                if hasattr(self, k):
                    setattr(self, k, v)
        except Exception:
            # Ignore bad JSON silently
            pass
