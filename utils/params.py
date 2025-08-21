import os
from dataclasses import dataclass

BUFFERS = {"NIFTY": 12, "BANKNIFTY": 30, "FINNIFTY": 15}

@dataclass
class Params:
    symbol: str
    buffer_points: int
    pcr_bull_high: float
    pcr_bear_low: float
    mp_support_dist: int
    vix_low: float
    vix_high: float
    min_target_points_n: int
    min_target_points_b: int
    min_target_points_f: int
    max_trades_per_day: int
    max_exposure_per_trade: int

    @staticmethod
    def from_env() -> "Params":
        sym = os.getenv("OC_SYMBOL", "NIFTY").strip().upper()
        buf = int(os.getenv(f"ENTRY_BAND_POINTS_{sym}", BUFFERS.get(sym, 12)))
        return Params(
            symbol=sym,
            buffer_points=buf,
            pcr_bull_high=float(os.getenv("PCR_BULL_HIGH", "1.10")),
            pcr_bear_low=float(os.getenv("PCR_BEAR_LOW", "0.90")),
            mp_support_dist=int(os.getenv(f"MP_SUPPORT_DIST_{sym}", "25" if sym=="NIFTY" else ("60" if sym=="BANKNIFTY" else "30"))),
            vix_low=float(os.getenv("VIX_LOW", "12")),
            vix_high=float(os.getenv("VIX_HIGH", "18")),
            min_target_points_n=int(os.getenv("MIN_TARGET_POINTS_N", "20")),
            min_target_points_b=int(os.getenv("MIN_TARGET_POINTS_B", "40")),
            min_target_points_f=int(os.getenv("MIN_TARGET_POINTS_F", "25")),
            max_trades_per_day=int(os.getenv("MAX_TRADES_PER_DAY", "6")),
            max_exposure_per_trade=int(os.getenv("MAX_EXPOSURE_PER_TRADE", "3000")),
        )
