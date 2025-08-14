
import json, yaml
from dataclasses import dataclass
from typing import Any

@dataclass
class Settings:
    symbol: str
    rr_target: int
    oc_refresh_secs_day: int
    oc_refresh_secs_night: int
    day_shift: dict
    night_shift: dict
    sheet: dict
    max_exposure_per_trade: int
    daily_loss_limit: int
    max_trades_per_day: int

def load_settings(path: str = "config/settings.yaml") -> 'Settings':
    with open(path, "r", encoding="utf-8") as f:
        y = yaml.safe_load(f)
    return Settings(
        symbol=y.get("symbol", "NIFTY"),
        rr_target=y.get("rr_target", 3),
        oc_refresh_secs_day=y.get("oc_refresh_secs_day", 10),
        oc_refresh_secs_night=y.get("oc_refresh_secs_night", 60),
        day_shift=y.get("day_shift", {}),
        night_shift=y.get("night_shift", {}),
        sheet=y.get("sheet", {}),
        max_exposure_per_trade=y.get("max_exposure_per_trade", 3000),
        daily_loss_limit=y.get("daily_loss_limit", 6000),
        max_trades_per_day=y.get("max_trades_per_day", 6),
    )

def load_strategy_params(path: str = "config/strategy_params.json") -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
