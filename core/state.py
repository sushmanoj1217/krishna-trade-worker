from dataclasses import dataclass, field
from typing import Dict, Any, Optional
from datetime import datetime

@dataclass
class AppState:
    last_levels: Dict[str, Any] = field(default_factory=dict)
    last_signal_ts: Dict[str, float] = field(default_factory=dict)
    open_trades: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    daily_pnl: float = 0.0
    daily_trades: int = 0
    day_date: Optional[str] = None

    def reset_if_new_day(self, today_str: str):
        if self.day_date != today_str:
            self.day_date = today_str
            self.daily_pnl = 0.0
            self.daily_trades = 0
            self.open_trades.clear()
            self.last_signal_ts.clear()
