# agents/trade_executor.py
import os, uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Literal
from feeds.option_price import get_ltp
from agents import logger

Side = Literal["CE", "PE"]

NOT_FILLED_TIMEOUT_SECS = int(os.getenv("NOT_FILLED_TIMEOUT_SECS", "120"))
RR_RISK_POINTS_DEFAULT = float(os.getenv("RR_RISK_POINTS", "10"))
QTY_PER_TRADE = int(os.getenv("QTY_PER_TRADE", "25"))

class TradeExecutor:
    def __init__(self):
        self.pending: Dict[str, Dict] = {}
        self.open: Dict[str, Dict] = {}

    # ---------- Public API ----------
    def place_limit(self, signal: Dict) -> Dict:
        """
        signal: {
          'symbol','side' ('CE'|'PE'),'trigger_level','level_tag'('S1*'|...),
          'target_rr':2, 'trail_after_rr':2, 'mv','reason', ...
        }
        """
        tid = str(uuid.uuid4())[:8]
        now = datetime.now().isoformat(timespec="seconds")
        risk_points = float(signal.get("risk_points") or RR_RISK_POINTS_DEFAULT)
        trade = {
            "id": tid,
            "symbol": signal.get("symbol", "NIFTY"),
            "side": signal["side"],
            "state": "PENDING",
            "created_at": now,
            "updated_at": now,
            "entry_trigger": float(signal["trigger_level"]),
            "level_tag": signal.get("level_tag"),
            "not_filled_timeout_secs": NOT_FILLED_TIMEOUT_SECS,
            "expires_at": (datetime.now() + timedelta(seconds=NOT_FILLED_TIMEOUT_SECS)).isoformat(timespec="seconds"),
            "qty": QTY_PER_TRADE,
            "risk_points": risk_points,
            "trail_started": False,
            "trail_stop": None,
            "fill_spot": None,
            "fill_opt_price": None,
            "last_ltp": None,
            "pnl_points": 0.0,
            "pnl_value": 0.0,
            "exit_reason": None,
        }
        self.pending[tid] = trade
        logger.log_trade_open(trade)  # state=PENDING row
        return trade

    def on_oc_tick(self, snapshot: Dict):
        """
        snapshot: {'spot': float, 'ts': iso8601, 'S1*':float,'S2*':float,'R1*':float,'R2*':float}
        Called every OC refresh.
        """
        ts = snapshot.get("ts") or datetime.now().isoformat(timespec="seconds")
        spot = float(snapshot["spot"])

        # 1) Try fill pending by trigger-cross
        to_fill = []
        to_cancel = []
        now = datetime.now()
        for tid, tr in list(self.pending.items()):
            trig = tr["entry_trigger"]
            expired = now > datetime.fromisoformat(tr["expires_at"])
            if expired:
                to_cancel.append((tid, tr, "CANCELLED_NOT_FILLED"))
                continue
            if tr["side"] == "CE" and spot <= trig:
                to_fill.append((tid, tr))
            elif tr["side"] == "PE" and spot >= trig:
                to_fill.append((tid, tr))

        for tid, tr in to_fill:
            tr["state"] = "OPEN"
            tr["updated_at"] = ts
            tr["fill_spot"] = spot
            tr["fill_opt_price"] = get_ltp(tr, spot)  # may be synthetic
            self.open[tid] = tr
            self.pending.pop(tid, None)
            logger.log_trade_update(tr)  # state=OPEN row update/append

        for tid, tr, reason in to_cancel:
            tr["state"] = "CANCELLED"
            tr["exit_reason"] = reason
            tr["updated_at"] = ts
            logger.log_trade_close(tr)
            self.pending.pop(tid, None)

        # 2) Update PnL & trailing for open trades
        for tid, tr in list(self.open.items()):
            ltp = get_ltp(tr, spot)
            if ltp is None:
                continue
            tr["last_ltp"] = ltp
            entry = float(tr["fill_opt_price"] or 0.0)

            # points PnL
            if tr["side"] == "CE":
                pnl_pts = ltp - entry
            else:
                pnl_pts = entry - ltp

            tr["pnl_points"] = float(round(pnl_pts, 2))
            tr["pnl_value"] = float(round(pnl_pts * tr["qty"], 2))

            # 1:2 trailing activation
            risk = float(tr.get("risk_points") or RR_RISK_POINTS_DEFAULT)
            if not tr["trail_started"] and pnl_pts >= (2.0 * risk):
                tr["trail_started"] = True
                tr["trail_stop"] = entry + (risk if tr["side"] == "CE" else -risk)

            # trail maintenance
            if tr["trail_started"]:
                if tr["side"] == "CE":
                    # raise stop only
                    tr["trail_stop"] = max(tr["trail_stop"], ltp - risk)
                    # stop-out check
                    if ltp <= tr["trail_stop"]:
                        tr["state"] = "CLOSED"
                        tr["exit_reason"] = "TRAIL_STOP"
                        tr["updated_at"] = ts
                        logger.log_trade_close(tr)
                        self.open.pop(tid, None)
                        continue
                else:  # PE
                    tr["trail_stop"] = min(tr["trail_stop"], ltp + risk)
                    if ltp >= tr["trail_stop"]:
                        tr["state"] = "CLOSED"
                        tr["exit_reason"] = "TRAIL_STOP"
                        tr["updated_at"] = ts
                        logger.log_trade_close(tr)
                        self.open.pop(tid, None)
                        continue

            logger.log_trade_update(tr)

    def close_all(self, reason: str):
        ts = datetime.now().isoformat(timespec="seconds")
        # cancel pendings
        for tid, tr in list(self.pending.items()):
            tr["state"] = "CANCELLED"
            tr["exit_reason"] = reason
            tr["updated_at"] = ts
            logger.log_trade_close(tr)
            self.pending.pop(tid, None)
        # close opens at last LTP
        for tid, tr in list(self.open.items()):
            tr["state"] = "CLOSED"
            tr["exit_reason"] = reason
            tr["updated_at"] = ts
            logger.log_trade_close(tr)
            self.open.pop(tid, None)
