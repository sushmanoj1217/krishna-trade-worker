# path: agents/logger.py
from datetime import datetime
from tzlocal import get_localzone
import os

def ensure_all_headers(sheet, cfg):
    sheet.ensure_headers(cfg.sheet.get("levels_tab","OC_Live"),
        ["spot","s1","s2","r1","r2","expiry","signal","ts","symbol"])
    sheet.ensure_headers(cfg.sheet.get("signals_tab","Signals"),
        ["signal_id","ts","symbol","side(CE|PE)","reason","level_hit(S1/S2/R1/R2)","spot","sl_pts","tp_pts","strat_ver","worker_id"])
    sheet.ensure_headers(cfg.sheet.get("trades_tab","Trades"),
        ["trade_id","ts_buy","symbol","side","buy_ltp","qty","sl","tp","ts_exit","exit_ltp","pnl","reason_buy","reason_exit","strat_ver","status","worker_id"])
    sheet.ensure_headers(cfg.sheet.get("perf_tab","Performance"),
        ["date","strat_ver","win_rate","avg_pnl","total_trades","max_dd","daily_pnl","notes","worker_id"])
    sheet.ensure_headers(cfg.sheet.get("events_tab","Events"),
        ["date","time_ist","name","severity","effect","window_start","window_end"])
    sheet.ensure_headers(cfg.sheet.get("status_tab","Status"),
        ["ts","worker_id","shift_mode","state","message"])

def _today():
    return datetime.now(get_localzone()).strftime("%Y-%m-%d")

def log_signal(sheet, cfg, sig, params, worker_id: str):
    tab = cfg.sheet.get("signals_tab","Signals")
    sid = f"{sig.get('level_hit','LVL')}_{sig['side']}_{sig.get('symbol',cfg.symbol)}_{sig['spot']}"
    # de-dup: skip if already logged today with same id & worker
    try:
        rows = sheet.read_all(tab)
        last_n = int(os.getenv("SIGNAL_DEDUP_LAST_N", "100"))
        recent = rows[-last_n:] if rows else []
        for r in reversed(recent):
            if r.get("signal_id")==sid and r.get("worker_id","")==worker_id and str(r.get("ts","")).startswith(_today()):
                return  # skip duplicate
    except Exception:
        pass

    row = {
        "signal_id": sid,
        "ts": datetime.now(get_localzone()).isoformat(),
        "symbol": sig.get("symbol", cfg.symbol),
        "side(CE|PE)": sig["side"],
        "reason": sig.get("reason",""),
        "level_hit(S1/S2/R1/R2)": sig.get("level_hit",""),
        "spot": sig.get("spot",""),
        "sl_pts": params["exits"]["initial_sl_points"],
        "tp_pts": params["exits"]["initial_sl_points"]*params["exits"]["target_rr"],
        "strat_ver": params.get("name","v1"),
        "worker_id": worker_id
    }
    sheet.append(tab, row)

def log_trade_open(sheet, trade) -> bool:
    tab = "Trades"
    rows = sheet.read_all(tab)
    tid = trade["trade_id"]
    if any(r.get("trade_id")==tid for r in rows):
        return False
    out = dict(trade); out.update({"ts_exit":"","exit_ltp":"","pnl":"","reason_exit":"","status":"OPEN"})
    sheet.append(tab, out); return True

def log_trade_update(sheet, trade_id: str, updates):
    log_status(sheet, {"message": f"trade {trade_id} update {updates}"})

def log_trade_close(sheet, trade_id: str, exit_ltp: float, reason_exit: str, pnl: float):
    log_status(sheet, {"message": f"trade {trade_id} closed {reason_exit} pnl={pnl}"})

def log_status(sheet, status):
    tab = "Status"
    base = {"ts": datetime.now(get_localzone()).isoformat(),"worker_id": "","shift_mode":"","state":"OK","message":""}
    base.update(status or {}); sheet.append(tab, base)
