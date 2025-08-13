from datetime import datetime
from tzlocal import get_localzone

def ensure_all_headers(sheet, cfg):
    sheet.ensure_headers(cfg.sheet.get("levels_tab","OC_Live"), ["spot","s1","s2","r1","r2","expiry","signal","ts"])
    sheet.ensure_headers(cfg.sheet.get("signals_tab","Signals"), ["signal_id","ts","symbol","side(CE|PE)","reason","level_hit(S1/S2/R1/R2)","spot","sl_pts","tp_pts","strat_ver","worker_id"])
    sheet.ensure_headers(cfg.sheet.get("trades_tab","Trades"), ["trade_id","ts_buy","symbol","side","buy_ltp","qty","sl","tp","ts_exit","exit_ltp","pnl","reason_buy","reason_exit","strat_ver","status","worker_id"])
    sheet.ensure_headers(cfg.sheet.get("perf_tab","Performance"), ["date","strat_ver","win_rate","avg_pnl","total_trades","max_dd","daily_pnl","notes","worker_id"])
    sheet.ensure_headers(cfg.sheet.get("events_tab","Events"), ["date","time_ist","name","severity","effect","window_start","window_end"])
    sheet.ensure_headers(cfg.sheet.get("status_tab","Status"), ["ts","worker_id","shift_mode","state","message"])

def log_signal(sheet, cfg, sig, params, worker_id):
    tab = cfg.sheet.get("signals_tab","Signals")
    row = {
        "signal_id": f"{sig.get('level_hit','LVL')}_{sig['side']}_{sig['spot']}",
        "ts": datetime.now(get_localzone()).isoformat(),
        "symbol": cfg.symbol,
        "side(CE|PE)": sig["side"],
        "reason": sig.get("reason",""),
        "level_hit(S1/S2/R1/R2)": sig.get("level_hit",""),
        "spot": sig.get("spot",""),
        "sl_pts": 0,
        "tp_pts": 0,
        "strat_ver": "v1",
        "worker_id": worker_id
    }
    sheet.append(tab, row)

def log_trade_open(sheet, trade):
    rows = sheet.read_all("Trades")
    tid = trade["trade_id"]
    if any(r.get("trade_id")==tid for r in rows):
        return False
    out = dict(trade)
    out.update({"ts_exit":"","exit_ltp":"","pnl":"","reason_exit":"","status":"OPEN"})
    sheet.append("Trades", out)
    return True

def log_trade_update(sheet, trade_id, updates):
    log_status(sheet, {"message": f"trade {trade_id} update {updates}"})

def log_trade_close(sheet, trade_id, exit_ltp, reason_exit, pnl):
    ts = datetime.now(get_localzone()).isoformat()
    sheet.append("Trades", {"trade_id":trade_id, "ts_buy":"", "symbol":"", "side":"", "buy_ltp":"", "qty":"", "sl":"", "tp":"", "ts_exit":ts, "exit_ltp":exit_ltp, "pnl":pnl, "reason_buy":"", "reason_exit":reason_exit, "strat_ver":"", "status":"CLOSED", "worker_id":""})
    log_status(sheet, {"message": f"trade {trade_id} closed {reason_exit} pnl={pnl}"})

def log_status(sheet, status):
    base = {"ts": datetime.now(get_localzone()).isoformat(), "worker_id":"", "shift_mode":"", "state":"OK", "message":""}
    base.update(status or {})
    sheet.append("Status", base)
