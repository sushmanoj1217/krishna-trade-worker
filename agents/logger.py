# path: agents/logger.py
from datetime import datetime
from tzlocal import get_localzone
import os, time

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
    # NEW: persistence tabs
    sheet.ensure_headers("Params_Override", ["ts","date","symbol","json"])
    sheet.ensure_headers("Snapshots", ["date","symbol","total_trades","sum_pnl","best_trade_id","best_pnl","worst_trade_id","worst_pnl","json"])

def _today():
    return datetime.now(get_localzone()).strftime("%Y-%m-%d")

def _append_retry(sheet, tab, row, attempts:int=3, sleep_s:float=1.0):
    for i in range(attempts):
        try:
            sheet.append(tab, row); return True
        except Exception as e:
            if i == attempts-1: print(f"[sheet] append fail {tab}: {e}"); return False
            time.sleep(sleep_s)
    return False

# ... (rest of file unchanged — your existing logging functions) ...
