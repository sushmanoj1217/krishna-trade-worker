
from typing import List, Dict, Any

def compute_metrics(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not trades:
        return {"win_rate":0.0,"avg_pnl":0.0,"total_trades":0,"max_dd":0.0,"daily_pnl":0.0}
    wins = sum(1 for t in trades if float(t.get("pnl",0))>0)
    total = len(trades)
    pnl_list = [float(t.get("pnl",0)) for t in trades]
    avg_pnl = sum(pnl_list)/total if total else 0.0
    cum=0.0; peak=0.0; max_dd=0.0
    for p in pnl_list:
        cum += p; peak = max(peak, cum); dd = peak - cum; max_dd = max(max_dd, dd)
    return {"win_rate": wins/total if total else 0.0, "avg_pnl": avg_pnl, "total_trades": total, "max_dd": max_dd, "daily_pnl": sum(pnl_list)}

def eod(sheet, cfg, worker_id: str, date_str: str):
    tab = cfg.sheet.get("trades_tab","Trades")
    allrows = sheet.read_all(tab)
    trades = [r for r in allrows if str(r.get("ts_buy","")).startswith(date_str) and r.get("worker_id","")==worker_id and r.get("status","CLOSED")=="CLOSED"]
    metrics = compute_metrics(trades)
    out = {"date": date_str, "strat_ver": "-", **metrics, "notes":"", "worker_id": worker_id}
    pt = cfg.sheet.get("perf_tab","Performance")
    sheet.ensure_headers(pt, ["date","strat_ver","win_rate","avg_pnl","total_trades","max_dd","daily_pnl","notes","worker_id"])
    sheet.append(pt, out)
    print("[perf] EOD metrics", out)
