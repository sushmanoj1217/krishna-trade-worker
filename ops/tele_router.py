# ops/tele_router.py
from __future__ import annotations
import os, time, threading, requests, json
from typing import Any, Dict, List, Optional
from agents import logger
from analytics import oc_refresh
from ops import eod_perf
from ops.closer import time_exit_all

API = "https://api.telegram.org"

def _bot(): return os.getenv("TELEGRAM_BOT_TOKEN","")
def _uids(): return [x.strip() for x in (os.getenv("TELEGRAM_USER_ID","") or "").split(",") if x.strip()]

def _get(upd_offset):
    try:
        r = requests.get(f"{API}/bot{_bot()}/getUpdates", params={"timeout":25, "offset":upd_offset}, timeout=30)
        j=r.json()
        return j.get("result",[]) if j.get("ok") else []
    except Exception:
        return []

def _send(cid, text):
    try: requests.post(f"{API}/bot{_bot()}/sendMessage", json={"chat_id":cid,"text":text}, timeout=12)
    except Exception: pass

def _auth(uid:int)->bool:
    u=_uids()
    return (not u) or (str(uid) in u)

def _fmt(x): 
    try: return f"{float(x):.2f}"
    except: return str(x)

def _handle_status(sheet, cfg, cid):
    logger.log_status(sheet, {"worker_id":cfg.worker_id,"shift_mode":cfg.shift_mode,"state":"OK","message":f"status {cfg.symbol}"})
    _send(cid, f"OK ✅\nshift={cfg.shift_mode} worker={cfg.worker_id}\nsymbol={cfg.symbol}")

def _handle_oc_now(sheet, cfg, cid):
    oc = None
    try: oc = oc_refresh.get_snapshot(cfg)
    except Exception: oc=None
    if not oc: return _send(cid,"OC snapshot failed ❌")
    logger.log_oc_live(sheet, {
        "ts":"", "symbol": oc.get("symbol") or cfg.symbol, "spot": oc.get("spot"),
        "s1": oc.get("s1"), "s2": oc.get("s2"), "r1": oc.get("r1"), "r2": oc.get("r2"),
        "expiry": oc.get("expiry") or "", "signal":""
    })
    _send(cid, f"OC updated ✅\nspot={_fmt(oc.get('spot'))}  S1={_fmt(oc.get('s1'))}  S2={_fmt(oc.get('s2'))}\nR1={_fmt(oc.get('r1'))}  R2={_fmt(oc.get('r2'))}\nexpiry={oc.get('expiry')}")

def _handle_perf_today(sheet, cfg, cid):
    perf = eod_perf.write_eod(sheet, cfg, cfg.symbol)  # compute (also appends if new day)
    _send(cid, f"Perf today 📊\nTrades={perf['trades']} Wins={perf['wins']} Losses={perf['losses']} WinRate={perf['win_rate']}%\nAvgPnL={perf['avg_pnl']} Net={perf['net_pnl']} MaxDD={perf['max_dd']}")

def _handle_open_trades(sheet, cfg, cid):
    ws = sheet.ss.worksheet("Trades")
    rows = ws.get_all_values()
    if not rows or len(rows) < 2: return _send(cid,"no trades")
    hdr = rows[0]; idx = {h:i for i,h in enumerate(hdr)}
    i_close=idx.get("ts_close",-1); i_sym=idx.get("symbol",-1); i_side=idx.get("side",-1); i_tid=idx.get("trade_id",-1)
    opens=[]
    for r in rows[1:]:
        if i_sym>=0 and len(r)>i_sym and r[i_sym] and r[i_sym]!=cfg.symbol: continue
        if i_close>=0 and len(r)>i_close and r[i_close]: continue
        tid = r[i_tid] if i_tid>=0 and len(r)>i_tid else ""
        side= r[i_side] if i_side>=0 and len(r)>i_side else ""
        if tid: opens.append(f"{tid}:{side}")
    _send(cid, "Open trades:\n" + ("\n".join(opens) if opens else "none"))

def _handle_events_today(sheet, cfg, cid):
    try:
        ws = sheet.ss.worksheet("Events")
        rows = ws.get_all_values()
        today = time.strftime("%Y-%m-%d")
        out=[]
        for r in rows[1:]:
            if len(r)<5: continue
            if str(r[0]).strip()==today and str(r[4]).strip().lower() in ("1","true","on","yes"):
                out.append(f"{r[1]} {r[2]} {r[3]}")
        _send(cid, "Events today:\n"+("\n".join(out) if out else "none"))
    except Exception:
        _send(cid, "Events tab not ready")

def _handle_eod_now(sheet, cfg, cid):
    perf = eod_perf.write_eod(sheet, cfg, cfg.symbol)
    eod_perf.send_daily_summary(perf, cfg)
    _send(cid, "EOD summary sent ✅")

def _handle_close_time_exit(sheet, cfg, cid):
    time_exit_all(sheet, cfg)
    _send(cid, "Time-exit executed ✅")

def start(sheet, cfg):
    if not _bot():
        print("[tele_router] TELEGRAM_BOT_TOKEN missing; router off", flush=True); return
    off=None; print("[tele_router] started polling", flush=True)
    while True:
        ups = _get(off)
        for up in ups:
            off = up.get("update_id", off)
            if off is not None: off += 1
            msg = up.get("message") or up.get("edited_message"); 
            if not msg: continue
            cid = (msg.get("chat") or {}).get("id")
            uid = (msg.get("from") or {}).get("id")
            text = (msg.get("text") or "").strip()
            if not cid or not uid or not text: continue
            if not _auth(uid): _send(cid,"unauthorized"); continue
            t = text.lower()
            if t in ("/status","/start"): _handle_status(sheet,cfg,cid)
            elif t in ("/oc_now","/run oc_now","/run oc_now"): _handle_oc_now(sheet,cfg,cid)
            elif t == "/perf_today": _handle_perf_today(sheet,cfg,cid)
            elif t == "/open_trades": _handle_open_trades(sheet,cfg,cid)
            elif t == "/events_today": _handle_events_today(sheet,cfg,cid)
            elif t == "/eod_now": _handle_eod_now(sheet,cfg,cid)
            elif t == "/close_time_exit": _handle_close_time_exit(sheet,cfg,cid)
            else: _send(cid, "commands: /status, /oc_now, /perf_today, /open_trades, /events_today, /eod_now, /close_time_exit")
        time.sleep(1)
