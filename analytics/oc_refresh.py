# path: analytics/oc_refresh.py
import os, json, time, random
from datetime import datetime
from tzlocal import get_localzone
from integrations.dhan import DhanClient, _sym_norm

try:
    from integrations import telegram
except Exception:
    telegram = None  # optional

LEVELS_PATH = "data/levels.json"

_LAST_CALL_TS: dict[str, float] = {}
_EXPIRY_CACHE: dict[str, dict] = {}
_LAST_429_ALERT_TS: float = 0.0

def _now_iso(): return datetime.now(get_localzone()).isoformat()

def _parse_symbols() -> list[str]:
    raw = os.getenv("OC_SYMBOL", os.getenv("OC_SYMBOL_PRIMARY", "NIFTY"))
    parts = [p.strip() for p in (raw or "").split(",") if p.strip()]
    return [_sym_norm(p) for p in parts] or ["NIFTY"]

def _choose_primary(symbols: list[str]) -> str:
    prim = os.getenv("OC_SYMBOL_PRIMARY", "").strip()
    return _sym_norm(prim) if prim else symbols[0]

def _compute_levels_from_oc(oc_json: dict) -> dict:
    d = oc_json.get("data") or {}
    spot = d.get("last_price")
    chain = d.get("oc") or {}
    supports, resists = [], []
    if spot is None or not chain:
        return {"spot": spot, "s1": None, "s2": None, "r1": None, "r2": None}
    for k, v in chain.items():
        try:
            strike = float(k)
        except:
            continue
        ce = (v or {}).get("ce") or {}
        pe = (v or {}).get("pe") or {}
        ce_oi = float(ce.get("oi") or 0); pe_oi = float(pe.get("oi") or 0)
        if strike <= spot: supports.append((strike, pe_oi))
        if strike >= spot: resists.append((strike, ce_oi))
    supports.sort(key=lambda x: (x[1], x[0]), reverse=True)
    resists.sort(key=lambda x: (x[1], -x[0]), reverse=True)
    s1 = supports[0][0] if len(supports) > 0 else None
    s2 = supports[1][0] if len(supports) > 1 else None
    r1 = resists[0][0]  if len(resists)  > 0 else None
    r2 = resists[1][0]  if len(resists)  > 1 else None
    return {"spot": spot, "s1": s1, "s2": s2, "r1": r1, "r2": r2}

def _expiry_list(client: DhanClient, sym: str, usid: int) -> list[str]:
    ttl = int(os.getenv("EXPIRY_TTL_SECS", "300") or "300")
    ent = _EXPIRY_CACHE.get(sym); now = time.time()
    if ent and (now - ent.get("ts", 0)) < ttl and ent.get("list"):
        return ent["list"]
    lst = client.get_expiries(usid)
    _EXPIRY_CACHE[sym] = {"ts": now, "list": lst}
    return lst

def update_levels_from_dhan(bus):
    global _LAST_429_ALERT_TS
    client   = DhanClient()
    symbols  = _parse_symbols()
    primary  = _choose_primary(symbols)

    fetch_all = os.getenv("OC_FETCH_ALL", "off").lower() == "on"
    if not fetch_all:
        symbols = [primary]

    min_interval = int(os.getenv("OC_MIN_INTERVAL_SECS", "15") or "15")
    jitter_hi = int(os.getenv("OC_JITTER_SECS", "3") or "3")

    levels_all = {}
    now_ts = time.time()

    for sym in symbols:
        last = _LAST_CALL_TS.get(sym, 0.0)
        wait_left = min_interval - (now_ts - last)
        if wait_left > 0:
            print(f"[oc] throttle {sym}: wait {wait_left:.1f}s")
            continue

        try:
            usid = client.resolve_underlying_scrip(sym)
            if not usid:
                print(f"[oc] resolve fail: {sym}")
                continue

            exps = _expiry_list(client, sym, usid)
            if not exps:
                print(f"[oc] no expiries: {sym}")
                continue

            expiry = exps[0]
            oc = client.get_option_chain(usid, expiry)
            lv = _compute_levels_from_oc(oc)
            lv.update({"ts": _now_iso(), "expiry": expiry, "symbol": sym})
            levels_all[sym] = lv
            bus.emit("levels", lv)
            print(f"[oc] {sym} spot={lv.get('spot')} s1={lv.get('s1')} r1={lv.get('r1')}")
            _LAST_CALL_TS[sym] = time.time() + random.randint(0, jitter_hi)

        except Exception as e:
            emsg = str(e)
            print(f"[oc] error {sym}: {emsg}")
            # Telegram alert for 429 (once in 5m)
            if "429" in emsg and os.getenv("ALERT_429", "on").lower() == "on" and telegram:
                now = time.time()
                if now - _LAST_429_ALERT_TS > 300:
                    telegram.send(f"⚠️ 429 on OC for {sym}. Auto-throttle active.")
                    _LAST_429_ALERT_TS = now

    try:
        with open(LEVELS_PATH, "w", encoding="utf-8") as f:
            json.dump({"symbols": levels_all, "ts": _now_iso(), "primary": primary}, f, indent=2)
    except Exception as e:
        print("[oc] levels.json write error:", e)

def oc_refresh_tick(bus):
    mode = os.getenv("OC_MODE", "dhan").lower()
    if mode == "dhan":
        update_levels_from_dhan(bus)
    else:
        try:
            from analytics.oc_refresh_sheet import update_levels_from_sheet
            update_levels_from_sheet(bus)
        except Exception as e:
            print("[oc] sheet helper missing:", e)
