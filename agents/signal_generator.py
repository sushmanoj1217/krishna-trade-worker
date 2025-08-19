from dataclasses import dataclass


for side, trig, lvl in candidates:
crossed = near_cross(trig, lvl)
if not crossed:
continue
# 6-Checks
c1 = True # TriggerCross by design
# C2 FlowBias@Trigger: simple proxy using bias_tag
c2 = (side == "CE" and (snap.bias_tag or "").startswith("mv_bull")) or \
(side == "PE" and (snap.bias_tag or "").startswith("mv_bear"))
# C3 WallSupport(ΣΔOI): assume True if we have S/R
c3 = True if lvl else False
# C4 Momentum(3–5m): not available → assume True (hook later)
c4 = True
# C5 RR feasible
sl = lvl - snap.extras.get("buffer", 12) if side == "CE" else lvl + snap.extras.get("buffer", 12)
rr_ok, risk, tp = rr_feasible(lvl, sl, p.min_target_points())
c5 = rr_ok
# C6 SystemGates
c6 = not is_no_trade_now()


all_ok = all([c1, c2, c3, c4, c5, c6])


sig_hash = _hash(side, trig, lvl)
if sig_hash in _seen_hashes:
log.info(f"Duplicate signal blocked {sig_hash}")
continue


s = Signal(
id=signal_id(), side=side, trigger=trig, eligible=all_ok,
reason=";".join([
f"C1={c1}", f"C2={c2}", f"C3={c3}", f"C4={c4}", f"C5={c5}", f"C6={c6}"
]),
basis={"entry": lvl, "sl": sl, "tp": tp, "risk": risk}
)


# Log in Sheets
try:
sh.append_row("Signals", [s.id, s.side, s.trigger, lvl, c1, c2, c3, c4, c5, c6, s.eligible, s.reason])
except Exception as e:
log.error(f"Signals append failed: {e}")


_seen_hashes.add(sig_hash)
return s


return None
