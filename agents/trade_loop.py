from dataclasses import dataclass
from typing import Dict
from utils.logger import log
from utils.ids import trade_id
from utils.params import Params
from utils.cache import get_snapshot
from integrations import sheets as sh


_open: Dict[str, "Trade"] = {}


@dataclass
class Trade:
id: str
signal_id: str
side: str
entry: float
sl: float
tp: float
qty: int
status: str # OPEN/CLOSED




def place_trade(signal):
p = Params()
t = Trade(
id=trade_id(), signal_id=signal.id, side=signal.side,
entry=signal.basis["entry"], sl=signal.basis["sl"], tp=signal.basis["tp"],
qty=p.qty_per_trade, status="OPEN"
)
_open[t.id] = t
try:
sh.append_row("Trades", [t.id, t.signal_id, t.side, t.entry, t.sl, t.tp, t.qty, "OPEN"])
except Exception as e:
log.error(f"Trades append failed: {e}")
log.info(f"Trade placed {t}")




def manage_open_trades():
# Paper logic uses spot as LTP proxy
snap = get_snapshot()
if not snap:
return
to_close = []
for tid, t in _open.items():
if t.status != "OPEN":
continue
ltp = snap.spot
if t.side == "CE":
if ltp <= t.sl or ltp >= t.tp:
to_close.append((tid, ltp))
else: # PE
if ltp >= t.sl or ltp <= t.tp:
to_close.append((tid, ltp))
del _open[tid]
