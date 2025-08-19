from utils.logger import log
from utils.cache import get_snapshot


# Trailing from 1:2 is effectively baked into trade_loop via TP price.
# Hook here for future dynamic trailing.


def run_once():
snap = get_snapshot()
if not snap:
return
# Placeholder: extend trailing logic here.
log.debug("TP/SL watcher tick")
