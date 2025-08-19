from utils.logger import log
from utils.cache import get_snapshot

def run_once():
    snap = get_snapshot()
    if not snap:
        return
    # Placeholder for advanced trailing; base TP logic in trade_loop.
    log.debug("TP/SL watcher tick")
