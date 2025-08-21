from utils.logger import log
from integrations import sheets as sh

async def trail_tick() -> bool:
    """
    Trailing watcher: for paper mode we just scan open trades & do nothing.
    Return True if any trade was modified/closed.
    """
    try:
        _ = await sh.get_open_trades()
        return False
    except Exception as e:
        log.error(f"trail_tick watcher failed: {e}")
        return False
