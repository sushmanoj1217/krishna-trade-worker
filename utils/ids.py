import random
import string
from datetime import datetime, timezone

def _rand4() -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=4))

def new_signal_id(now=None) -> str:
    now = now or datetime.now(timezone.utc)
    return f"SIG-{now.strftime('%Y%m%d-%H%M%S')}-{_rand4()}"

def new_trade_id(now=None) -> str:
    now = now or datetime.now(timezone.utc)
    return f"TRD-{now.strftime('%Y%m%d-%H%M%S')}-{_rand4()}"
