from datetime import datetime
import random, string


def _rand(n=4):
return ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))


def signal_id() -> str:
return f"SIG-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{_rand()}"


def trade_id() -> str:
return f"TRD-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{_rand()}"
