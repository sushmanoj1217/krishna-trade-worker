
from typing import Callable, Dict, List, Any

class Bus:
    def __init__(self):
        self._subs: Dict[str, List[Callable[[Any], None]]] = {}

    def on(self, event: str, fn: Callable[[Any], None]):
        self._subs.setdefault(event, []).append(fn)

    def emit(self, event: str, data):
        for fn in self._subs.get(event, []):
            try:
                fn(data)
            except Exception as e:
                print(f"[bus] handler error for {event}: {e}")
