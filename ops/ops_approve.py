from .ops_learn import _queue

def run(index: int):
    try:
        item = _queue.pop(index)
        return f"approved: {item[:60]}"
    except Exception:
        return "invalid index"
