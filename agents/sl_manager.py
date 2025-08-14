
def maybe_trail(side, buy_ltp, sl, cur, params):
    ta = params["exits"]["trail_after_points"]
    step = params["exits"]["trail_step_points"]
    if side == "CE":
        profit = cur - buy_ltp
        if profit >= ta:
            return max(sl, cur - step)
    else:
        profit = buy_ltp - cur
        if profit >= ta:
            return min(sl, cur + step)
    return sl
