from utils.params import Params

def rr_feasible(side: str, entry: float, sl: float, p: Params) -> bool:
    # simple: requires >= 2x SL distance & min points
    if side == "CE":
        min_pts = p.min_target_points_n if p.symbol=="NIFTY" else (p.min_target_points_b if p.symbol=="BANKNIFTY" else p.min_target_points_f)
        tgt = entry + 2*(entry - sl)
        return (tgt - entry) >= min_pts
    else:
        min_pts = p.min_target_points_n if p.symbol=="NIFTY" else (p.min_target_points_b if p.symbol=="BANKNIFTY" else p.min_target_points_f)
        tgt = entry - 2*(sl - entry)
        return (entry - tgt) >= min_pts
