import math

WEIGHTS = {
    "rank_norm": 1.6,
    "hist_surface": 1.2,
    "hist_speed": 0.8,
    "hist_month": 0.6,
    "ytd": 1.2,
    "last10": 1.0,
    "h2h": 0.6,
    "inactive": 0.5,
}

ADJUSTS = {
    "surf_change": -0.05,
    "local": 0.03,
    "mot_points": 0.05,
}

def logistic(x: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0

def clamp(x, lo, hi):
    return max(lo, min(hi, x))
