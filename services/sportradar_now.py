import os, re, urllib.parse, requests, logging
log = logging.getLogger("sportradar_now")
SR_API_KEY = os.getenv("SR_API_KEY", "")
SR_BASE = "https://api.sportradar.com/tennis/trial/v3/en"

def _sr_url(path, params=None):
    params = dict(params or {})
    params["api_key"] = SR_API_KEY or "REPLACE_ME"
    return f"{SR_BASE}/{path}?{urllib.parse.urlencode(params)}"

def _get(path, params=None, timeout=15):
    url = _sr_url(path, params)
    red = re.sub(r"api_key=[^&]+", "api_key=***", url)
    log.info("SR GET %s", red)
    r = requests.get(url, timeout=timeout, headers={"accept":"application/json"})
    log.info("SR RESP %s (ratelimit-remaining=%s)", r.status_code, r.headers.get("x-ratelimit-remaining"))
    return r

def get_profile(sr_id):
    if not sr_id: return {}
    sid = sr_id if str(sr_id).startswith("sr:") else f"sr:competitor:{sr_id}"
    r = _get(f"competitors/{sid}/profile.json")
    return r.json() if r.ok else {}

def get_last10(competitor_id: str) -> List[Dict[str, Any]]:
    if HAVE_CUSTOM:
        raise NotImplementedError("Enchufa tu wrapper real aquí")
    # Devuelve [{winner: bool, date: epoch, surface: str}, ...]
    return []

def get_ytd_record(competitor_id: str) -> Dict[str, int]:
    if HAVE_CUSTOM:
        raise NotImplementedError("Enchufa tu wrapper real aquí")
    # Devuelve {wins: int, losses: int}
    return {"wins": 0, "losses": 0}

def get_h2h(p_id: str, o_id: str) -> Tuple[int, int]:
    if HAVE_CUSTOM:
        raise NotImplementedError("Enchufa tu wrapper real aquí")
    # Devuelve (wins_p, wins_o)
    return (0, 0)

def compute_now_features(profile: dict, last10_matches: list, ytd_record: dict):
    ranking_now = None
    try:
        ranking_now = profile.get("competitor", {}).get("rankings", [{}])[0].get("rank", None)
    except Exception:
        pass

    w = sum(1 for m in last10_matches if m.get("winner") is True)
    n = len(last10_matches) or 1
    winrate_last10 = w / n

    wy = ytd_record.get("wins", 0)
    ly = ytd_record.get("losses", 0)
    winrate_ytd = wy / max(1, (wy + ly))

    days_inactive = 0.0
    last_surface = None
    try:
        last = next((m for m in last10_matches if "date" in m), None)
        if last and last.get("date"):
            ts = last["date"]
            if isinstance(ts, (int, float)):
                days_inactive = max(0.0, (time.time() - ts) / 86400.0)
        last_surface = (last.get("surface") or "").lower() if last else None
    except Exception:
        pass

    return dict(
        winrate_last10=winrate_last10,
        winrate_ytd=winrate_ytd,
        ranking_now=ranking_now,
        days_inactive=days_inactive,
        last_surface=last_surface
    )
