import os, time, requests
from typing import List, Dict, Any, Tuple

SR_BASE = os.getenv("SR_BASE", "https://api.sportradar.com/tennis/trial/v3/en")
SR_API_KEY = os.getenv("SR_API_KEY", "")
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "12"))

# Si ya tienes wrappers propios para Sportradar, cámbialo a True e impórtalos abajo.
HAVE_CUSTOM = False

def _sr_get(path: str, params: dict = None):
    params = params or {}
    if "api_key" not in params:
        params["api_key"] = SR_API_KEY
    headers = {"Accept": "application/json"}
    r = requests.get(f"{SR_BASE}{path}", headers=headers, params=params, timeout=HTTP_TIMEOUT)
    if r.status_code >= 300:
        raise RuntimeError(f"Sportradar GET {path}: {r.status_code} {r.text}")
    return r.json()

def get_profile(competitor_id: str) -> Dict[str, Any]:
    if HAVE_CUSTOM:
        raise NotImplementedError("Enchufa tu wrapper real aquí")
    try:
        return _sr_get(f"/competitors/{competitor_id}/profile.json")
    except Exception:
        return {}

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
