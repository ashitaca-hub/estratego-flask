# services/sportradar_now.py
from __future__ import annotations
import os
import re
import time
import urllib.parse
import requests
import logging
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timezone

log = logging.getLogger("sportradar_now")

# Config
SR_API_KEY = os.getenv("SR_API_KEY", "").strip()
SR_BASE = "https://api.sportradar.com/tennis/trial/v3/en"  # ajusta si usas otro plan/locale

# --------------------------- Utils internas ---------------------------

def _normalize_sr(sr_id: str | int | None) -> Optional[str]:
    if not sr_id:
        return None
    s = str(sr_id)
    return s if s.startswith("sr:") else f"sr:competitor:{s}"

def _sr_url(path: str, params: dict | None = None) -> str:
    params = dict(params or {})
    params["api_key"] = SR_API_KEY or "REPLACE_ME"
    return f"{SR_BASE}/{path}?{urllib.parse.urlencode(params)}"

def _get(path: str, params: dict | None = None, timeout: int = 15) -> requests.Response:
    url = _sr_url(path, params)
    red = re.sub(r"api_key=[^&]+", "api_key=***", url)  # no logeamos la clave
    log.info("SR GET %s", red)
    r = requests.get(url, timeout=timeout, headers={"accept": "application/json"})
    log.info("SR RESP %s (ratelimit-remaining=%s)", r.status_code, r.headers.get("x-ratelimit-remaining"))
    return r

def _parse_iso_to_epoch(ts: str | None) -> Optional[float]:
    if not ts:
        return None
    try:
        # Ej.: "2025-08-10T18:00:00+00:00" o con 'Z'
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.timestamp()
    except Exception:
        try:
            # fallback: quitar microsegundos si vinieran
            base = ts.split(".")[0]
            dt = datetime.fromisoformat(base.replace("Z", "+00:00"))
            return dt.replace(tzinfo=timezone.utc).timestamp()
        except Exception:
            return None

# --------------------------- API pública ---------------------------

def get_profile(sr_id: str | int | None) -> dict:
    """
    Perfil del competidor. Devuelve {} si la petición no es OK.
    """
    sid = _normalize_sr(sr_id)
    if not sid:
        return {}
    r = _get(f"competitors/{sid}/profile.json")
    return r.json() if r.ok else {}

def get_last10(competitor_id: str | int | None) -> List[Dict[str, Any]]:
    """
    Últimos 10 partidos del competidor (orden natural de SR).
    Devuelve una lista de dicts: [{winner: bool, date: epoch, surface: str}, ...]
    """
    sid = _normalize_sr(competitor_id)
    if not sid:
        return []
    r = _get(f"competitors/{sid}/summaries.json")
    if not r.ok:
        return []
    data = r.json()
    out: List[Dict[str, Any]] = []
    for s in (data.get("summaries") or [])[:10]:
        ev = s.get("sport_event", {}) or {}
        st = s.get("sport_event_status", {}) or {}

        # ganador
        wid = (st.get("winner_id") or "").lower()
        winner = True if wid and wid == str(sid).lower() else False

        # fecha
        start_time = ev.get("start_time")
        epoch = _parse_iso_to_epoch(start_time)

        # superficie
        surface = (ev.get("sport_event_context", {}) or {}).get("surface", {}) or {}
        surf_name = (surface.get("name") or "").lower() or None

        out.append({"winner": winner, "date": epoch, "surface": surf_name})
    return out

def get_ytd_record(competitor_id: str | int | None) -> Dict[str, int]:
    """
    Balance YTD (año en curso) aproximado sumando periodos/surfaces del perfil.
    Devuelve {"wins": int, "losses": int}.
    """
    sid = _normalize_sr(competitor_id)
    if not sid:
        return {"wins": 0, "losses": 0}

    r = _get(f"competitors/{sid}/profile.json")
    if not r.ok:
        return {"wins": 0, "losses": 0}
    prof = r.json()

    year_now = datetime.now(timezone.utc).year
    wins = 0
    played = 0

    for per in prof.get("periods", []) or []:
        if per.get("year") == year_now:
            for surf in per.get("surfaces", []) or []:
                stats = surf.get("statistics", {}) or {}
                w = int(stats.get("matches_won", 0) or 0)
                p = int(stats.get("matches_played", 0) or 0)
                wins += w
                played += p

    losses = max(0, played - wins)
    return {"wins": wins, "losses": losses}

def get_h2h(p_id: str | int | None, o_id: str | int | None) -> Tuple[int, int]:
    """
    Caras: (wins_p, wins_o) a partir de last_meetings en SR.
    """
    ps = _normalize_sr(p_id)
    os_ = _normalize_sr(o_id)
    if not ps or not os_:
        return (0, 0)
    r = _get(f"competitors/{ps}/versus/{os_}/summaries.json")
    if not r.ok:
        return (0, 0)
    data = r.json()
    wins_p = 0
    wins_o = 0

    # Preferimos recorrer last_meetings para no depender de estructuras agregadas
    for m in (data.get("last_meetings") or []):
        wid = (m.get("sport_event_status", {}) or {}).get("winner_id")
        if not wid:
            continue
        wid = str(wid).lower()
        if wid == str(ps).lower():
            wins_p += 1
        elif wid == str(os_).lower():
            wins_o += 1

    return (wins_p, wins_o)

def compute_now_features(profile: dict, last10_matches: list, ytd_record: dict) -> Dict[str, Any]:
    """
    Calcula señales “NOW” a partir de datos crudos:
      - ranking_now: int|None
      - winrate_last10: [0..1]
      - winrate_ytd:    [0..1]
      - days_inactive:  días desde la última fecha con timestamp
      - last_surface:   str|None (en minúsculas)
    """
    # ranking actual (distintos esquemas en SR según feed)
    ranking_now = None
    try:
        ranking_now = (
            profile.get("competitor_rankings", [{}])[0].get("rank", None)
            if "competitor_rankings" in profile
            else profile.get("competitor", {}).get("rankings", [{}])[0].get("rank", None)
        )
    except Exception:
        ranking_now = None

    # last10
    try:
        w = sum(1 for m in last10_matches if m.get("winner") is True)
        n = max(1, len(last10_matches))
        winrate_last10 = w / n
    except Exception:
        winrate_last10 = 0.0

    # ytd
    try:
        wy = int(ytd_record.get("wins", 0) or 0)
        ly = int(ytd_record.get("losses", 0) or 0)
        winrate_ytd = wy / max(1, (wy + ly))
    except Exception:
        winrate_ytd = 0.0

    # inactividad + última superficie
    days_inactive = 0.0
    last_surface = None
    try:
        # buscamos el primer partido que tenga timestamp válido (ya vienen ordenados recientes primero)
        last = next((m for m in last10_matches if m.get("date")), None)
        if last and isinstance(last["date"], (int, float)):
            days_inactive = max(0.0, (time.time() - float(last["date"])) / 86400.0)
        last_surface = (last.get("surface") or None) if last else None
    except Exception:
        pass

    return dict(
        winrate_last10=winrate_last10,
        winrate_ytd=winrate_ytd,
        ranking_now=ranking_now,
        days_inactive=days_inactive,
        last_surface=last_surface,
    )
