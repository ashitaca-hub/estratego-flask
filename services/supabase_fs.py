# services/supabase_fs.py
from __future__ import annotations
import os
import logging
import urllib.parse
import requests
from typing import Any, Dict, Optional

log = logging.getLogger("supabase_fs")

# === Config Supabase (service role recomendado para backend) ===
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "20"))

HEADERS_SB = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

def _get(table: str, params: Dict[str, Any] | None = None, select: str = "*") -> list[dict]:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL / SUPABASE_KEY no configurados.")
    q = {"select": select}
    if params:
        q.update(params)
    url = f"{SUPABASE_URL}/rest/v1/{table}?{urllib.parse.urlencode(q, doseq=True)}"
    r = requests.get(url, headers=HEADERS_SB, timeout=HTTP_TIMEOUT)
    if r.status_code >= 300:
        log.warning("SB GET %s -> %s %s", table, r.status_code, r.text[:200])
    r.raise_for_status()
    return r.json()

def _rpc(fn: str, payload: Dict[str, Any]) -> Any:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL / SUPABASE_KEY no configurados.")
    url = f"{SUPABASE_URL}/rest/v1/rpc/{fn}"
    r = requests.post(url, headers=HEADERS_SB, json=payload, timeout=HTTP_TIMEOUT)
    if r.status_code >= 300:
        log.info("SB RPC %s(%s) -> %s %s", fn, payload, r.status_code, r.text[:200])
        r.raise_for_status()
    return r.json() if r.text else None

# -------------------------------------------------------------------
# Torneo → surface / speed
# -------------------------------------------------------------------
def _speed_bucket_from_rank(speed_rank: Optional[float]) -> Optional[str]:
    if speed_rank is None:
        return None
    try:
        v = float(speed_rank)
    except Exception:
        return None
    # buckets simples (ajusta si tu vista ya trae bucket)
    if v <= 33:
        return "Slow"
    if v <= 66:
        return "Medium"
    return "Fast"

def get_tourney_meta(tournament_name: str) -> dict:
    """
    Devuelve {surface, speed_bucket?, speed_rank?, category?} a partir de court_speed_rankig_norm.
    Si la vista no tiene speed_bucket, lo derivamos con _speed_bucket_from_rank.
    """
    if not tournament_name:
        return {}
    try:
        rows = _get(
            "court_speed_rankig_norm",
            {"tournament_name": f"ilike.*{tournament_name}*", "limit": 1},
            select="tournament_name,surface,speed_rank,speed_bucket,category"
        )
    except Exception:
        rows = []
    if not rows:
        return {}
    row = rows[0]
    meta = {
        "surface": (row.get("surface") or "hard").lower(),
        "speed_rank": row.get("speed_rank"),
        "speed_bucket": row.get("speed_bucket"),
        "category": row.get("category"),
    }
    if not meta.get("speed_bucket"):
        meta["speed_bucket"] = _speed_bucket_from_rank(meta.get("speed_rank")) or "Medium"
    return meta

# -------------------------------------------------------------------
# Histórico (Feature Store)
# -------------------------------------------------------------------
def _try_rpc_winrate_month(player_id: int, month: int, years_back: int) -> Optional[float]:
    """
    Intenta RPC fs_month_winrate(p_id int, p_month int, p_years int) → float (0..1)
    """
    try:
        val = _rpc("fs_month_winrate", {"p_id": player_id, "p_month": month, "p_years": years_back})
        if isinstance(val, (int, float)):
            return float(val)
    except Exception:
        pass
    return None

def _try_rpc_winrate_surface(player_id: int, surface: str, years_back: int) -> Optional[float]:
    """
    Intenta RPC fs_surface_winrate(p_id int, p_surface text, p_years int) → float (0..1)
    """
    try:
        val = _rpc("fs_surface_winrate", {"p_id": player_id, "p_surface": surface, "p_years": years_back})
        if isinstance(val, (int, float)):
            return float(val)
    except Exception:
        pass
    return None

def _try_rpc_winrate_speed(player_id: int, speed_bucket: str, years_back: int) -> Optional[float]:
    """
    Intenta RPC fs_speed_winrate(p_id int, p_speed text, p_years int) → float (0..1)
    """
    try:
        val = _rpc("fs_speed_winrate", {"p_id": player_id, "p_speed": speed_bucket, "p_years": years_back})
        if isinstance(val, (int, float)):
            return float(val)
    except Exception:
        pass
    return None

def _try_view_winrate_month(player_id: int, month: int, years_back: int) -> Optional[float]:
    """
    Plan B si no hay RPC: vista agregada p.ej. fs_player_month_winrate con columnas:
      player_id int, month int, years_back int, winrate float
    """
    try:
        rows = _get(
            "fs_player_month_winrate",
            {"player_id": f"eq.{player_id}", "month": f"eq.{month}", "years_back": f"eq.{years_back}", "limit": 1},
            select="winrate"
        )
        if rows:
            return float(rows[0].get("winrate"))
    except Exception:
        pass
    return None

def _try_view_winrate_surface(player_id: int, surface: str, years_back: int) -> Optional[float]:
    try:
        rows = _get(
            "fs_player_surface_winrate",
            {"player_id": f"eq.{player_id}", "surface": f"eq.{surface}", "years_back": f"eq.{years_back}", "limit": 1},
            select="winrate"
        )
        if rows:
            return float(rows[0].get("winrate"))
    except Exception:
        pass
    return None

def _try_view_winrate_speed(player_id: int, speed_bucket: str, years_back: int) -> Optional[float]:
    try:
        rows = _get(
            "fs_player_speed_winrate",
            {"player_id": f"eq.{player_id}", "speed_bucket": f"eq.{speed_bucket}", "years_back": f"eq.{years_back}", "limit": 1},
            select="winrate"
        )
        if rows:
            return float(rows[0].get("winrate"))
    except Exception:
        pass
    return None

def _winrate_month(player_id: int, month: int, years_back: int) -> Optional[float]:
    return (
        _try_rpc_winrate_month(player_id, month, years_back)
        or _try_view_winrate_month(player_id, month, years_back)
    )

def _winrate_surface(player_id: int, surface: str, years_back: int) -> Optional[float]:
    return (
        _try_rpc_winrate_surface(player_id, surface, years_back)
        or _try_view_winrate_surface(player_id, surface, years_back)
    )

def _winrate_speed(player_id: int, speed_bucket: str, years_back: int) -> Optional[float]:
    return (
        _try_rpc_winrate_speed(player_id, speed_bucket, years_back)
        or _try_view_winrate_speed(player_id, speed_bucket, years_back)
    )

def get_matchup_hist_vector(
    p_id: int,
    o_id: int,
    yrs: int,
    tname: str,
    month: int,
) -> dict:
    """
    Devuelve un diccionario con:
      {
        "surface": <str>,
        "speed_bucket": <str>,
        "d_hist_month":   <float>,   # (winrate_p - winrate_o) en [0..1] (no clamp aquí)
        "d_hist_surface": <float>,
        "d_hist_speed":   <float>,
      }

    Estrategia:
      1) Obtener meta del torneo (surface/speed_bucket).
      2) Intentar winrates por mes/superficie/velocidad para ambos via RPC; si no existen, via vistas.
      3) Si falta cualquiera, devolver 0.0 en ese delta (no rompe).
    """
    meta = get_tourney_meta(tname) if tname else {}
    surface = (meta.get("surface") or "hard").lower()
    speed_bucket = meta.get("speed_bucket") or "Medium"

    # Winrates (pueden ser None si no hay datos / no existen vistas-RPC)
    wr_p_month = _winrate_month(p_id, month, yrs)
    wr_o_month = _winrate_month(o_id, month, yrs)

    wr_p_surf = _winrate_surface(p_id, surface, yrs)
    wr_o_surf = _winrate_surface(o_id, surface, yrs)

    wr_p_speed = _winrate_speed(p_id, speed_bucket, yrs)
    wr_o_speed = _winrate_speed(o_id, speed_bucket, yrs)

    # Deltas: si alguno es None, dejamos 0.0 para esa dimensión
    def _delta(a: Optional[float], b: Optional[float]) -> float:
        try:
            if a is None or b is None:
                return 0.0
            # asegurar 0..1
            a1 = min(1.0, max(0.0, float(a)))
            b1 = min(1.0, max(0.0, float(b)))
            return a1 - b1
        except Exception:
            return 0.0

    out = {
        "surface": surface,
        "speed_bucket": speed_bucket,
        "d_hist_month": _delta(wr_p_month, wr_o_month),
        "d_hist_surface": _delta(wr_p_surf, wr_o_surf),
        "d_hist_speed": _delta(wr_p_speed, wr_o_speed),
        # opcionalmente podrías incluir los winrates crudos para depurar:
        # "wr": {"p": {"month": wr_p_month, "surface": wr_p_surf, "speed": wr_p_speed},
        #        "o": {"month": wr_o_month, "surface": wr_o_surf, "speed": wr_o_speed}},
    }

    log.info("FS get_matchup_hist_vector p=%s o=%s yrs=%s t=%s m=%s -> %s",
             p_id, o_id, yrs, tname, month, out)
    return out
