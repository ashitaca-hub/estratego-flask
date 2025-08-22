import os
import requests

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

HEADERS_SB = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "12"))

def rpc(fn: str, payload: dict):
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_KEY")
    url = f"{SUPABASE_URL}/rest/v1/rpc/{fn}"
    r = requests.post(url, headers=HEADERS_SB, json=payload, timeout=HTTP_TIMEOUT)
    if r.status_code >= 300:
        raise RuntimeError(f"RPC {fn} failed: {r.status_code} {r.text}")
    return r.json()

def get_matchup_hist_vector(p_id: str, o_id: str, yrs: int, tname: str, month: int):
    data = rpc("get_matchup_hist_vector", {
        "p_id": p_id, "o_id": o_id, "yrs": yrs, "tname": tname, "month": month
    })
    return (data[0] if isinstance(data, list) and data else {}) or {}

def get_tourney_meta(tname: str):
    data = rpc("get_tourney_meta", {"tname": tname})
    return (data[0] if isinstance(data, list) and data else {}) or {}

import urllib.parse

def rest_get(table: str, params: dict, select="*"):
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_KEY")
    base = SUPABASE_URL.rstrip("/") + "/rest/v1/" + table
    q = {"select": select}
    q.update(params or {})
    url = base + "?" + urllib.parse.urlencode(q, doseq=True)
    r = requests.get(url, headers=HEADERS_SB, timeout=HTTP_TIMEOUT)
    if r.status_code >= 300:
        raise RuntimeError(f"GET {table} failed: {r.status_code} {r.text}")
    return r.json()

def resolve_player_uuid_by_sr(sr_id: str) -> str | None:
    if not sr_id:
        return None
    # Acepta formatos "sr:competitor:1234" o solo "1234"
    sr = sr_id.split(":")[-1]
    rows = rest_get("players",
        {"ext_sportradar_id": f"eq.{sr}"}, select="player_id,name,ext_sportradar_id")
    return rows[0]["player_id"] if rows else None

def resolve_player_uuid_by_name(name: str) -> str | None:
    if not name:
        return None
    # Si tienes view pública 'players_min', úsala; si no, tabla 'players'
    for table in ("players_min", "players"):
        try:
            rows = rest_get(table,
                {"name": f"ilike.*{name}*", "limit": 1}, select="player_id,name")
            if rows:
                return rows[0]["player_id"]
        except Exception:
            continue
    return None

def tourney_meta_by_name_like(tname: str) -> dict:
    """Fallback REST si la RPC get_tourney_meta no devuelve nada."""
    if not tname:
        return {}
    try:
        rows = rest_get("court_speed_rankig_norm",
            {"tournament_name": f"ilike.*{tname}*", "limit": 1},
            select="tournament_name,surface,speed_rank,category")
        return rows[0] if rows else {}
    except Exception:
        return {}

import urllib.parse

def rest_get(table: str, params: dict, select="*"):
    base = SUPABASE_URL.rstrip("/") + "/rest/v1/" + table
    q = {"select": select}; q.update(params or {})
    url = base + "?" + urllib.parse.urlencode(q, doseq=True)
    r = requests.get(url, headers=HEADERS_SB, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def rest_patch(table: str, where: dict, payload: dict):
    base = SUPABASE_URL.rstrip("/") + "/rest/v1/" + table
    url = base + "?" + urllib.parse.urlencode(where, doseq=True)
    h = HEADERS_SB.copy(); h["Prefer"] = "return=minimal"
    r = requests.patch(url, headers=h, json=payload, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return True

def resolve_player_uuid_by_name(name: str) -> str | None:
    if not name: return None
    for table in ("players_min", "players"):
        try:
            rows = rest_get(table, {"name": f"ilike.*{name}*", "limit": 1}, select="player_id,name")
            if rows: return rows[0]["player_id"]
        except Exception:
            continue
    return None

def attach_sr_id_to_uuid(player_uuid: str, sr_num: str) -> bool:
    if not (player_uuid and sr_num): return False
    rest_patch("players", {"player_id": f"eq.{player_uuid}"}, {"ext_sportradar_id": sr_num})
    return True

