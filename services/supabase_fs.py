# services/supabase_fs.py
from __future__ import annotations
import os, json
import logging
import urllib.parse
import psycopg2
import requests
from typing import Any, Dict, Optional

try:
    import psycopg2
except ImportError:
    psycopg2 = None
    
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

def norm_tourney(txt: str | None) -> str | None:
    """
    Normaliza nombre de torneo vía RPC (si está expuesto por PostgREST).
    Devuelve None si no está disponible.
    """
    if not txt:
        return None
    try:
        res = _rpc("norm_tourney", {"txt": txt})
        # PostgREST puede devolver list/dict/str según la config
        if isinstance(res, list) and res:
            res = res[0]
        if isinstance(res, dict) and "norm_tourney" in res:
            return res["norm_tourney"]
        if isinstance(res, str):
            return res
    except Exception:
        pass
    return None


# -------------------------------------------------------------------
# Torneo → surface / speed
# -------------------------------------------------------------------


log = logging.getLogger("estratego")
if not log.handlers:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

def get_sr_id_from_player_int(player_id: int) -> str | None:
    """
    Devuelve 'sr:competitor:<id>' a partir del player_id interno (INT).
    Lee de public.players_lookup (ext_sportradar_id).
    """
    try:
        rows = _get(  # <-- si tu helper es _rest_get, cámbialo aquí
            "players_lookup",
            {"player_id": f"eq.{int(player_id)}", "limit": 1},
            select="ext_sportradar_id"
        )
        if rows and rows[0].get("ext_sportradar_id"):
            return f"sr:competitor:{rows[0]['ext_sportradar_id']}"
    except Exception as e:
        log.info("get_sr_id_from_player_int(%s) fallo: %s", player_id, e)
    return None


def _speed_bucket_from_rank(rank):
    if rank is None:
        return None
    r = int(rank)
    return "Fast" if r <= 33 else ("Medium" if r <= 66 else "Slow")

def get_tourney_meta(tournament_name: str) -> dict:
    """
    Prioriza resolver (tourney_speed_resolved por clave normalizada).
    Si no hay match, cae a court_speed_rankig_norm por ilike.
    """
    if not tournament_name:
        return {}
    # 1) Resolver por clave normalizada
    key = norm_tourney(tournament_name)
    if key:
        try:
            rows = _get(
                "tourney_speed_resolved",
                {"tourney_key": f"eq.{key}", "limit": 1},
                select="tourney_key,surface,speed_rank,speed_bucket"
            )
            if rows:
                row = rows[0]
                meta = {
                    "surface": (row.get("surface") or "hard").lower(),
                    "speed_rank": row.get("speed_rank"),
                    "speed_bucket": row.get("speed_bucket"),
                }
                if not meta.get("speed_bucket"):
                    meta["speed_bucket"] = _speed_bucket_from_rank(meta.get("speed_rank")) or "Medium"
                return meta
        except Exception:
            pass
    # 2) Fallback compat
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

import datetime as _dt  # al inicio del archivo si no lo tienes

def get_matchup_hist_vector(
    p_id: int,
    o_id: int,
    yrs: int,
    tname: str,
    month: int,
) -> dict:
    """
    Primero intenta el RPC 'get_matchup_hist_vector' (as-of, suavizado).
    Si no está disponible, cae a la lógica actual con winrates.
    """
    # -------- intento RPC único --------
    try:
        payload = {
            "p_player_id": int(p_id),
            "p_opponent_id": int(o_id),
            "p_years_back": int(yrs),
            "p_as_of": _dt.date.today().isoformat(),
            "p_tournament_name": tname,
            "p_month": int(month),
            # Puedes pasar k si quieres: "p_k_month": 8, "p_k_surface": 8, "p_k_speed": 8
        }
        data = _rpc("get_matchup_hist_vector", payload)
        if isinstance(data, list) and len(data) == 1 and isinstance(data[0], dict):
            data = data[0]
        if isinstance(data, dict) and "d_hist_month" in data:
            return {
                "surface": (data.get("surface") or "hard").lower(),
                "speed_bucket": data.get("speed_bucket") or "Medium",
                "d_hist_month": float(data.get("d_hist_month") or 0.0),
                "d_hist_surface": float(data.get("d_hist_surface") or 0.0),
                "d_hist_speed": float(data.get("d_hist_speed") or 0.0),
            }
    except Exception as e:
        log.info("RPC get_matchup_hist_vector no disponible, fallback winrates: %s", e)

    # -------- FALLBACK: tu lógica actual con winrates --------
    meta = get_tourney_meta(tname) if tname else {}
    surface = (meta.get("surface") or "hard").lower()
    speed_bucket = meta.get("speed_bucket") or "Medium"

    wr_p_month = _winrate_month(p_id, month, yrs)
    wr_o_month = _winrate_month(o_id, month, yrs)

    wr_p_surf = _winrate_surface(p_id, surface, yrs)
    wr_o_surf = _winrate_surface(o_id, surface, yrs)

    wr_p_speed = _winrate_speed(p_id, speed_bucket, yrs)
    wr_o_speed = _winrate_speed(o_id, speed_bucket, yrs)

    def _delta(a, b) -> float:
        try:
            if a is None or b is None:
                return 0.0
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
    }
    log.info("FS get_matchup_hist_vector (fallback) p=%s o=%s yrs=%s t=%s m=%s -> %s",
             p_id, o_id, yrs, tname, month, out)
    return out


# === Matchup cache helpers (Postgres) ===============================
import os, json
try:
    import psycopg2  # opcional
except Exception:
    psycopg2 = None

DISABLE_DB_CACHE = str(os.environ.get("DISABLE_DB_CACHE", "")).lower() in ("1","true","yes")

def _pg_conn_or_env(existing=None):
    if existing: return existing, False
    dsn = os.environ.get("DATABASE_URL")
    if not dsn or psycopg2 is None:
        raise RuntimeError("DB no disponible (psycopg2 o DATABASE_URL)")
    return psycopg2.connect(dsn), True

def get_player_meta(pid_int=None, sr_id=None, conn=None):
    """
    Devuelve: {name, country_code, rank, ytd_wr, def_points, def_title}
    - name / country_code: de public.players_lookup (por player_id interno o ext_sportradar_id).
    - ytd_wr: winrate del año actual en fs_matches_long.
    - rank/def_*: placeholders (rellénalos si tienes vistas/servicios).
    """
    pg, opened = _pg_conn_or_env(conn)
    out = {"name": None, "country_code": None, "rank": None, "ytd_wr": None,
           "def_points": None, "def_title": None}
    try:
        with pg.cursor() as cur:
            # 1) Nombre y país desde players_lookup
            if pid_int is not None:
                cur.execute("""
                    SELECT name, country_code
                    FROM public.players_lookup
                    WHERE player_id = %s
                    LIMIT 1
                """, (pid_int,))
            elif sr_id:
                # sr_id viene como 'sr:competitor:XXXX'
                sr_num = sr_id.split(":")[-1] if ":" in sr_id else sr_id
                cur.execute("""
                    SELECT name, country_code
                    FROM public.players_lookup
                    WHERE ext_sportradar_id = %s
                    LIMIT 1
                """, (sr_num,))
            row = cur.fetchone()
            if row:
                out["name"], out["country_code"] = row[0], row[1]

            # 2) YTD (año actual) en fs_matches_long
            #    Ten en cuenta que player_id en fs_matches_long es entero interno
            if pid_int is not None:
                cur.execute("""
                    WITH hist AS (
                      SELECT
                        CASE WHEN winner_id = player_id THEN 1 ELSE 0 END AS won
                      FROM public.fs_matches_long
                      WHERE player_id = %s
                        AND match_date >= date_trunc('year', current_date)
                        AND match_date <= current_date
                    )
                    SELECT AVG(won::float) FROM hist
                """, (pid_int,))
                ytd = cur.fetchone()[0]
                out["ytd_wr"] = float(ytd) if ytd is not None else None

            # TODO opcional:
            # 3) Rank actual (si tienes una vista o API para rank en tiempo real)
            # 4) Puntos a defender (si tienes vista con entradas del torneo actual)

    finally:
        if opened:
            pg.close()
    return out


def get_matchup_cache_json(player_id:int, opponent_id:int,
                           tournament_name:str, mon:int,
                           speed_bucket:str, years_back:int,
                           using_sr:bool, conn=None):
    """
    Devuelve (tourney_key, json|None). Si no hay psycopg2 o cache desactivada, devuelve (None, None).
    """
    if DISABLE_DB_CACHE or psycopg2 is None:
        return None, None
    pg, opened = _pg_conn_or_env(conn)
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT public.norm_tourney(%s)", (tournament_name,))
            tkey = (cur.fetchone() or [None])[0]
            cur.execute("""
                SELECT public.get_matchup_cache_json(%s,%s,%s,%s,%s,%s,%s)
            """, (player_id, opponent_id, tkey, mon, speed_bucket or "", years_back, using_sr))
            row = cur.fetchone()
            return tkey, (row[0] if row and row[0] is not None else None)
    finally:
        if opened:
            pg.close()

def put_matchup_cache_json(player_id:int, opponent_id:int,
                           tournament_name:str, mon:int,
                           surface:str, speed_bucket:str, years_back:int,
                           using_sr:bool, prob_player:float,
                           features:dict, flags:dict, weights_hist:dict|None,
                           sources:dict|None, ttl_seconds:int|None,
                           conn=None):
    """
    No-op si no hay psycopg2 o cache desactivada.
    """
    if DISABLE_DB_CACHE or psycopg2 is None:
        return
    pg, opened = _pg_conn_or_env(conn)
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT public.norm_tourney(%s)", (tournament_name,))
            tkey = (cur.fetchone() or [None])[0]
            cur.execute("""
                SELECT public.put_matchup_cache_json(
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s::jsonb,%s
                )
            """, (
                player_id, opponent_id, tkey, mon, surface.lower() if surface else None,
                speed_bucket or "", years_back, using_sr, float(prob_player),
                json.dumps(features), json.dumps(flags),
                json.dumps(weights_hist) if weights_hist is not None else None,
                json.dumps(sources) if sources is not None else None,
                ttl_seconds
            ))
        if opened:
            pg.commit()
    finally:
        if opened:
            pg.close()

# === Bracket runs (opcional) ========================================
def insert_bracket_run(
    tournament_name: str,
    tournament_month: int,
    years_back: int,
    mode: str,
    entrants: list[dict],
    result: dict,
    champion_id: int | None = None,
    champion_name: str | None = None,
    used_sr: bool | None = None,
    api_version: str | None = None,
    conn=None,
):
    """
    Inserta una corrida de bracket. Si psycopg2 no está presente, no hace nada (no rompe en CI).
    Rellena columnas extra solo si se proporcionan.
    """
    if psycopg2 is None:  # fallback si no tienes psycopg2 instalado
        return

    cols = ["tournament_name", "tournament_month", "years_back", "mode", "entrants", "result"]
    vals = [tournament_name, tournament_month, years_back, mode, json.dumps(entrants), json.dumps(result)]

    if champion_id is not None:
        cols.append("champion_id")
        vals.append(champion_id)
    if champion_name is not None:
        cols.append("champion_name")
        vals.append(champion_name)
    if used_sr is not None:
        cols.append("used_sr")
        vals.append(used_sr)
    if api_version is not None:
        cols.append("api_version")
        vals.append(api_version)

    placeholders = ", ".join(["%s"] * len(cols))
    collist = ", ".join(cols)

    pg, opened = _pg_conn_or_env(conn)
    try:
        with pg.cursor() as cur:
            cur.execute(
                f"INSERT INTO public.bracket_runs ({collist}) VALUES ({placeholders})",
                vals
            )
        if opened:
            pg.commit()
    finally:
        if opened:
            pg.close()




