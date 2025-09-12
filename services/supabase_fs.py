# services/supabase_fs.py
from __future__ import annotations
import os, json, re
import logging
import urllib.parse
from typing import Any, Dict, Optional

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None

import requests
import datetime as _dt

# ───────────────────────────────────────────────────────────────────
# Configuración
# ───────────────────────────────────────────────────────────────────
log = logging.getLogger("supabase_fs")
if not log.handlers:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# Supabase REST (si lo usas en otros helpers)
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "20"))

HEADERS_SB = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

# Sportradar fallbacks (si existen en tu proyecto)
try:
    # wrappers opcionales
    from services.sportradar_now import get_current_rank, get_ytd_wr  # ajusta si difieren
except Exception:
    get_current_rank = None
    get_ytd_wr = None


# ───────────────────────────────────────────────────────────────────
# Helpers REST a Supabase PostgREST
# ───────────────────────────────────────────────────────────────────

def norm_tourney_py(name: str) -> str:
    return " ".join((name or "").lower().split())

def get_defense_prev_year(tourney_name: str, player_ids, conn=None):
    """
    Devuelve dict {player_id: {points:int, title_code:'champ'|'runner'}} para ese torneo.
    """
    if not tourney_name or not player_ids:
        return {}
    tkey = norm_tourney_py(tourney_name)
    sql = """
    SELECT player_id, points, title_code
    FROM public.player_defense_prev_year
    WHERE tourney_key = public.norm_tourney(%s)
      AND player_id = ANY(%s)
    """
    close_conn = False
    if conn is None:
        conn = psycopg.connect(os.environ["DATABASE_URL"])
        close_conn = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (tkey, player_ids))
            rows = cur.fetchall()
        out = {}
        for pid, pts, code in rows:
            out[pid] = {"points": pts, "title_code": code}
        return out
    finally:
        if close_conn:
            conn.close()

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


# ───────────────────────────────────────────────────────────────────
# Conexión PG
# ───────────────────────────────────────────────────────────────────

DISABLE_DB_CACHE = str(os.environ.get("DISABLE_DB_CACHE", "")).lower() in ("1","true","yes")

def _pg_conn_or_env(conn=None):
    if conn:
        return conn, False
    if psycopg2 is None:
        raise RuntimeError("psycopg2 no disponible y no se pasó conn")
    dsn = os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DB_URL")
    pg = psycopg2.connect(dsn)
    return pg, True

# --- helpers PG-only ------------------------------------------------

def _pg_fetch_one(sql: str, params: tuple = (), conn=None) -> dict | None:
    try:
        pg, opened = _pg_conn_or_env(conn)
        try:
            with pg.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                return cur.fetchone()
        finally:
            if opened: pg.close()
    except Exception as e:
        log.info("_pg_fetch_one fallo: %s", e)
    return None


# ───────────────────────────────────────────────────────────────────
# Normalización / torneo
# ───────────────────────────────────────────────────────────────────

def norm_tourney(txt: str | None) -> str | None:
    if not txt:
        return None
    try:
        res = _rpc("norm_tourney", {"txt": txt})
        if isinstance(res, list) and res:
            res = res[0]
        if isinstance(res, dict) and "norm_tourney" in res:
            return res["norm_tourney"]
        if isinstance(res, str):
            return res
    except Exception:
        pass
    # fallback local
    return re.sub(r'[^a-z0-9]+', ' ', txt.lower()).strip()

def _speed_bucket_from_rank(rank):
    if rank is None:
        return None
    r = int(rank)
    return "Fast" if r <= 33 else ("Medium" if r <= 66 else "Slow")

def get_tourney_meta(tournament_name: str) -> dict:
    """
    1º intenta tourney_speed_resolved por clave normalizada;
    2º fallback a court_speed_rankig_norm por ILIKE;
    devuelve surface/speed_rank/speed_bucket/category si hay.
    """
    if not tournament_name:
        return {}
    key = norm_tourney(tournament_name)
    # 1) resolved
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
    # 2) compat
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

def get_tourney_country(tournament_name: str | None) -> str | None:
    """Devuelve un ISO-2 si lo tienes en tablas; si no, None."""
    if not tournament_name:
        return None
    key = norm_tourney(tournament_name)
    # 1) resolved (preferido)
    try:
        rows = _get("tourney_speed_resolved",
                    {"tourney_key": f"eq.{key}", "limit": 1},
                    select="country_code")
        if rows and rows[0].get("country_code"):
            return rows[0]["country_code"]
    except Exception:
        pass
    # 2) compat
    try:
        rows = _get("court_speed_rankig_norm",
                    {"tournament_name": f"ilike.*{tournament_name}*", "limit": 1},
                    select="country_code")
        if rows and rows[0].get("country_code"):
            return rows[0]["country_code"]
    except Exception:
        pass
    return None


# ───────────────────────────────────────────────────────────────────
# Feature store – winrates (RPC o vistas precalculadas)
# ───────────────────────────────────────────────────────────────────

def _try_rpc_winrate_month(player_id: int, month: int, years_back: int) -> Optional[float]:
    try:
        val = _rpc("fs_month_winrate", {"p_id": player_id, "p_month": month, "p_years": years_back})
        if isinstance(val, (int, float)):
            return float(val)
    except Exception:
        pass
    return None

def _try_rpc_winrate_surface(player_id: int, surface: str, years_back: int) -> Optional[float]:
    try:
        val = _rpc("fs_surface_winrate", {"p_id": player_id, "p_surface": surface, "p_years": years_back})
        if isinstance(val, (int, float)):
            return float(val)
    except Exception:
        pass
    return None

def _try_rpc_winrate_speed(player_id: int, speed_bucket: str, years_back: int) -> Optional[float]:
    try:
        val = _rpc("fs_speed_winrate", {"p_id": player_id, "p_speed": speed_bucket, "p_years": years_back})
        if isinstance(val, (int, float)):
            return float(val)
    except Exception:
        pass
    return None

def _try_view_winrate_month(player_id: int, month: int, years_back: int) -> Optional[float]:
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


# ───────────────────────────────────────────────────────────────────
# Matchup histórico (RPC con fallback a winrates)
# ───────────────────────────────────────────────────────────────────

def get_matchup_hist_vector(
    p_id: int,
    o_id: int,
    yrs: int,
    tname: str,
    month: int,
) -> dict:
    # 1) intento RPC único (si lo tienes)
    try:
        payload = {
            "p_player_id": int(p_id),
            "p_opponent_id": int(o_id),
            "p_years_back": int(yrs),
            "p_as_of": _dt.date.today().isoformat(),
            "p_tournament_name": tname,
            "p_month": int(month),
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

    # 2) FALLBACK (vistas/RPC simples)
    meta = get_tourney_meta(tname) if tname else {}
    surface = (meta.get("surface") or "hard").lower()
    speed_bucket = meta.get("speed_bucket") or "Medium"

    def _delta(a, b) -> float:
        try:
            if a is None or b is None:
                return 0.0
            a1 = min(1.0, max(0.0, float(a)))
            b1 = min(1.0, max(0.0, float(b)))
            return a1 - b1
        except Exception:
            return 0.0

    wr_p_month = _winrate_month(p_id, month, yrs)
    wr_o_month = _winrate_month(o_id, month, yrs)
    wr_p_surf  = _winrate_surface(p_id, surface, yrs)
    wr_o_surf  = _winrate_surface(o_id, surface, yrs)
    wr_p_speed = _winrate_speed(p_id, speed_bucket, yrs)
    wr_o_speed = _winrate_speed(o_id, speed_bucket, yrs)

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


# ───────────────────────────────────────────────────────────────────
# Player meta: Ranking + YTD (DB → SR fallback)
# ───────────────────────────────────────────────────────────────────

def _digits_from_sr_id(sr_id: str | None) -> Optional[int]:
    if not sr_id:
        return None
    d = re.sub(r"\D", "", sr_id)
    return int(d) if d else None

def _get_rank_from_db_view(player_id_int: int, conn=None) -> tuple[Optional[int], Optional[int]]:
    """
    Lee ranking/points desde la vista pública v_player_rank_now_int.
    """
    try:
        pg, opened = _pg_conn_or_env(conn)
        try:
            with pg.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    select rank, points
                    from public.v_player_rank_now_int
                    where player_id = %s
                    limit 1
                """, (player_id_int,))
                row = cur.fetchone()
                if row:
                    return (row.get("rank"), row.get("points"))
        finally:
            if opened: pg.close()
    except Exception as e:
        log.info("_get_rank_from_db_view fallo: %s", e)
    return (None, None)

def _get_ytd_from_db_view(player_id_int: int, conn=None) -> Optional[float]:
    """
    Lee winrate YTD (0..1) desde v_player_ytd_now_int.
    """
    try:
        pg, opened = _pg_conn_or_env(conn)
        try:
            with pg.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    select winrate
                    from public.v_player_ytd_now_int
                    where player_id = %s
                    limit 1
                """, (player_id_int,))
                row = cur.fetchone()
                if row and row.get("winrate") is not None:
                    return float(row["winrate"])
        finally:
            if opened: pg.close()
    except Exception as e:
        log.info("_get_ytd_from_db_view fallo: %s", e)
    return None

def _rank_norm(rank: Optional[int]) -> Optional[float]:
    if rank is None:
        return None
    r = min(max(int(rank), 1), 200)  # cap 200
    return max(0.0, 1.0 - (r / 200.0))

def _upsert_rank_snapshot_sr(player_id_int: int, rank: int,
                             points: Optional[int] = None,
                             year: Optional[int] = None,
                             week: Optional[int] = None,
                             name: Optional[str] = None,
                             country_code: Optional[str] = None,
                             conn=None):
    """
    Cachea el ranking de SR en rankings_snapshot_int para snapshot_date = hoy.
    Si no tienes year/week, puedes pasar None (se guardarán NULL).
    """
    try:
        pg, opened = _pg_conn_or_env(conn)
        try:
            with pg.cursor() as cur:
                cur.execute("""
                    insert into public.rankings_snapshot_int
                      (snapshot_date, year, week, player_id, rank, points, player_name, country_code, source)
                    values (current_date, %s, %s, %s, %s, %s, %s, %s, 'sportradar')
                    on conflict (snapshot_date, player_id) do update set
                      rank = excluded.rank,
                      points = excluded.points,
                      player_name = coalesce(excluded.player_name, rankings_snapshot_int.player_name),
                      country_code = coalesce(excluded.country_code, rankings_snapshot_int.country_code),
                      updated_at = now()
                """, (year, week, int(player_id_int), int(rank), points, name, country_code))
            if opened: pg.commit()
        finally:
            if opened: pg.close()
    except Exception as e:
        log.info("_upsert_rank_snapshot_sr fallo (no crítico): %s", e)

def get_sr_id_from_player_int(player_id: int) -> str | None:
    """
    Devuelve 'sr:competitor:<id>' a partir del player_id INT.
    1) BD: players_lookup.ext_sportradar_id
    2) BD: players_ext.ext_sportradar_id
    3) (opcional) REST _get como último recurso
    """
    pid = int(player_id)

    # 1) BD: players_lookup
    row = _pg_fetch_one("""
        select ext_sportradar_id
        from public.players_lookup
        where player_id = %s
        limit 1
    """, (pid,))
    if row and row.get("ext_sportradar_id"):
        ext = row["ext_sportradar_id"]
        return ext if ext.startswith("sr:") else f"sr:competitor:{ext}"

    # 2) BD: players_ext
    row = _pg_fetch_one("""
        select ext_sportradar_id
        from public.players_ext
        where player_id = %s
        limit 1
    """, (pid,))
    if row and row.get("ext_sportradar_id"):
        ext = row["ext_sportradar_id"]
        return ext if ext.startswith("sr:") else f"sr:competitor:{ext}"

    # 3) REST (solo si hay credenciales)
    try:
        rows = _get("players_lookup", {"player_id": f"eq.{pid}", "limit": 1}, select="ext_sportradar_id")
        if rows and rows[0].get("ext_sportradar_id"):
            ext = rows[0]["ext_sportradar_id"]
            return ext if ext.startswith("sr:") else f"sr:competitor:{ext}"
    except Exception as e:
        log.info("get_sr_id_from_player_int REST fallback fallo: %s", e)

    return None



def get_player_meta(
    pid_int: Optional[int] = None,
    sr_id: Optional[str] = None,
    asof_date: Optional[date] = None,
    conn=None,
) -> Dict[str, Any]:
    """
    Devuelve metadatos de jugador tirando **solo de BD**:
      - players_lookup (name, country_code, ext_sportradar_id)
      - v_player_rank_now_int (rank/points actuales)
      - v_player_ytd_now_int (win-rate YTD)
      - fallback a rankings_snapshot_int para name/country si faltan
    """
    meta: Dict[str, Any] = {
        "player_id": pid_int,
        "ext_sportradar_id": None,
        "name": None,
        "country_code": None,
        "rank": None,
        "rank_points": None,
        "rank_source": None,   # e.g., "db:rankings_snapshot_int"
        "ytd_wr": None,        # 0..1
    }

    # Si viene sr_id y no tenemos pid_int, intenta extraer los dígitos
    if pid_int is None and sr_id:
        try:
            m = re.search(r"(\d+)$", sr_id)
            if m:
                pid_int = int(m.group(1))
                meta["player_id"] = pid_int
        except Exception:
            pass

    # --- 1) BD directo: players_lookup (name/country/sr_id)
    if pid_int is not None:
        row = _pg_fetch_one(
            """
            select name, country_code, ext_sportradar_id
            from public.players_lookup
            where player_id = %s
            limit 1
            """,
            (int(pid_int),),
            conn=conn,
        )
        if row:
            if not meta["name"]:
                meta["name"] = row.get("name")
            if not meta["country_code"]:
                meta["country_code"] = row.get("country_code")
            if not meta["ext_sportradar_id"] and row.get("ext_sportradar_id"):
                ext = row["ext_sportradar_id"]
                meta["ext_sportradar_id"] = ext if isinstance(ext, str) and ext.startswith("sr:") else f"sr:competitor:{ext}"

    # --- 2) SR id desde BD auxiliar si aún falta
    if not meta["ext_sportradar_id"] and pid_int is not None:
        meta["ext_sportradar_id"] = get_sr_id_from_player_int(pid_int)

    # --- 3) Rank ahora (vista DB)
    if pid_int is not None:
        row = _pg_fetch_one(
            """
            select rank, points
            from public.v_player_rank_now_int
            where player_id = %s
            limit 1
            """,
            (int(pid_int),),
            conn=conn,
        )
        if row:
            meta["rank"] = row.get("rank")
            meta["rank_points"] = row.get("points")
            meta["rank_source"] = "db:rankings_snapshot_int"

    # --- 4) YTD ahora (vista DB)
    if pid_int is not None and meta["ytd_wr"] is None:
        # Asumimos que la vista expone 'wr' o 'ytd_wr'; probamos 'wr' primero
        row = _pg_fetch_one(
            """
            select wr
            from public.v_player_ytd_now_int
            where player_id = %s
            limit 1
            """,
            (int(pid_int),),
            conn=conn,
        )
        if not row:
            # fallback de nombre alternativo
            row = _pg_fetch_one(
                """
                select ytd_wr as wr
                from public.v_player_ytd_now_int
                where player_id = %s
                limit 1
                """,
                (int(pid_int),),
                conn=conn,
            )
        if row and row.get("wr") is not None:
            meta["ytd_wr"] = float(row["wr"])

    # --- 5) Fallback final de nombre/país: último snapshot de rankings si siguen faltando
    if pid_int is not None and (meta["name"] is None or meta["country_code"] is None):
        row = _pg_fetch_one(
            """
            select player_name, country_code
            from public.rankings_snapshot_int
            where player_id = %s
            order by snapshot_date desc
            limit 1
            """,
            (int(pid_int),),
            conn=conn,
        )
        if row:
            if meta["name"] is None and row.get("player_name"):
                meta["name"] = row["player_name"]
            if meta["country_code"] is None and row.get("country_code"):
                meta["country_code"] = row["country_code"]

    return meta



# ───────────────────────────────────────────────────────────────────
# Matchup cache JSON (opcional si tienes funciones SQL)
# ───────────────────────────────────────────────────────────────────

# --- NEW: defensa puntos por SR IDs (campeón/runner del año previo) ---
def get_defense_prev_year_by_sr(tourney_name: str, sr_ids, conn=None):
    """
    Devuelve { sr_competitor_id(int): {"points": int, "title_code": "champ|runner"} }
    tourney_name: nombre del torneo tal como llega al servidor (se normaliza dentro de SQL)
    sr_ids: lista de ints Sportradar (ej. [225050, 407573])
    """
    if not tourney_name or not sr_ids:
        return {}
    ids = [int(x) for x in sr_ids if x is not None]

    close = False
    if conn is None:
        import os, psycopg
        conn = psycopg.connect(os.environ["DATABASE_URL"])
        close = True

    try:
        with conn.cursor() as cur:
            # 1) Intento con la vista puente (más directa)
            try:
                cur.execute(
                    """
                    SELECT sr_competitor_id, points, title_code
                    FROM public.v_player_defense_prev_year_sr
                    WHERE tourney_key = public.norm_tourney(%s)
                      AND sr_competitor_id = ANY(%s)
                    """,
                    (tourney_name, ids),
                )
                rows = cur.fetchall()
                if rows:
                    return {r[0]: {"points": r[1], "title_code": r[2]} for r in rows}
            except Exception:
                # La vista no existe o no es accesible: fallback via mapping
                pass

            # 2) Fallback: player_defense_prev_year + mapping (players_ext/players_lookup)
            cur.execute(
                """
                WITH mapped AS (
                  SELECT
                    pd.tourney_key,
                    pd.player_id,
                    pd.points,
                    pd.title_code,
                    COALESCE(pe.ext_sportradar_id, pl.ext_sportradar_id) AS ext_sportradar_id
                  FROM public.player_defense_prev_year pd
                  LEFT JOIN public.players_ext    pe ON pe.player_id = pd.player_id
                  LEFT JOIN public.players_lookup pl ON pl.player_id = pd.player_id
                  WHERE pd.tourney_key = public.norm_tourney(%s)
                )
                SELECT NULLIF(regexp_replace(ext_sportradar_id, '\D','','g'),'')::int AS sr_id,
                       points, title_code
                FROM mapped
                WHERE ext_sportradar_id IS NOT NULL
                  AND NULLIF(regexp_replace(ext_sportradar_id, '\D','','g'),'')::int = ANY(%s)
                """,
                (tourney_name, ids),
            )
            rows = cur.fetchall()
            return {r[0]: {"points": r[1], "title_code": r[2]} for r in rows}
    finally:
        if close:
            conn.close()


def get_matchup_cache_json(player_id:int, opponent_id:int,
                           tournament_name:str, mon:int,
                           speed_bucket:str, years_back:int,
                           using_sr:bool, conn=None):
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


# ───────────────────────────────────────────────────────────────────
# Bracket runs (opcional)
# ───────────────────────────────────────────────────────────────────

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
    if psycopg2 is None:
        return
    cols = ["tournament_name", "tournament_month", "years_back", "mode", "entrants", "result"]
    vals = [tournament_name, tournament_month, years_back, mode, json.dumps(entrants), json.dumps(result)]
    if champion_id is not None:
        cols.append("champion_id"); vals.append(champion_id)
    if champion_name is not None:
        cols.append("champion_name"); vals.append(champion_name)
    if used_sr is not None:
        cols.append("used_sr"); vals.append(used_sr)
    if api_version is not None:
        cols.append("api_version"); vals.append(api_version)

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
