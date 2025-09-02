# poblar_2025_sportradar.py  — Opción A con fix de tournament_name/surface + fallback de season info
# - Descubre competitions ATP+GS (o usa las que pases), encuentra seasons 2025,
# - descarga /seasons/{id}/summaries.json, normaliza a formato "long" (2 filas por partido),
# - y hace UPSERT a public.matches_long_base.
# Fix clave: extracción robusta de tournament_name/surface + fallback a /seasons/{id}/info.json.

import os, time, re, sys
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# ==== CONFIG por variables de entorno ====
SR_API_KEY     = os.getenv("SR_API_KEY", "")
DATABASE_URL   = os.getenv("DATABASE_URL", "")
BASE_URL       = os.getenv("BASE_URL", "https://api.sportradar.com/tennis/trial/v3/en")
RATE_SLEEP     = float(os.getenv("RATE_SLEEP", "0.35"))  # sube a 0.8 si ves 429
SEASON_IDS_CSV = os.getenv("SEASON_IDS_CSV", "")         # opcional: "sr:season:123,sr:season:456"
COMPS_CSV      = os.getenv("COMPETITIONS_CSV", "")       # opcional: "sr:competition:111,sr:competition:222"

# ==== Overrides (si pasas listas manuales via inputs del workflow) ====
OVERRIDE_SEASON_IDS = [s.strip() for s in SEASON_IDS_CSV.split(",") if s.strip()]
OVERRIDE_COMPETITIONS = [c.strip() for c in COMPS_CSV.split(",") if c.strip()]

# ==== Helpers HTTP / parsing ====
def sr_get(path, expect_ok=True):
    url = f"{BASE_URL}{path}"
    r = requests.get(url, headers={"accept":"application/json","x-api-key":SR_API_KEY}, timeout=30)
    if expect_ok and r.status_code != 200:
        raise RuntimeError(f"SR {r.status_code}: {r.text[:200]}")
    return r.json()

def digits(s):
    m = re.sub(r"\D", "", s or "")
    return int(m) if m else None

# ==== 1) Descubrir competitions (ATP + Grand Slams) ====
def discover_atp_gs_competitions():
    data = sr_get("/competitions.json")
    comps = data.get("competitions", []) or []
    keep = []
    for c in comps:
        gender = (c.get("gender") or "").lower()   # "men", "women", "mixed", ...
        name   = (c.get("name") or "")
        cat    = c.get("category") or {}
        cat_name = (cat.get("name") or "")
        if gender and gender != "men":
            continue
        cn = cat_name.lower()
        nn = name.lower()
        is_grand_slam = ("grand" in cn and "slam" in cn) or any(gs in nn for gs in [
            "australian open","roland garros","wimbledon","us open"
        ])
        is_atp_tour = ("atp" in cn and "challenger" not in cn) and ("doubles" not in nn)
        if is_grand_slam or is_atp_tour:
            keep.append({"id": c["id"], "name": name, "category": cat_name, "gender": gender or "n/a"})
    return keep

# ==== 2) De competition → seasons 2025 ====
def seasons_2025_for_competition(comp_id):
    data = sr_get(f"/competitions/{comp_id}/seasons.json")
    out = []
    for s in data.get("seasons", []) or []:
        year = s.get("year")
        if str(year) == "2025":
            out.append({"id": s["id"], "name": s.get("name"), "year": year})
    return out

# ==== Extractores robustos de torneo/surface y season meta cache ====
SEASON_META_CACHE = {}

def extract_tournament_meta(sport_event):
    # Intenta in 'tournament' y luego en 'competition'
    t = sport_event.get("tournament")  or {}
    c = sport_event.get("competition") or {}
    # nombre y surface (si viene)
    tname   = t.get("name")     or c.get("name")
    surface = t.get("surface")  or c.get("surface")
    # season id asociado
    t_season = (t.get("season") or {}).get("id")
    c_season = (c.get("season") or {}).get("id")
    season_id = t_season or c_season
    return tname, surface, season_id

def get_season_name(sid):
    """Fallback: /seasons/{id}/info.json para obtener nombre del torneo/season."""
    if not sid: 
        return None
    if sid in SEASON_META_CACHE:
        return SEASON_META_CACHE[sid]
    data = sr_get(f"/seasons/{sid}/info.json", expect_ok=False)
    # Algunas feeds traen `season.name`, otras `competition.name`
    nm = (data.get("season") or {}).get("name") or (data.get("competition") or {}).get("name")
    SEASON_META_CACHE[sid] = nm
    return nm

# ==== 3) Descargar summaries de una season con fix ====
def fetch_season_summaries(season_id):
    data = sr_get(f"/seasons/{season_id}/summaries.json")
    rows = []
    for ev in data.get("summaries", []) or []:
        st_obj = ev.get("sport_event_status", {}) or {}
        status = st_obj.get("status")
        if status not in ("closed", "ended"):  # solo partidos finalizados
            continue

        se = ev.get("sport_event", {}) or {}
        start_time    = se.get("start_time")
        ext_event_id  = se.get("id")

        # Nuevo: extracciones robustas
        tname, surface, season_from_se = extract_tournament_meta(se)
        ext_season_id = season_from_se or season_id

        # Participantes
        comps = se.get("competitors", []) or []
        ids = [digits(c.get("id")) for c in comps]
        if len(ids) != 2 or None in ids:
            continue

        winner_full = st_obj.get("winner_id")
        w_id = digits(winner_full)

        if not start_time or not ext_event_id or not w_id:
            continue

        # Fallback de nombre: si tname vacío, usar /seasons/{id}/info.json
        if not tname:
            tname = get_season_name(ext_season_id)

        # Parse fecha
        try:
            dt = datetime.fromisoformat(start_time.replace("Z", "+00:00")).date().isoformat()
        except Exception:
            # Si viniera sin Z ni TZ, intenta parse directo
            dt = datetime.fromisoformat(start_time.split("+")[0]).date().isoformat()

        p_id, o_id = ids[0], ids[1]

        # Formato long: dos filas por partido
        rows.append((dt, p_id, o_id, w_id, tname, surface, ext_season_id, ext_event_id))
        rows.append((dt, o_id, p_id, w_id, tname, surface, ext_season_id, ext_event_id))
    return rows

# ==== 4) UPSERT en BD ====
UPSERT_SQL = """
insert into public.matches_long_base
  (match_date, player_id, opponent_id, winner_id, tournament_name, surface, ext_season_id, ext_event_id)
values %s
on conflict (ext_event_id, player_id) do update set
  opponent_id    = excluded.opponent_id,
  winner_id      = excluded.winner_id,
  tournament_name= coalesce(excluded.tournament_name, public.matches_long_base.tournament_name),
  surface        = coalesce(excluded.surface, public.matches_long_base.surface),
  updated_at     = now();
"""

def ensure_base_tables(conn):
    """Crea la tabla base y vista fs_matches_long si no existen."""
    with conn.cursor() as cur:
        cur.execute("""
        create table if not exists public.matches_long_base (
          match_date date not null,
          player_id int not null,
          opponent_id int not null,
          winner_id int not null,
          tournament_name text,
          surface text,
          ext_season_id text,
          ext_event_id text,
          created_at timestamptz default now(),
          updated_at timestamptz default now()
        );
        """)
        cur.execute("""
        create unique index if not exists matches_long_base_uniq
          on public.matches_long_base (ext_event_id, player_id);
        """)
        # Vista canónica que el resto del stack usa
        cur.execute("""
        create or replace view public.fs_matches_long as
        select match_date, player_id, opponent_id, winner_id,
               tournament_name, surface, ext_season_id, ext_event_id
        from public.matches_long_base;
        """)
    conn.commit()

def upsert_matches_long_base(conn, rows):
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, page_size=2000)
    conn.commit()
    return len(rows)

# ==== MAIN ====
def main():
    if not SR_API_KEY:
        print("❌ Falta SR_API_KEY", file=sys.stderr); sys.exit(1)
    if not DATABASE_URL:
        print("❌ Falta DATABASE_URL", file=sys.stderr); sys.exit(1)

    # 0) Conecta y asegura tabla/vista base
    conn = psycopg2.connect(DATABASE_URL)
    ensure_base_tables(conn)

    # 1) Determinar seasons objetivo
    season_ids = list(OVERRIDE_SEASON_IDS)
    if not season_ids:
        comp_ids = list(OVERRIDE_COMPETITIONS)
        if not comp_ids:
            comps = discover_atp_gs_competitions()
            comp_ids = [c["id"] for c in comps]
            print(f"[INFO] Competitions detectadas (ATP+GS): {len(comp_ids)}")
        else:
            print(f"[INFO] Competitions provistas: {len(comp_ids)}")

        seen = set()
        for cid in comp_ids:
            try:
                seas = seasons_2025_for_competition(cid)
                for s in seas:
                    if s["id"] not in seen:
                        season_ids.append(s["id"])
                        seen.add(s["id"])
            except Exception as e:
                print(f"[WARN] No se pudo leer seasons de {cid}: {e}")
            time.sleep(RATE_SLEEP)
        season_ids = sorted(set(season_ids))

    print(f"[INFO] Seasons 2025 a cargar: {len(season_ids)}")

    # 2) Descargar summaries y upsert
    total = 0
    for sid in season_ids:
        try:
            rows = fetch_season_summaries(sid)
            n = upsert_matches_long_base(conn, rows)
            total += n
            print(f"[OK] {sid}: {n} filas long (2 por partido) upserted")
        except Exception as e:
            print(f"[ERR] {sid}: {e}")
        time.sleep(RATE_SLEEP)

    conn.close()
    print(f"[DONE] Total filas long insertadas/actualizadas: {total}")

if __name__ == "__main__":
    main()
