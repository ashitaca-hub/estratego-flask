# poblar_2025_sportradar.py
import os, time, re
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

SR_API_KEY = os.getenv("SR_API_KEY", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
BASE_URL = "https://api.sportradar.com/tennis/trial/v3/en"
RATE_SLEEP = float(os.getenv("RATE_SLEEP", "0.35"))

# Si quieres pasar listas manuales (sobrescriben el descubrimiento):
OVERRIDE_SEASON_IDS = [s.strip() for s in os.getenv("SEASON_IDS_CSV", "").split(",") if s.strip()]
OVERRIDE_COMPETITIONS = [c.strip() for c in os.getenv("COMPETITIONS_CSV", "").split(",") if c.strip()]

def sr_get(path):
    url = f"{BASE_URL}{path}"
    r = requests.get(url, headers={"accept":"application/json", "x-api-key": SR_API_KEY}, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"SR {r.status_code}: {r.text[:200]}")
    return r.json()

def digits(s):
    m = re.sub(r"\D", "", s or "")
    return int(m) if m else None

# ---- 1) DESCUBRIR COMPETITIONS (ATP + GRAND SLAMS) ----
def discover_atp_gs_competitions():
    data = sr_get("/competitions.json")
    comps = data.get("competitions", []) or []

    keep = []
    for c in comps:
        gender = (c.get("gender") or "").lower()           # "men", "women", "mixed", ...
        name = (c.get("name") or "")
        cat = c.get("category") or {}
        cat_name = (cat.get("name") or "")

        # Criterios:
        # - Queremos ATP Tour + Grand Slams (no Challenger, no Doubles, no Mixed).
        # - gender: men (para ATP/GS masculino).
        # Notas: Los GS a veces no indican "ATP" en category; por eso aceptamos "Grand Slam".
        if gender and gender != "men":
            continue
        cn = cat_name.lower()
        nn = name.lower()

        is_grand_slam = ("grand" in cn and "slam" in cn) or any(gs in nn for gs in ["australian open", "roland garros", "wimbledon", "us open"])
        is_atp_tour   = ("atp" in cn and "challenger" not in cn) and ("doubles" not in nn)

        if is_grand_slam or is_atp_tour:
            keep.append({"id": c["id"], "name": name, "category": cat_name, "gender": gender or "n/a"})

    return keep

# ---- 2) DE COMPETITION → SEASONS 2025 ----
def seasons_2025_for_competition(comp_id):
    data = sr_get(f"/competitions/{comp_id}/seasons.json")
    out = []
    for s in data.get("seasons", []):
        year = s.get("year")
        if str(year) == "2025":
            out.append({"id": s["id"], "name": s.get("name"), "year": year})
    return out

# ---- 3) DESCARGAR SUMMARIES DE UNA SEASON ----
def fetch_season_summaries(season_id):
    data = sr_get(f"/seasons/{season_id}/summaries.json")
    rows = []
    for ev in data.get("summaries", []) or []:
        status = ev.get("sport_event_status", {}).get("status")
        if status not in ("closed", "ended"):
            continue

        se = ev.get("sport_event", {}) or {}
        st = se.get("start_time")
        comp = se.get("competition", {}) or {}
        tname = comp.get("name")
        surface = comp.get("surface")
        ext_event_id = se.get("id")
        ext_season_id = comp.get("season", {}).get("id") or season_id

        comps = se.get("competitors", []) or []
        ids = [digits(c.get("id")) for c in comps]
        if len(ids) != 2 or None in ids:
            continue

        winner_full = ev.get("sport_event_status", {}).get("winner_id")
        w_id = digits(winner_full)
        if not st or not ext_event_id or not w_id:
            continue

        dt = datetime.fromisoformat(st.replace("Z", "+00:00")).date().isoformat()
        p_id, o_id = ids[0], ids[1]

        # Formato "long": dos filas por partido
        rows.append((dt, p_id, o_id, w_id, tname, surface, ext_season_id, ext_event_id))
        rows.append((dt, o_id, p_id, w_id, tname, surface, ext_season_id, ext_event_id))
    return rows

# ---- 4) UPSERT EN BD ----
def upsert_matches_long_base(conn, rows):
    if not rows:
        return 0
    sql = """
    insert into public.matches_long_base
      (match_date, player_id, opponent_id, winner_id, tournament_name, surface, ext_season_id, ext_event_id)
    values %s
    on conflict (ext_event_id, player_id) do update set
      opponent_id = excluded.opponent_id,
      winner_id   = excluded.winner_id,
      tournament_name = coalesce(excluded.tournament_name, public.matches_long_base.tournament_name),
      surface     = coalesce(excluded.surface, public.matches_long_base.surface),
      updated_at  = now();
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=2000)
    conn.commit()
    return len(rows)

def main():
    assert SR_API_KEY, "Falta SR_API_KEY"
    assert DATABASE_URL, "Falta DATABASE_URL"

    # Si pasas SEASON_IDS, se usan tal cual. Si no, descubrimos por competitions 2025.
    season_ids = list(OVERRIDE_SEASON_IDS)
    if not season_ids:
        comp_ids = list(OVERRIDE_COMPETITIONS)
        comps = []
        if not comp_ids:
            # Descubrir competitions ATP + Grand Slams automáticamente
            comps = discover_atp_gs_competitions()
            comp_ids = [c["id"] for c in comps]
            print(f"[INFO] Competitions detectadas (ATP+GS): {len(comp_ids)}")
        else:
            print(f"[INFO] Competitions provistas: {len(comp_ids)}")

        for cid in comp_ids:
            try:
                seas = seasons_2025_for_competition(cid)
                for s in seas:
                    season_ids.append(s["id"])
            except Exception as e:
                print(f"[WARN] No se pudo leer seasons de {cid}: {e}")
            time.sleep(RATE_SLEEP)

        # Unicos y ordenados
        season_ids = sorted(set(season_ids))

    print(f"[INFO] Seasons 2025 a cargar: {len(season_ids)}")
    conn = psycopg2.connect(DATABASE_URL)

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
