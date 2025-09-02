# poblar_2025_sportradar.py
import os, time, json, re
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

CONFIG = {
    "SR_API_KEY": os.getenv("SR_API_KEY", "TU_KEY_AQUI"),
    "BASE_URL": "https://api.sportradar.com/tennis/trial/v3/en",
    # OJO: pon aquí season_ids 2025 que quieras cargar
    "SEASON_IDS": [
        # "sr:season:XXXXX",
    ],
    # Alternativa: descubrir seasons a partir de competitions (si los sabes)
    "COMPETITIONS": [
        # "sr:competition:XXXX",  # opcional
    ],
    "DB_DSN": os.getenv("DATABASE_URL", "postgresql://user:pass@host:5432/dbname"),
    "RATE_SLEEP": 0.35,  # ~3 req/seg
}

def digits(s):
    m = re.sub(r"\D", "", s or "")
    return int(m) if m else None

def sr_get(path):
    url = f"{CONFIG['BASE_URL']}{path}"
    headers = {"accept": "application/json", "x-api-key": CONFIG["SR_API_KEY"]}
    r = requests.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"SR error {r.status_code}: {r.text[:200]}")
    return r.json()

def discover_seasons_for_2025():
    season_ids = set(CONFIG["SEASON_IDS"])
    for comp_id in CONFIG["COMPETITIONS"]:
        data = sr_get(f"/competitions/{comp_id}/seasons.json")
        for s in data.get("seasons", []):
            # Heurística: coge temporadas con year >= 2025 o fechas 2025
            yr = s.get("year")
            sid = s.get("id")
            if yr and int(yr) == 2025:
                season_ids.add(sid)
    return list(season_ids)

def fetch_season_summaries(season_id):
    data = sr_get(f"/seasons/{season_id}/summaries.json")
    rows = []
    for ev in data.get("summaries", []):
        status = ev.get("sport_event_status", {}).get("status")
        if status not in ("closed", "ended"):  # solo partidos finalizados
            continue

        sport_event = ev.get("sport_event", {})
        start_time = sport_event.get("start_time")  # ISO 8601
        comp = sport_event.get("competition", {}) or {}
        tournament_name = comp.get("name")
        surface = comp.get("surface")  # si viene
        ext_event_id = sport_event.get("id")
        ext_season_id = comp.get("season", {}).get("id") or season_id

        # participantes
        comps = sport_event.get("competitors", []) or []
        ids = [digits(c.get("id")) for c in comps]  # int externos SR
        names = [c.get("name") for c in comps]
        if len(ids) != 2 or None in ids:
            continue  # partidos raros o datos incompletos

        p_id, o_id = ids[0], ids[1]
        # ganador por winner_id
        w_full = ev.get("sport_event_status", {}).get("winner_id")
        w_id = digits(w_full)

        if not start_time or not ext_event_id or not w_id:
            continue

        dt = datetime.fromisoformat(start_time.replace("Z", "+00:00")).date().isoformat()

        # Dos filas (formato long): una para cada jugador
        rows.append((dt, p_id, o_id, w_id, tournament_name, surface, ext_season_id, ext_event_id))
        rows.append((dt, o_id, p_id, w_id, tournament_name, surface, ext_season_id, ext_event_id))

    return rows

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
    season_ids = CONFIG["SEASON_IDS"] or discover_seasons_for_2025()
    if not season_ids:
        raise RuntimeError("No hay season_ids configurados ni COMPETITIONS para descubrirlos.")

    conn = psycopg2.connect(CONFIG["DB_DSN"])
    total_rows = 0
    for sid in season_ids:
        print(f"[INFO] Season {sid} ...")
        try:
            rows = fetch_season_summaries(sid)
            n = upsert_matches_long_base(conn, rows)
            total_rows += n
            print(f"[OK] {sid}: {n} filas (long) upserted")
        except Exception as e:
            print(f"[ERR] {sid}: {e}")
        time.sleep(CONFIG["RATE_SLEEP"])
    conn.close()
    print(f"[DONE] Filas totales (long): {total_rows}")

if __name__ == "__main__":
    main()
