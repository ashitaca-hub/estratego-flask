# apps_script/load_players_from_sr.py
import os, time, json, math
import psycopg2
import psycopg2.extras
import requests

SR_API_KEY = os.environ["SR_API_KEY"]
DATABASE_URL = os.environ["DATABASE_URL"]
SESSION = requests.Session()
SESSION.headers.update({"accept": "application/json", "x-api-key": SR_API_KEY})

def fetch_profile(sr_id: str) -> dict | None:
    # sr_id: 'sr:competitor:407573'
    enc = sr_id.replace(":", "%3A")
    url = f"https://api.sportradar.com/tennis/trial/v3/en/competitors/{enc}/profile.json"
    r = SESSION.get(url, timeout=20)
    if r.status_code == 200:
        return r.json()
    # rate limit simple
    if r.status_code == 429:
        time.sleep(1.0)
        return fetch_profile(sr_id)
    print("WARN fetch_profile", r.status_code, r.text[:200])
    return None

def upsert_player(conn, pid: int, sr_id: str, prof: dict):
    # mapping desde el JSON de SR
    # competitor block
    comp = (prof or {}).get("competitor") or {}
    info = (prof or {}).get("info") or {}
    # Nota: más campos/periods/competitions si los quieras guardar aparte
    row = {
        "player_id": pid,
        "ext_sportradar_id": sr_id,
        "name": comp.get("name"),
        "country": comp.get("country"),
        "country_code": comp.get("country_code"),
        "gender": comp.get("gender"),
        "handedness": info.get("handedness"),
        "date_of_birth": info.get("date_of_birth"),
        "height_cm": info.get("height"),
        "weight_kg": info.get("weight"),
        "pro_year": info.get("pro_year"),
        "highest_rank": info.get("highest_singles_ranking"),
        "highest_rank_date": info.get("highest_singles_ranking_date"),
    }
    with conn.cursor() as cur:
        cur.execute("""
            insert into public.players_dim
            (player_id, ext_sportradar_id, name, country, country_code, gender,
             handedness, date_of_birth, height_cm, weight_kg, pro_year,
             highest_rank, highest_rank_date)
            values
            (%(player_id)s, %(ext_sportradar_id)s, %(name)s, %(country)s, %(country_code)s, %(gender)s,
             %(handedness)s, %(date_of_birth)s, %(height_cm)s, %(weight_kg)s, %(pro_year)s,
             %(highest_rank)s, %(highest_rank_date)s)
            on conflict (player_id) do update set
              ext_sportradar_id = excluded.ext_sportradar_id,
              name              = coalesce(excluded.name, players_dim.name),
              country           = coalesce(excluded.country, players_dim.country),
              country_code      = coalesce(excluded.country_code, players_dim.country_code),
              gender            = coalesce(excluded.gender, players_dim.gender),
              handedness        = coalesce(excluded.handedness, players_dim.handedness),
              date_of_birth     = coalesce(excluded.date_of_birth, players_dim.date_of_birth),
              height_cm         = coalesce(excluded.height_cm, players_dim.height_cm),
              weight_kg         = coalesce(excluded.weight_kg, players_dim.weight_kg),
              pro_year          = coalesce(excluded.pro_year, players_dim.pro_year),
              highest_rank      = coalesce(excluded.highest_rank, players_dim.highest_rank),
              highest_rank_date = coalesce(excluded.highest_rank_date, players_dim.highest_rank_date),
              updated_at        = now();
        """, row)

def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Target: jugadores que aún no tienen name/country_code
            cur.execute("""
                select player_id, ext_sportradar_id
                from public.players_dim
                where (name is null or country_code is null)
                order by player_id
                limit 500;
            """)
            rows = cur.fetchall()

        print(f"[INFO] jugadores pendientes: {len(rows)}")
        # Throttle simple (SR trial)
        for i, r in enumerate(rows, 1):
            pid = r["player_id"]
            srid = r["ext_sportradar_id"] or f"sr:competitor:{pid}"
            prof = fetch_profile(srid)
            if prof:
                upsert_player(conn, pid, srid, prof)
                conn.commit()
            if i % 10 == 0:
                print(f"[INFO] {i}/{len(rows)}")
            time.sleep(0.15)  # ~6-7 req/s, ajusta si hace falta
    finally:
        conn.close()

if __name__ == "__main__":
    main()
