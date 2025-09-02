# load_rankings_sportradar.py
import os, re, requests, psycopg2
from psycopg2.extras import execute_values
from datetime import date

SR_API_KEY   = os.getenv("SR_API_KEY", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
BASE_URL     = "https://api.sportradar.com/tennis/trial/v3/en"

def sr_get(path):
    r = requests.get(f"{BASE_URL}{path}",
                     headers={"accept":"application/json","x-api-key":SR_API_KEY},
                     timeout=30)
    r.raise_for_status()
    return r.json()

def digits(s):
    m = re.sub(r"\D", "", s or "")
    return int(m) if m else None

def fetch_atp_rankings():
    data = sr_get("/rankings.json")
    # buscamos bloque "ATP" masculino (type_id=1/name='ATP'/gender='men')
    for blk in data.get("rankings", []):
        if (blk.get("name") == "ATP") and ((blk.get("gender") or "").lower() == "men"):
            year = int(blk.get("year"))
            week = int(blk.get("week"))
            rows = []
            for cr in blk.get("competitor_rankings", []):
                rank   = cr.get("rank")
                points = cr.get("points")
                comp   = cr.get("competitor") or {}
                pid    = digits(comp.get("id"))  # sr:competitor:225050 → 225050
                if pid and rank:
                    rows.append((date.today().isoformat(), year, week, pid, int(rank), points))
            return rows, year, week
    return [], None, None

UPSERT_SQL = """
insert into public.rankings_snapshot_int
  (snapshot_date, year, week, player_id, rank, points)
values %s
on conflict (snapshot_date, player_id) do update set
  rank = excluded.rank,
  points = excluded.points,
  updated_at = now();
"""

def main():
    assert SR_API_KEY, "Falta SR_API_KEY"
    assert DATABASE_URL, "Falta DATABASE_URL"

    rows, year, week = fetch_atp_rankings()
    if not rows:
        print("No se encontraron rankings ATP.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    with conn, conn.cursor() as cur:
        # aseguramos la tabla (por si aún no existe)
        cur.execute("""
        create table if not exists public.rankings_snapshot_int (
          snapshot_date date not null,
          year int not null,
          week int not null,
          player_id int not null,
          rank int not null,
          points int,
          source text default 'sportradar',
          created_at timestamptz default now(),
          updated_at timestamptz default now(),
          primary key (snapshot_date, player_id)
        );
        create index if not exists rankings_snapshot_int_week_idx
          on public.rankings_snapshot_int (year, week);
        """)
        execute_values(cur, UPSERT_SQL, rows, page_size=2000)
    conn.close()
    print(f"OK: {len(rows)} filas insertadas/actualizadas para {year}-W{week}")

if __name__ == "__main__":
    main()
