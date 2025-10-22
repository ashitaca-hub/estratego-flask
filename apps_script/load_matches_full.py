#!/usr/bin/env python3
import argparse, csv, os
import logging
import psycopg2
from psycopg2.extras import execute_values

DDL_SCHEMA = "estratego_v1"

def connect(url):
    return psycopg2.connect(url)

def as_int(x):
    if x is None: return None
    s = str(x).strip()
    if s in ("", "NA", "N/A"): return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        logging.warning("Unable to convert %r to int", s)
        return None

def as_float(x):
    try: return float(x)
    except: return None

def norm_hand(val):
    if not val:
        return None
    v = str(val).strip().upper()
    if v in ("RIGHT", "R", "RH"): return "R"
    if v in ("LEFT", "L", "LH"):  return "L"
    if v in ("AMBIDEXTROUS", "AMBIDEXTROXO", "AMBIDEXTRO", "A"): return "A"
    if v in ("U", "UNKNOWN", "NA", "N/A"): return "U"
    return None

def norm_surface(s):
    if not s: return None
    v = str(s).strip()
    if v.lower() in ("indoor hard", "hard indoor"): return "Hard"
    if v.lower() in ("hard", "clay", "grass", "carpet", "unknown"):
        return v.capitalize()
    return "Unknown"

def load_player_name_id_map(cur):
    cur.execute(f"SELECT player_id, name FROM {DDL_SCHEMA}.players")
    return {name.strip(): pid for pid, name in cur.fetchall() if pid is not None and name}

def resolve_player_id(raw_id, name, name_id_map):
    pid = as_int(raw_id)
    if pid is not None:
        return pid
    return name_id_map.get(name.strip())

def upsert_players(cur, rows):
    seen = set()
    batch = []
    for r in rows:
        for side in ("winner", "loser"):
            pid = as_int(r.get(f"{side}_id"))
            if pid is None or pid in seen:
                continue
            seen.add(pid)
            batch.append((
                pid,
                r.get(f"{side}_name"),
                norm_hand(r.get(f"{side}_hand")),
                as_int(r.get(f"{side}_ht")),
                (r.get(f"{side}_ioc") or None)
            ))
    if batch:
        sql = f"""
            INSERT INTO {DDL_SCHEMA}.players(player_id, name, hand, height_cm, ioc)
            VALUES %s
            ON CONFLICT (player_id) DO UPDATE SET
                name = EXCLUDED.name,
                hand = COALESCE(EXCLUDED.hand, {DDL_SCHEMA}.players.hand),
                height_cm = COALESCE(EXCLUDED.height_cm, {DDL_SCHEMA}.players.height_cm),
                ioc = COALESCE(EXCLUDED.ioc, {DDL_SCHEMA}.players.ioc)
        """
        execute_values(cur, sql, batch, page_size=1000)

def upsert_matches_full(cur, rows, name_id_map, dry_run=False):
    m_rows, skipped = [], 0
    for r in rows:
        try:
            tid = r.get('tourney_id')
            mnum = as_int(r.get('match_num'))
            wid = resolve_player_id(r.get("winner_id"), r.get("winner_name"), name_id_map)
            lid = resolve_player_id(r.get("loser_id"), r.get("loser_name"), name_id_map)
            if not tid or mnum is None or wid is None or lid is None:
                skipped += 1
                continue
            m_rows.append((
                tid, r.get("tourney_name"), norm_surface(r.get("surface")), as_int(r.get("draw_size")),
                r.get("tourney_level"), r.get("tourney_date")[:4] + '-' + r.get("tourney_date")[4:6] + '-' + r.get("tourney_date")[6:],
                mnum, wid, r.get("winner_seed"), r.get("winner_entry"), r.get("winner_name"), norm_hand(r.get("winner_hand")),
                as_int(r.get("winner_ht")), r.get("winner_ioc"), as_float(r.get("winner_age")),
                lid, r.get("loser_seed"), r.get("loser_entry"), r.get("loser_name"), norm_hand(r.get("loser_hand")),
                as_int(r.get("loser_ht")), r.get("loser_ioc"), as_float(r.get("loser_age")),
                r.get("score"), as_int(r.get("best_of")), r.get("round"), as_int(r.get("minutes")),
                as_int(r.get("w_ace")), as_int(r.get("w_df")), as_int(r.get("w_svpt")), as_int(r.get("w_1stIn")),
                as_int(r.get("w_1stWon")), as_int(r.get("w_2ndWon")), as_int(r.get("w_SvGms")), as_int(r.get("w_bpSaved")), as_int(r.get("w_bpFaced")),
                as_int(r.get("l_ace")), as_int(r.get("l_df")), as_int(r.get("l_svpt")), as_int(r.get("l_1stIn")),
                as_int(r.get("l_1stWon")), as_int(r.get("l_2ndWon")), as_int(r.get("l_SvGms")), as_int(r.get("l_bpSaved")), as_int(r.get("l_bpFaced")),
                as_int(r.get("winner_rank")), as_int(r.get("winner_rank_points")),
                as_int(r.get("loser_rank")), as_int(r.get("loser_rank_points"))
            ))
        except Exception as e:
            logging.error(f"Error procesando fila: {e}")
            skipped += 1

    if not dry_run and m_rows:
        execute_values(cur, f"""
            INSERT INTO {DDL_SCHEMA}.matches_full (
              tourney_id, tourney_name, surface, draw_size, tourney_level, tourney_date, match_num,
              winner_id, winner_seed, winner_entry, winner_name, winner_hand, winner_ht, winner_ioc, winner_age,
              loser_id, loser_seed, loser_entry, loser_name, loser_hand, loser_ht, loser_ioc, loser_age,
              score, best_of, round, minutes,
              w_ace, w_df, w_svpt, w_1stIn, w_1stWon, w_2ndWon, w_SvGms, w_bpSaved, w_bpFaced,
              l_ace, l_df, l_svpt, l_1stIn, l_1stWon, l_2ndWon, l_SvGms, l_bpSaved, l_bpFaced,
              winner_rank, winner_rank_points, loser_rank, loser_rank_points
            )
            VALUES %s
            ON CONFLICT DO NOTHING;
        """, m_rows, page_size=1000)

    return len(m_rows), skipped

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Ruta al CSV atp_matches_2024.csv")
    ap.add_argument("--dburl", default=os.getenv("DATABASE_URL"), help="DATABASE_URL de Postgres")
    ap.add_argument("--dry-run", action="store_true", help="Validar sin insertar datos")
    args = ap.parse_args()
    if not args.dburl:
        raise SystemExit("DATABASE_URL no especificado (usa --dburl o variable de entorno).")

    conn = connect(args.dburl)
    try:
        with conn:
            with conn.cursor() as cur:
                name_id_map = load_player_name_id_map(cur)
                batch = []
                inserted, skipped = 0, 0
                with open(args.csv, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        batch.append(row)
                        if len(batch) >= 1000:
                            upsert_players(cur, batch)
                            ins, skip = upsert_matches_full(cur, batch, name_id_map, dry_run=args.dry_run)
                            inserted += ins
                            skipped += skip
                            batch.clear()
                    if batch:
                        upsert_players(cur, batch)
                        ins, skip = upsert_matches_full(cur, batch, name_id_map, dry_run=args.dry_run)
                        inserted += ins
                        skipped += skip
                print(f"✅ {inserted} partidos insertados desde {args.csv}")
                if skipped:
                    print(f"⚠️  {skipped} partidos ignorados por datos incompletos")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
