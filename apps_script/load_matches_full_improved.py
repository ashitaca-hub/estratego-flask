#!/usr/bin/env python3
import argparse, csv, os
import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

DDL_SCHEMA = "estratego_v1"
IGNORED_OUTPUT = "ignored_matches.csv"

def connect(url):
    return psycopg2.connect(url)

def as_int(x):
    if x is None: return None
    s = str(x).strip()
    if s in ("", "NA", "N/A"): return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
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
    cur.execute(f"SELECT player_id, name FROM {DDL_SCHEMA}.players ORDER BY player_id")
    name_to_ids = {}
    for pid, name in cur.fetchall():
        if pid is None or not name:
            continue
        name_to_ids.setdefault(name.strip(), []).append(pid)
    name_id_map = {}
    for name, ids in name_to_ids.items():
        if len(ids) > 1:
            logging.warning("Nombre ambiguo en %s.players para %r: ids %s (se ignora, no se resolvera por nombre)",
                             DDL_SCHEMA, name, ids)
            continue
        name_id_map[name] = ids[0]
    return name_id_map

def resolve_from_mapping(cur, raw_id, name):
    csv_id = str(raw_id).strip() if raw_id not in (None, "") else None

    # 1) ext_atp_id: id oficial de la ATP (ej. "DH50"), permanente y sin
    #    ambiguedad - es la fuente de verdad una vez rellenado.
    if csv_id is not None:
        cur.execute(f"SELECT player_id FROM {DDL_SCHEMA}.players WHERE ext_atp_id = %s", (csv_id,))
        row = cur.fetchone()
        if row:
            return row[0]

        # 2) cache historico (csv_id -> player_id) de resoluciones por nombre previas
        cur.execute(f"""
            SELECT resolved_player_id FROM {DDL_SCHEMA}.player_name_map
            WHERE csv_id = %s
        """, (csv_id,))
        row = cur.fetchone()
        if row:
            resolved_id = row[0]
            cur.execute(f"""
                UPDATE {DDL_SCHEMA}.players SET ext_atp_id = %s
                WHERE player_id = %s AND ext_atp_id IS NULL
            """, (csv_id, resolved_id))
            return resolved_id

    # 3) fallback: match exacto por nombre (solo si no es ambiguo). Se trata el
    #    guion como un espacio en ambos lados: el CSV de origen escribe algunos
    #    apellidos con guion ("Auger-Aliassime") mientras que estratego_v1.players
    #    los tiene guardados con espacio ("Auger Aliassime"), y un match exacto
    #    sin esta normalizacion falla en silencio para siempre (el jugador nunca
    #    consigue ext_atp_id y sus partidos quedan en ignored_matches.csv).
    cur.execute(
        f"""
        SELECT player_id FROM {DDL_SCHEMA}.players
        WHERE lower(replace(name, '-', ' ')) = lower(replace(%s, '-', ' '))
        ORDER BY player_id
        """,
        (name.strip(),),
    )
    rows = cur.fetchall()
    if not rows:
        return None
    if len(rows) > 1:
        logging.warning("Nombre ambiguo %r: ids %s (no se crea mapping automatico, requiere resolucion manual)",
                         name.strip(), [r[0] for r in rows])
        return None
    resolved_id = rows[0][0]
    if csv_id is not None:
        cur.execute(f"""
            INSERT INTO {DDL_SCHEMA}.player_name_map (csv_id, player_name, resolved_player_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (csv_id) DO NOTHING
        """, (csv_id, name.strip(), resolved_id))
        cur.execute(f"""
            UPDATE {DDL_SCHEMA}.players SET ext_atp_id = %s
            WHERE player_id = %s AND ext_atp_id IS NULL
        """, (csv_id, resolved_id))
    return resolved_id

def resolve_player_id(raw_id, name, name_id_map, cur):
    try:
        int_id = int(str(raw_id))
        if int_id in name_id_map.values():
            return int_id
    except:
        pass
    return resolve_from_mapping(cur, raw_id, name)

def new_player_id(cur):
    cur.execute(f"SELECT nextval('{DDL_SCHEMA}.players_player_id_seq')")
    return cur.fetchone()[0]

def upsert_players(cur, rows):
    seen = set()
    batch = []
    for r in rows:
        for side in ("winner", "loser"):
            raw_id = r.get(f"{side}_id")
            name = r.get(f"{side}_name")
            if not name:
                continue
            csv_id = str(raw_id).strip() if raw_id not in (None, "") else None
            pid = resolve_from_mapping(cur, raw_id, name)
            is_new = pid is None
            if is_new:
                pid = new_player_id(cur)
                logging.info("Jugador nuevo: %r (ext_atp_id=%s) -> player_id=%s", name.strip(), csv_id, pid)
            if pid in seen:
                continue
            seen.add(pid)
            batch.append((
                pid,
                name,
                norm_hand(r.get(f"{side}_hand")),
                as_int(r.get(f"{side}_ht")),
                (r.get(f"{side}_ioc") or None),
                csv_id,
            ))
    if batch:
        sql = f"""
            INSERT INTO {DDL_SCHEMA}.players(player_id, name, hand, height_cm, ioc, ext_atp_id)
            VALUES %s
            ON CONFLICT (player_id) DO UPDATE SET
                name = EXCLUDED.name,
                hand = COALESCE(EXCLUDED.hand, {DDL_SCHEMA}.players.hand),
                height_cm = COALESCE(EXCLUDED.height_cm, {DDL_SCHEMA}.players.height_cm),
                ioc = COALESCE(EXCLUDED.ioc, {DDL_SCHEMA}.players.ioc),
                ext_atp_id = COALESCE({DDL_SCHEMA}.players.ext_atp_id, EXCLUDED.ext_atp_id)
        """
        execute_values(cur, sql, batch, page_size=1000)

def upsert_matches_full(cur, rows, name_id_map, dry_run=False):
    m_rows, snapshot_rows, ignored = [], [], []
    skipped = 0
    already_loaded = 0

    # matches_full no tiene una restriccion unica real, asi que sin esta guarda
    # volver a cargar un CSV ya procesado (p.ej. para rellenar partidos que
    # antes fallaron por nombre) duplicaria TODO lo que ya estaba bien cargado.
    tourney_ids = sorted({r.get("tourney_id") for r in rows if r.get("tourney_id")})
    existing = set()
    if tourney_ids:
        cur.execute(
            f"SELECT tourney_id, match_num FROM {DDL_SCHEMA}.matches_full WHERE tourney_id = ANY(%s)",
            (tourney_ids,),
        )
        existing = {(t, m) for t, m in cur.fetchall()}

    for r in rows:
        try:
            tid = r.get('tourney_id')
            mnum = as_int(r.get('match_num'))
            if tid and (tid, mnum) in existing:
                already_loaded += 1
                continue
            wid = resolve_player_id(r.get("winner_id"), r.get("winner_name"), name_id_map, cur)
            lid = resolve_player_id(r.get("loser_id"), r.get("loser_name"), name_id_map, cur)
            reason = None
            if not tid:
                reason = "tourney_id missing"
            elif wid is None:
                reason = "winner_id not resolved"
            elif lid is None:
                reason = "loser_id not resolved"
            if reason:
                r["reason"] = reason
                ignored.append(r)
                skipped += 1
                continue

            match_id = f"{r['tourney_date'][:4]}_{tid}_{mnum or 0}"
            w_rank, w_rank_pts = as_int(r.get("winner_rank")), as_int(r.get("winner_rank_points"))
            l_rank, l_rank_pts = as_int(r.get("loser_rank")), as_int(r.get("loser_rank_points"))

            if wid and w_rank is not None:
                snapshot_rows.append((wid, match_id, w_rank, w_rank_pts, 'winner'))
            if lid and l_rank is not None:
                snapshot_rows.append((lid, match_id, l_rank, l_rank_pts, 'loser'))

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
                w_rank, w_rank_pts, l_rank, l_rank_pts
            ))
        except Exception as e:
            r["reason"] = f"unknown error: {str(e)}"
            ignored.append(r)
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

    if snapshot_rows:
        execute_values(cur, f"""
            INSERT INTO {DDL_SCHEMA}.rankings_snapshot_v2 (
              player_id, match_id, rank, rank_points, side
            )
            VALUES %s
            ON CONFLICT DO NOTHING;
        """, snapshot_rows, page_size=1000)

    if ignored:
        fieldnames = list(ignored[0].keys())
        with open(IGNORED_OUTPUT, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(ignored)

    return len(m_rows), skipped, already_loaded

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
                inserted, skipped, already = 0, 0, 0
                with open(args.csv, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        batch.append(row)
                        if len(batch) >= 1000:
                            upsert_players(cur, batch)
                            ins, skip, alr = upsert_matches_full(cur, batch, name_id_map, dry_run=args.dry_run)
                            inserted += ins
                            skipped += skip
                            already += alr
                            batch.clear()
                    if batch:
                        upsert_players(cur, batch)
                        ins, skip, alr = upsert_matches_full(cur, batch, name_id_map, dry_run=args.dry_run)
                        inserted += ins
                        skipped += skip
                        already += alr
                print(f"✅ {inserted} partidos insertados desde {args.csv}")
                if already:
                    print(f"ℹ️  {already} partidos ya estaban cargados (omitidos, no duplicados)")
                if skipped:
                    print(f"⚠️  {skipped} partidos ignorados por datos incompletos (ver {IGNORED_OUTPUT})")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
