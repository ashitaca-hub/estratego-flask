# apps_script/load_from_staging.py

import requests
import os
import sys
from datetime import datetime

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

STAGING_TABLE = "stg_draw_entries_by_name"
DRAW_ENTRIES_TABLE = "draw_entries"

TOURNEY_ID = "2025-329"



def fetch_staging():
    # ✅ Supabase necesita is.null.true
    url = f"{SUPABASE_URL}/rest/v1/stg_draw_entries_by_name?tourney_id=eq.{tourney_id}&processed_at=is.null"
    res = requests.get(url, headers=HEADERS)
    res.raise_for_status()
    return res.json()


def reorder_name(name: str) -> str:
    if not name or "," not in name:
        return name
    last, first = [p.strip() for p in name.split(",", 1)]
    return f"{first} {last}"


def resolve_player_id(name: str):
    if not name:
        return None

    variants = [name, reorder_name(name)]
    for variant in variants:
        if not variant:
            continue
        url = f"{SUPABASE_URL}/rest/v1/players_dim?name=ilike.*{variant}*"
        res = requests.get(url, headers=HEADERS)
        res.raise_for_status()
        rows = res.json()
        if rows:
            return rows[0]["player_id"]

    return None


def insert_draw_entries(rows):
    url = f"{SUPABASE_URL}/rest/v1/{DRAW_ENTRIES_TABLE}"
    res = requests.post(
        url,
        headers={**HEADERS, "Prefer": "resolution=merge-duplicates"},
        json=rows,
    )
    if res.status_code >= 200 and res.status_code < 300:
        print(f"✅ Insertados {len(rows)} registros en draw_entries.")
    else:
        print(f"❌ Error al insertar: {res.status_code}\n{res.text}")
        res.raise_for_status()


def mark_processed(ids):
    now = datetime.utcnow().isoformat()
    for pos in ids:
        url = f"{SUPABASE_URL}/rest/v1/{STAGING_TABLE}?tourney_id=eq.{TOURNEY_ID}&pos=eq.{pos}"
        res = requests.patch(
            url,
            headers=HEADERS,
            json={"processed_at": now},
        )
        res.raise_for_status()


if __name__ == "__main__":
    staging_rows = fetch_staging()
    if not staging_rows:
        print("ℹ️ No hay registros nuevos en staging.")
        sys.exit(0)

    print(f"🔄 Procesando {len(staging_rows)} filas desde staging…")

    draw_entries = []
    processed_ids = []

    for row in staging_rows:
        player_id = resolve_player_id(row["player_name"])
        entry = {
            "tourney_id": TOURNEY_ID,
            "pos": row["pos"],
            "player_id": player_id,
            "seed": row["seed"],
            "tag": row["tag"] if row["tag"] else ("UNRESOLVED" if not player_id else None),
        }
        draw_entries.append(entry)
        processed_ids.append(row["pos"])

        print(f" → Pos {row['pos']} | {row['player_name']} → player_id={player_id or '❌ UNRESOLVED'}")

    # limpiar previos
    url = f"{SUPABASE_URL}/rest/v1/{DRAW_ENTRIES_TABLE}?tourney_id=eq.{TOURNEY_ID}"
    del_res = requests.delete(url, headers=HEADERS)
    del_res.raise_for_status()
    print(f"🗑️ Eliminados registros previos en draw_entries para tourney_id={TOURNEY_ID}.")

    # insertar
    insert_draw_entries(draw_entries)

    # marcar staging como procesado
    mark_processed(processed_ids)
    print("✅ Marcadas filas como procesadas en staging.")
