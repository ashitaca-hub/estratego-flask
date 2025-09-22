# apps_script/load_from_staging.py

import requests
import os
import datetime

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

STAGING_TABLE = "stg_draw_entries_by_name"
TARGET_TABLE = "draw_entries"
PLAYERS_TABLE = "players_dim"

TOURNEY_ID = 329  # se puede parametrizar


def fetch_staging():
    url = f"{SUPABASE_URL}/rest/v1/{STAGING_TABLE}?tourney_id=eq.{TOURNEY_ID}&processed_at=is.null"
    res = requests.get(url, headers=HEADERS)
    res.raise_for_status()
    return res.json()


def find_player_id(player_name):
    # Coincidencia exacta por nombre
    url = f"{SUPABASE_URL}/rest/v1/{PLAYERS_TABLE}?name=eq.{player_name}"
    res = requests.get(url, headers=HEADERS)
    res.raise_for_status()
    data = res.json()
    if data:
        return data[0]["player_id"]
    return None


def insert_draw_entries(records):
    url = f"{SUPABASE_URL}/rest/v1/{TARGET_TABLE}"
    res = requests.post(url, headers={**HEADERS, "Prefer": "return=representation"}, json=records)
    res.raise_for_status()
    return res.json()


def mark_processed(ids):
    now = datetime.datetime.utcnow().isoformat()
    for row_id in ids:
        url = f"{SUPABASE_URL}/rest/v1/{STAGING_TABLE}?tourney_id=eq.{TOURNEY_ID}&pos=eq.{row_id}"
        res = requests.patch(url, headers=HEADERS, json={"processed_at": now})
        res.raise_for_status()


if __name__ == "__main__":
    staging_rows = fetch_staging()
    if not staging_rows:
        print("No hay registros nuevos en staging.")
        exit(0)

    draw_entries = []
    processed_ids = []

    for row in staging_rows:
        player_id = None
        if row["player_name"] not in ("", None):
            player_id = find_player_id(row["player_name"])

        draw_entries.append({
            "tourney_id": TOURNEY_ID,
            "pos": row["pos"],
            "player_id": player_id,
            "seed": row["seed"],
            "tag": row["tag"]
        })
        processed_ids.append(row["pos"])

    inserted = insert_draw_entries(draw_entries)
    print(f"Insertados {len(inserted)} registros en draw_entries.")

    mark_processed(processed_ids)
    print("Marcados como procesados en staging.")
