import os
import sys
import requests
import json
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}


def fetch_staging(tourney_id):
    url = f"{SUPABASE_URL}/rest/v1/stg_draw_entries_by_name?tourney_id=eq.{tourney_id}"
    res = requests.get(url, headers=HEADERS)
    if not res.ok:
        print("Error al consultar staging:", res.status_code, res.text)
        res.raise_for_status()
    return res.json()


def clear_existing_draw_entries(tourney_id):
    print(f"🧹 Borrando draw_entries y draw_matches para torneo {tourney_id}...")
    for table in ["draw_matches", "draw_entries"]:
        url = f"{SUPABASE_URL}/rest/v1/{table}?tourney_id=eq.{tourney_id}"
        res = requests.delete(url, headers=HEADERS)
        if not res.ok:
            print(f"Error borrando {table}: {res.status_code} {res.text}")
            res.raise_for_status()


def resolve_player_id(player_name):
    if not player_name:
        return None

    name_variants = [player_name.strip()]
    if "," in player_name:
        parts = player_name.split(",", 1)
        name_variants.append(f"{parts[1].strip()} {parts[0].strip()}")

    for variant in name_variants:
        url = f"{SUPABASE_URL}/rest/v1/players_dim?name=ilike.{variant}"
        res = requests.get(url, headers=HEADERS)
        if res.ok and len(res.json()) == 1:
            return res.json()[0]["player_id"]

    return None


def insert_draw_entry(tourney_id, row, player_id):
    payload = {
        "tourney_id": tourney_id,
        "pos": row["pos"],
        "player_id": player_id,
        "seed": row.get("seed"),
        "tag": row.get("tag") or ("UNRESOLVED" if not player_id else None)
    }
    url = f"{SUPABASE_URL}/rest/v1/draw_entries"
    res = requests.post(url, headers=HEADERS, data=json.dumps(payload))
    if not res.ok:
        print(f"Error insertando draw_entry en pos {row['pos']}: {res.status_code} {res.text}")
        res.raise_for_status()


def mark_as_processed(tourney_id, pos):
    now = datetime.utcnow().isoformat()
    url = f"{SUPABASE_URL}/rest/v1/stg_draw_entries_by_name?tourney_id=eq.{tourney_id}&pos=eq.{pos}"
    payload = {"processed_at": now}
    res = requests.patch(url, headers=HEADERS, data=json.dumps(payload))
    if not res.ok:
        print(f"Error marcando como procesado (tourney={tourney_id}, pos={pos}):", res.text)


def main():
    if len(sys.argv) < 2:
        print("Uso: python load_from_staging.py <tourney_id>")
        sys.exit(1)

    tourney_id = sys.argv[1]
    clear_existing_draw_entries(tourney_id)
    staging_rows = fetch_staging(tourney_id)

    print(f"📥 Filas a procesar para torneo {tourney_id}: {len(staging_rows)}")

    for row in staging_rows:
        player_id = resolve_player_id(row.get("player_name"))
        insert_draw_entry(tourney_id, row, player_id)
        mark_as_processed(tourney_id, row["pos"])

        if not player_id and not row.get("tag"):
            print(f"[!] No se pudo resolver: {row['player_name']} (pos {row['pos']})")

    print("✅ Migración completada.")


if __name__ == "__main__":
    main()
