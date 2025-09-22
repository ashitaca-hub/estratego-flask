# apps_script/upsert_tournament.py

import requests
import os

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=representation"
}

TOURNAMENTS_TABLE = "tournaments"

# Datos del torneo Tokyo 2025
tournament = {
    "tourney_id": "2025-329",
    "name": "Kinoshita Group Japan Open Tennis Championships",
    "level": "ATP500",
    "surface": "Hard",
    "draw_size": 32,
    "tourney_date": 20250924
}

url = f"{SUPABASE_URL}/rest/v1/{TOURNAMENTS_TABLE}"
res = requests.post(url, headers=HEADERS, json=[tournament])

if res.status_code >= 200 and res.status_code < 300:
    try:
        print("Torneo upserted correctamente:", res.json())
    except Exception:
        print("Torneo upserted correctamente (sin respuesta JSON).")
else:
    print(f"Error al upsert torneo: {res.status_code}\n{res.text}")
    res.raise_for_status()
