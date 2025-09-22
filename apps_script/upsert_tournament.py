# apps_script/upsert_tournament.py

import requests
import os

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

TOURNAMENTS_TABLE = "tournaments"

# Datos del torneo Tokyo 2025
tournament = {
    "tourney_id": "2025-329",
    "name": "Kinoshita Group Japan Open Tennis Championships",
    "surface": "Hard",
    "location": "Tokyo, Japan",
    "date_start": "2025-09-24",
    "date_end": "2025-09-30"
}

url = f"{SUPABASE_URL}/rest/v1/{TOURNAMENTS_TABLE}"
res = requests.post(url, headers=HEADERS, json=[tournament])

if res.status_code >= 200 and res.status_code < 300:
    print("Torneo upserted correctamente:", res.json())
else:
    print(f"Error al upsert torneo: {res.status_code}\n{res.text}")
    res.raise_for_status()
