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

def get_latest_tournament_entry(tourney_code):
    url = f"{SUPABASE_URL}/rest/v1/tournaments?tourney_id=like=%.{tourney_code}&order=tourney_date.desc"
    res = requests.get(url, headers=HEADERS)
    if res.ok and res.json():
        return res.json()[0]
    return None

def insert_tournament(tourney_id, original):
    payload = {
        "tourney_id": tourney_id,
        "name": original["name"],
        "level": original["level"],
        "surface": original["surface"],
        "draw_size": original["draw_size"],
        "tourney_date": int(tourney_id.split("-")[0] + "0901")  # estimado para septiembre
    }
    url = f"{SUPABASE_URL}/rest/v1/tournaments"
    res = requests.post(url, headers=HEADERS, data=json.dumps(payload))
    if not res.ok:
        print("❌ Error al insertar torneo:", res.text)
        res.raise_for_status()
    print("✅ Torneo insertado correctamente")

def main():
    if len(sys.argv) < 2:
        print("Uso: python upsert_tournament.py <tourney_id>")
        sys.exit(1)

    tourney_id = sys.argv[1]  # ej: 2025-747
    year, code = tourney_id.split("-")

    existing = get_latest_tournament_entry(code)
    if not existing:
        print(f"❌ No se encontró torneo anterior con código {code}")
        sys.exit(1)

    insert_tournament(tourney_id, existing)

if __name__ == "__main__":
    main()
