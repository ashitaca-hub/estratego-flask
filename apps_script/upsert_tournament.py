import os
import sys
import requests
import json
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

def tournament_exists(tourney_id):
    url = f"{SUPABASE_URL}/rest/v1/tournaments?tourney_id=eq.{tourney_id}"
    res = requests.get(url, headers=HEADERS)
    return res.ok and len(res.json()) > 0

def find_previous_tournament(code):
    url = f"{SUPABASE_URL}/rest/v1/tournaments?tourney_id=like=%.{code}&order=tourney_date.desc&limit=1"
    res = requests.get(url, headers=HEADERS)
    if res.ok and len(res.json()) == 1:
        return res.json()[0]
    return None

def insert_tournament(tourney_id, base):
    year = int(tourney_id.split("-")[0])
    payload = {
        "tourney_id": tourney_id,
        "name": base["name"],
        "level": base["level"],
        "surface": base["surface"],
        "draw_size": base["draw_size"],
        "tourney_date": int(f"{year}0930")  # Aproximamos a fin de septiembre
    }
    url = f"{SUPABASE_URL}/rest/v1/tournaments"
    res = requests.post(url, headers=HEADERS, data=json.dumps(payload))
    if not res.ok:
        print("Error insertando torneo:", res.status_code, res.text)
        res.raise_for_status()
    else:
        print(f"✅ Torneo {tourney_id} insertado")

def main():
    if len(sys.argv) < 2:
        print("Uso: python upsert_tournament.py <tourney_id>")
        sys.exit(1)

    tourney_id = sys.argv[1]
    if tournament_exists(tourney_id):
        print(f"✅ Torneo {tourney_id} ya existe")
        return

    code = tourney_id.split("-")[1]
    base = find_previous_tournament(code)
    if not base:
        print(f"❌ No se encontró torneo anterior con código {code}")
        sys.exit(1)

    insert_tournament(tourney_id, base)

if __name__ == "__main__":
    main()
