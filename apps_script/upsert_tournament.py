import os
import sys
import requests
import json

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

def extract_code(tourney_id):
    return tourney_id.split("-")[-1]

def fetch_previous_tournament(tourney_id):
    code = extract_code(tourney_id)
    year = int(tourney_id.split("-")[0])
    url = f"{SUPABASE_URL}/rest/v1/tournaments?tourney_id=like.*-{code}&select=*&order=tourney_id.desc"
    res = requests.get(url, headers=HEADERS)
    if not res.ok:
        raise Exception(f"Error buscando torneos: {res.text}")

    for row in res.json():
        y = int(row["tourney_id"].split("-")[0])
        if y < year:
            return row

    return None

def upsert_tournament(new_id, template):
    payload = {
        "tourney_id": new_id,
        "name": template["name"],
        "level": template["level"],
        "surface": template["surface"],
        "draw_size": template["draw_size"],
        "tourney_date": int(new_id.split("-")[0] + "01"),
    }
    url = f"{SUPABASE_URL}/rest/v1/tournaments"
    headers = HEADERS.copy()
    headers["Prefer"] = "resolution=merge-duplicates"
    res = requests.post(url, headers=headers, data=json.dumps(payload))
    if not res.ok:
        raise Exception(f"Error insertando torneo: {res.text}")

def main():
    if len(sys.argv) < 2:
        print("Uso: python upsert_tournament.py <tourney_id>")
        sys.exit(1)

    new_id = sys.argv[1]
    print(f"🔍 Buscando plantilla para {new_id}...")

    prev = fetch_previous_tournament(new_id)
    if not prev:
        print("❌ No se encontró torneo anterior con ese código")
        sys.exit(1)

    print(f"✅ Usando {prev['tourney_id']} como plantilla")
    upsert_tournament(new_id, prev)
    print(f"🎾 Torneo {new_id} insertado correctamente")

if __name__ == "__main__":
    main()
