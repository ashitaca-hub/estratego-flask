# scripts/load_draw_to_staging.py

import sys
import pandas as pd
import requests
import os

CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "data/draw_329.csv"
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
TABLE_NAME = "stg_draw_entries_by_name"
TOURNEY_ID = 329

# Leer CSV
entries = pd.read_csv(CSV_PATH)
entries.fillna("", inplace=True)
entries["seed"] = entries["seed"].replace("", None)
entries["tag"] = entries["tag"].replace("", None)
entries["tourney_id"] = TOURNEY_ID

# Convertir a registros
records = entries.to_dict(orient="records")

# Insertar vía REST
url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}"
headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}
response = requests.post(url, json=records, headers=headers)

if response.status_code >= 200 and response.status_code < 300:
    print(f"Cargado CSV con {len(records)} filas a staging.")
else:
    print(f"Error al insertar: {response.status_code}\n{response.text}")
    response.raise_for_status()
