# scripts/load_draw_to_staging.py

import sys
import pandas as pd
import requests
import os

CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "data/draw_329.csv"
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
TABLE_NAME = "stg_draw_entries_by_name"

# tourney_id en formato año-id
tourney_id = "2025-329"

# Leer CSV
entries = pd.read_csv(CSV_PATH)
entries.fillna("", inplace=True)
entries["seed"] = entries["seed"].replace("", None)
entries["tag"] = entries["tag"].replace("", None)
entries["tourney_id"] = tourney_id

# Convertir a registros
records = entries.to_dict(orient="records")

# Headers comunes
headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

# DELETE previo para evitar duplicados
delete_url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}?tourney_id=eq.{tourney_id}"
delete_res = requests.delete(delete_url, headers=headers)
if delete_res.status_code >= 200 and delete_res.status_code < 300:
    print(f"Eliminadas filas existentes para tourney_id={tourney_id}.")
else:
    print(f"Error al eliminar: {delete_res.status_code}\n{delete_res.text}")
    delete_res.raise_for_status()

# Insertar nueva data
insert_url = f"{SUPABASE_URL}/rest/v1/{TABLE_NAME}"
headers["Prefer"] = "return=representation"
insert_res = requests.post(insert_url, json=records, headers=headers)

if insert_res.status_code >= 200 and insert_res.status_code < 300:
    print(f"Cargado CSV con {len(records)} filas a staging.")
else:
    print(f"Error al insertar: {insert_res.status_code}\n{insert_res.text}")
    insert_res.raise_for_status()
