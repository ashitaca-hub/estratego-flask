# apps_script/load_draw_to_staging.py

import pandas as pd
import requests
import os
import sys

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

STAGING_TABLE = "stg_draw_entries_by_name"

# ⚡ Ajusta aquí el torneo
TOURNEY_ID = "2025-329"

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python load_draw_to_staging.py <csv_file>")
        sys.exit(1)

    csv_file = sys.argv[1]
    df = pd.read_csv(csv_file)

    # Asegurar tipos
    if "seed" in df.columns:
        df["seed"] = df["seed"].astype("Int64")

    # Convertir NaN → None
    entries = df.replace({pd.NA: None}).where(pd.notnull(df), None).to_dict(orient="records")

    # Añadir tourney_id a cada fila
    for e in entries:
        e["tourney_id"] = TOURNEY_ID

    # Subir a Supabase REST
    url = f"{SUPABASE_URL}/rest/v1/{STAGING_TABLE}"
    res = requests.post(
        url,
        headers={**HEADERS, "Prefer": "resolution=merge-duplicates"},
        json=entries,
    )

    if res.status_code >= 200 and res.status_code < 300:
        print(f"Cargado CSV con {len(entries)} filas a staging para torneo {TOURNEY_ID}.")
    else:
        print(f"Error al insertar: {res.status_code}\n{res.text}")
        res.raise_for_status()
