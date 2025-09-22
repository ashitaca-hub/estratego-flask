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
    "Content-Type": "application/json"
}

STAGING_TABLE = "stg_draw_entries_by_name"


def parse_row(row):
    """
    Parse una fila del CSV exportado desde el PDF.
    Formato típico:
    1 1 ALCARAZ, Carlos ESP
    2 BAEZ, Sebastian ARG
    7 Qualifier
    10 WC MOCHIZUKI, Shintaro JPN
    """
    tokens = row.strip().split()
    if not tokens:
        return None

    pos = tokens[0]
    seed = None
    tag = None
    player_name = None
    country = None

    # Caso: solo posición + tag (Qualifier, BYE)
    if len(tokens) == 2 and tokens[1].isalpha():
        tag = tokens[1]
        return {"pos": int(pos), "player_name": None, "seed": None, "tag": tag, "country": None}

    # Si el segundo token es numérico => seed
    idx = 1
    if tokens[1].isdigit():
        seed = tokens[1]
        idx = 2
    # Si el segundo token es un tag especial
    elif tokens[1] in ["WC", "Qualifier", "BYE"]:
        tag = tokens[1]
        idx = 2

    # El último token puede ser país (3 letras mayúsculas)
    if len(tokens[-1]) == 3 and tokens[-1].isupper():
        country = tokens[-1]
        name_tokens = tokens[idx:-1]
    else:
        name_tokens = tokens[idx:]

    player_name = " ".join(name_tokens).replace(" ,", ",")

    return {
        "pos": int(pos),
        "player_name": player_name if player_name else None,
        "seed": int(seed) if seed else None,
        "tag": tag,
        "country": country
    }


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python load_draw_to_staging.py <csv_file>")
        sys.exit(1)

    csv_file = sys.argv[1]
    df = pd.read_csv(csv_file)

    entries = []
    for _, row in df.iterrows():
        parsed = parse_row(str(row["player_name"]))
        if parsed:
            entries.append(parsed)

    # Subir a Supabase REST
    url = f"{SUPABASE_URL}/rest/v1/{STAGING_TABLE}"
    res = requests.post(url, headers={**HEADERS, "Prefer": "resolution=merge-duplicates"}, json=entries)

    if res.status_code >= 200 and res.status_code < 300:
        print(f"Cargado CSV con {len(entries)} filas a staging.")
    else:
        print(f"Error al insertar: {res.status_code}\n{res.text}")
        res.raise_for_status()
