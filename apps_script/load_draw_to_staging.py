# scripts/load_draw_to_staging.py

import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os

CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "data/draw_329.csv"
DB_URL = os.environ["SUPABASE_DB_URL"]

conn = psycopg2.connect(DB_URL)
cursor = conn.cursor()

# Leer CSV
entries = pd.read_csv(CSV_PATH)

# Normalizar datos
entries.fillna("", inplace=True)
entries["seed"] = entries["seed"].replace("", None)
entries["tag"] = entries["tag"].replace("", None)

# Borrar staging previo
cursor.execute("TRUNCATE estratego_v1.stg_draw_entries_by_name")

# Insertar
values = list(entries.itertuples(index=False, name=None))
execute_values(
    cursor,
    """
    INSERT INTO estratego_v1.stg_draw_entries_by_name (pos, player_name, seed, tag)
    VALUES %s
    """,
    values
)

conn.commit()
cursor.close()
conn.close()
print(f"Cargado CSV con {len(entries)} filas a staging.")
