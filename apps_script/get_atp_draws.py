# apps_script/get_atp_draws.py

import pdfplumber
import pandas as pd
import sys
import tempfile
import requests
from pathlib import Path

VALID_TAGS = {"WC", "Qualifier", "BYE", "PR", "LL"}


def parse_line(line: str):
    tokens = line.strip().split()
    if not tokens or not tokens[0].isdigit():
        return None

    pos = int(tokens[0])
    if pos > 32:
        return None  # ⛔ Solo R32

    seed = None
    tag = None
    country = None
    player_name = None

    idx = 1

    # Seed
    if idx < len(tokens) and tokens[idx].isdigit():
        seed = int(tokens[idx])
        idx += 1

    # Tag
    if idx < len(tokens) and tokens[idx] in VALID_TAGS:
        tag = tokens[idx]
        idx += 1

    # Buscar país (primer token que son 3 letras mayúsculas)
    country_idx = None
    for i in range(idx, len(tokens)):
        if len(tokens[i]) == 3 and tokens[i].isupper():
            country_idx = i
            break

    if country_idx is None:
        return None  # país no encontrado

    country = tokens[country_idx]
    name_tokens = tokens[idx:country_idx]
    player_name = " ".join(name_tokens).replace(" ,", ",").strip()

    return {
        "pos": pos,
        "player_name": player_name if player_name else None,
        "seed": seed,
        "tag": tag,
        "country": country,
    }


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python get_atp_draws.py <pdf_url> <out_csv_file>")
        sys.exit(1)

    pdf_url = sys.argv[1]
    out_csv_file = sys.argv[2]

    res = requests.get(pdf_url)
    res.raise_for_status()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(res.content)
        tmp_path = tmp.name

    entries = []
    with pdfplumber.open(tmp_path) as pdf:
        for page in pdf.pages:
            lines = page.extract_text().split("\n")
            for line in lines:
                parsed = parse_line(line)
                if parsed:
                    entries.append(parsed)

    df = pd.DataFrame(entries, columns=["pos", "player_name", "seed", "tag", "country"])
    df["seed"] = df["seed"].astype("Int64")
    df.to_csv(out_csv_file, index=False)
    print(f"Generado CSV con {len(df)} filas: {out_csv_file}")
