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

    # ⛔ Solo considerar posiciones de R32 (hasta 32)
    if pos > 32:
        return None

    # ⛔ Filtrar líneas que contienen resultados numéricos
    if any(t.replace("(", "").replace(")", "").isdigit() for t in tokens[1:]):
        return None

    seed = None
    tag = None
    player_name = None
    country = None

    if len(tokens) == 2 and tokens[1] in VALID_TAGS:
        return {
            "pos": pos,
            "player_name": None,
            "seed": None,
            "tag": tokens[1],
            "country": None,
        }

    idx = 1
    if tokens[1].isdigit():
        seed = int(tokens[1])
        idx = 2
    elif tokens[1] in VALID_TAGS:
        tag = tokens[1]
        idx = 2

    if len(tokens[-1]) == 3 and tokens[-1].isupper():
        country = tokens[-1]
        name_tokens = tokens[idx:-1]
    else:
        return None

    player_name = " ".join(name_tokens).replace(" ,", ",").strip()
    if not player_name and not tag:
        return None

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
