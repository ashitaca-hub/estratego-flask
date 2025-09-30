# apps_script/get_atp_draws.py

import pdfplumber
import pandas as pd
import sys
import tempfile
import requests
import re
from pathlib import Path

VALID_TAGS = {"WC", "Qualifier", "BYE", "PR", "LL"}

def parse_line(line: str):
    line = line.strip()
    if not line:
        return None

    tokens = line.split()
    if not tokens or not tokens[0].isdigit():
        return None

    try:
        pos = int(tokens[0])
    except ValueError:
        return None

    seed = None
    tag = None
    country = None
    player_name = None

    idx = 1

    if len(tokens) > idx and tokens[idx].isdigit():
        seed = int(tokens[idx])
        idx += 1

    if len(tokens) > idx and tokens[idx] in VALID_TAGS:
        tag = tokens[idx]
        idx += 1

    remaining = tokens[idx:]

    if remaining and re.fullmatch(r"[A-Z]{3}", remaining[-1]):
        country = remaining[-1]
        name_tokens = remaining[:-1]
    else:
        name_tokens = remaining

    name = " ".join(name_tokens).replace(" ,", ",").strip()
    name = name.replace("…", "").replace("..", "").strip()
    name = re.sub(r"\([^)]*\)", "", name).strip()
    name = re.sub(r"\d{2,}.*", "", name).strip()

    if not name and not tag:
        return None

    return {
        "pos": pos,
        "player_name": name if name else None,
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
