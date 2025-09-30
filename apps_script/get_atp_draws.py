# apps_script/get_atp_draws.py

import pdfplumber
import pandas as pd
import sys
import tempfile
import requests
import re

VALID_TAGS = {"WC", "Qualifier", "BYE", "PR", "LL", "Q"}
MAX_POS = 32

def clean_name(name: str):
    # eliminar puntuaciones de resultado al final, ej “41” o “63 30”
    name = re.sub(r"\s+\d+(\s+\d+)*$", "", name)
    name = name.replace(" ,", ",").strip()
    return name


def parse_line(line: str):
    tokens = line.strip().split()
    if not tokens:
        return None

    # 🧹 Ignorar líneas que no empiezan con número
    if not tokens[0].isdigit():
        return None

    pos = int(tokens[0])
    tag = None
    seed = None
    country = None
    name_tokens = []

    idx = 1
    # Detectar TAG y/o SEED en cualquier orden
    for _ in range(2):  # como mucho 2 tokens (tag y seed)
        if idx < len(tokens) and tokens[idx] in VALID_TAGS:
            tag = tokens[idx]
            idx += 1
        elif idx < len(tokens) and tokens[idx].isdigit():
            seed = int(tokens[idx])
            idx += 1

    # Buscar país al final (3 letras mayúsculas)
    if len(tokens) >= idx + 2 and re.fullmatch(r"[A-Z]{3}", tokens[-1]):
        country = tokens[-1]
        name_tokens = tokens[idx:-1]
    else:
        name_tokens = tokens[idx:]

    # Buscar coma que separa apellido y nombre
    if not any("," in token for token in name_tokens):
        return None  # nombre mal formado

    player_name = " ".join(name_tokens).replace(" ,", ",").strip()

    # limpiar puntuaciones si se colaron (ej: “63 64” al final del nombre)
    player_name = re.sub(r"\d[\d\s\-]+$", "", player_name).strip()

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
            txt = page.extract_text()
            if not txt:
                continue
            lines = txt.split("\n")
            for line in lines:
                parsed = parse_line(line)
                if parsed:
                    entries.append(parsed)

    df = pd.DataFrame(entries, columns=["pos", "player_name", "seed", "tag", "country"])
    df["seed"] = df["seed"].astype("Int64")
    df.to_csv(out_csv_file, index=False)
    print(f"✅ Generado CSV con {len(df)} filas: {out_csv_file}")
