# apps_script/get_atp_draws.py

import pdfplumber
import pandas as pd
import sys
import tempfile
import requests
import re

VALID_TAGS = {"WC", "Qualifier", "BYE", "PR", "LL", "Q"}
MAX_POS = 32  # Máxima posición a considerar (primera ronda)

def parse_line(line: str):
    tokens = line.strip().split()
    if not tokens or not tokens[0].isdigit():
        return None

    pos = int(tokens[0])
    if pos > MAX_POS:
        return None

    seed = None
    tag = None
    player_name = None
    country = None
    idx = 1

    # Posible semilla
    if idx < len(tokens) and tokens[idx].isdigit():
        seed = int(tokens[idx])
        idx += 1

    # Posible etiqueta
    if idx < len(tokens) and tokens[idx] in VALID_TAGS:
        tag = tokens[idx]
        idx += 1

    # País debe estar al final y tener 3 letras
    if len(tokens) >= 2 and re.match(r"^[A-Z]{3}$", tokens[-1]):
        country = tokens[-1]
        name_tokens = tokens[idx:-1]
    else:
        return None

    # Limpiar nombre del jugador
    name = " ".join(name_tokens)
    name = re.sub(r"[A-Z]\.\s\S+", "", name)  # eliminar posibles scores mal pegados
    name = name.replace(" ,", ",").strip()

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
                if len(entries) >= MAX_POS:
                    break
            if len(entries) >= MAX_POS:
                break

    df = pd.DataFrame(entries, columns=["pos", "player_name", "seed", "tag", "country"])
    df["seed"] = df["seed"].astype("Int64")
    df.to_csv(out_csv_file, index=False)
    print(f"Generado CSV con {len(df)} filas: {out_csv_file}")
