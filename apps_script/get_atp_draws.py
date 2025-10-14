import pdfplumber
import pandas as pd
import sys
import tempfile
import requests
import re

VALID_TAGS = {"WC", "Qualifier", "BYE", "PR", "LL", "Q"}
MAX_POS = 32

def clean_name(name: str):
    name = re.sub(r"\s+\d+(\s+\d+)*$", "", name)
    name = name.replace(" ,", ",").strip()
    return name

def parse_line(line: str):
    tokens = line.strip().split()
    if not tokens or not tokens[0].isdigit():
        return None

    pos = int(tokens[0])
    tag = None
    seed = None
    country = None
    name_tokens = []

    idx = 1
    for _ in range(2):
        if idx < len(tokens) and tokens[idx] in VALID_TAGS:
            tag = tokens[idx]
            idx += 1
        elif idx < len(tokens) and tokens[idx].isdigit():
            seed = int(tokens[idx])
            idx += 1

    while idx < len(tokens):
        token = tokens[idx]
        if re.fullmatch(r"[A-Z]{3}", token):
            country = token
            idx += 1
            break
        name_tokens.append(token)
        idx += 1

    # Incluir BYE aunque no tenga nombre ni coma
    if tag == "BYE":
        return {
            "pos": pos,
            "player_name": None,
            "seed": seed,
            "tag": tag,
            "country": None,
        }

    # Ignorar si no hay coma en el nombre (formato inválido)
    if not any("," in tok for tok in name_tokens):
        return None

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
    print(f"\u2705 Generado CSV con {len(df)} filas: {out_csv_file}")
