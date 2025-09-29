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
    if not tokens:
        return None

    # ✅ ignorar encabezados/no numéricos
    if not tokens[0].isdigit():
        return None

    pos = tokens[0]
    seed = None
    tag = None
    player_name = None
    country = None

    # Si solo hay 2 tokens y el segundo es un tag => posición vacía (Qualifier, BYE, etc.)
    if len(tokens) == 2 and tokens[1] in VALID_TAGS:
        return {
            "pos": int(pos),
            "player_name": None,
            "seed": None,
            "tag": tokens[1],
            "country": None,
        }

    idx = 1
    # Si el segundo token es numérico => seed
    if tokens[1].isdigit():
        seed = int(tokens[1])
        idx = 2
    # Si el segundo token es un tag especial
    elif tokens[1] in VALID_TAGS:
        tag = tokens[1]
        idx = 2

    # El último token debe ser país (3 letras mayúsculas)
    if len(tokens[-1]) == 3 and tokens[-1].isupper():
        country = tokens[-1]
        name_tokens = tokens[idx:-1]
    else:
        # si no hay país válido, descartar la línea (ej. cabeceras de PDF)
        return None

    # reconstruir nombre
    player_name = " ".join(name_tokens).replace(" ,", ",").strip()
    if not player_name and not tag:
        return None  # ignorar filas vacías

    return {
        "pos": int(pos),
        "player_name": player_name if player_name else None,
        "seed": seed,
        "tag": tag,
        "country": country,
    }


def download_pdf(tourney_id: int, year: int = 2025, output_dir: str = "data") -> Path:
    """
    Descarga el PDF del cuadro principal (Main Draw Singles) de ProTennisLive.
    """
    import requests

    url = f"https://www.protennislive.com/posting/{year}/{tourney_id}/mds.pdf"
    output_path = Path(output_dir) / f"mds_{year}_{tourney_id}.pdf"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    res = requests.get(url)
    res.raise_for_status()

    with open(output_path, "wb") as f:
        f.write(res.content)

    print(f"PDF descargado en {output_path}")
    return output_path


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python get_atp_draws.py <pdf_file> <out_csv_file>")
        sys.exit(1)

pdf_url = sys.argv[1]
csv_output = sys.argv[2]

res = requests.get(pdf_url)
res.raise_for_status()

with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
    tmp.write(res.content)
    tmp_path = tmp.name

    entries = []
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            lines = page.extract_text().split("\n")
            for line in lines:
                parsed = parse_line(line)
                if parsed:
                    entries.append(parsed)

    df = pd.DataFrame(entries, columns=["pos", "player_name", "seed", "tag", "country"])
    # fuerza a que seed sea Int64 (nullable) para no tener floats
    df["seed"] = df["seed"].astype("Int64")
    df.to_csv(out_csv_file, index=False)
    print(f"Generado CSV con {len(df)} filas: {out_csv_file}")
