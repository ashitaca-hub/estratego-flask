# get_atp_draws.py

import requests
import pdfplumber
import csv
from pathlib import Path


def download_pdf(tourney_id: int, year: int = 2025, output_dir: str = "data") -> Path:
    url = f"https://www.protennislive.com/posting/{year}/{tourney_id}/mds.pdf"
    output_path = Path(output_dir) / f"mds_{tourney_id}.pdf"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(url)
    if response.status_code == 200:
        with open(output_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded PDF for tourney_id={tourney_id}")
        return output_path
    else:
        raise ValueError(f"PDF not found for tourney_id={tourney_id} ({response.status_code})")


def parse_line(line: str):
    """
    Parse una línea del draw desde PDF.
    Ejemplos:
    1 1 ALCARAZ, Carlos ESP
    2 BAEZ, Sebastian ARG
    7 Qualifier
    10 WC MOCHIZUKI, Shintaro JPN
    """
    tokens = line.strip().split()
    if not tokens:
        return None

    pos = tokens[0]
    seed = None
    tag = None
    player_name = None
    country = None

    # Caso: solo posición + tag (Qualifier, BYE)
    if len(tokens) == 2 and tokens[1] in ["Qualifier", "BYE"]:
        tag = tokens[1]
        return {
            "pos": int(pos),
            "player_name": None,
            "seed": None,
            "tag": tag,
            "country": None,
        }

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
        "country": country,
    }


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python get_atp_draws.py <pdf_file> <out_csv_file>")
        sys.exit(1)

    pdf_file = sys.argv[1]
    out_csv_file = sys.argv[2]

    entries = []
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            lines = page.extract_text().split("\n")
            for line in lines:
                parsed = parse_line(line)
                if parsed:
                    entries.append(parsed)

    df = pd.DataFrame(entries, columns=["pos", "player_name", "seed", "tag", "country"])
    df.to_csv(out_csv_file, index=False)
    print(f"Generado CSV con {len(df)} filas: {out_csv_file}")
    entries = parse_draw(pdf_path)
    save_to_csv(entries, f"data/draw_{tourney_id}.csv")
    print(f"Parsed and saved {len(entries)} entries for tourney {tourney_id}.")
