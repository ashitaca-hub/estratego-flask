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


def parse_draw(pdf_path: Path) -> list:
    entries = []
    with pdfplumber.open(pdf_path) as pdf:
        text = "\n".join(page.extract_text() for page in pdf.pages if page.extract_text())

    # Localizar sección Main Draw Singles
    if "Main Draw Singles" not in text:
        raise ValueError("No se encontró la sección Main Draw Singles en el PDF")
    section = text.split("Main Draw Singles", 1)[1]

    # Cortar antes de Round of 32 o cualquier otra sección
    stop_words = ["Round of 32", "Seeded Players", "Last Direct Acceptance"]
    for stop in stop_words:
        if stop in section:
            section = section.split(stop, 1)[0]

    lines = section.splitlines()

    seen_pos = set()
    for line in lines:
        parts = line.strip().split(" ", 2)
        if len(parts) >= 3 and parts[0].isdigit():
            pos = int(parts[0])
            if pos in seen_pos:
                continue  # evitar duplicados
            seen_pos.add(pos)
            player_name = parts[2].strip()
            entries.append({
                "pos": pos,
                "player_name": player_name,
                "seed": None,
                "tag": None
            })

    return entries


def save_to_csv(entries: list, output_file: str):
    with open(output_file, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["pos", "player_name", "seed", "tag"])
        writer.writeheader()
        for entry in entries:
            writer.writerow(entry)


if __name__ == "__main__":
    tourney_id = 329
    pdf_path = download_pdf(tourney_id)
    entries = parse_draw(pdf_path)
    save_to_csv(entries, f"data/draw_{tourney_id}.csv")
    print(f"Parsed and saved {len(entries)} entries for tourney {tourney_id}.")
