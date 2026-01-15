import re
import csv
import os
import requests

URL = "https://www.atptour.com/es/scores/current/australian-open/580/draws"

TAG_SET = {"Q", "WC", "LL", "PR", "ALT"}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.atptour.com/",
}

def parse_paren(x: str):
    x = x.strip()
    if x.isdigit():
        return x, ""
    if x in TAG_SET:
        return "", x
    return "", ""

def main():
    r = requests.get(URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    html = r.text

    # Matchea links a jugadores y posible (seed/tag) justo después como texto.
    # Ej: >C. Alcaraz</a> (1)
    #     >Z. Svajda</a> (Q)
    player_pattern = re.compile(
        r">\s*([A-Z]\.\s*[^<]{1,60}?)\s*</a>\s*(?:\(([^)]+)\))?",
        re.UNICODE
    )

    matches = player_pattern.findall(html)

    # Si no hay resultados, guarda HTML para debug (muy útil en Actions)
    os.makedirs("data", exist_ok=True)
    if len(matches) < 10:
        with open("data/debug_atp_draw.html", "w", encoding="utf-8") as f:
            f.write(html)
        print(f"[WARN] Solo {len(matches)} jugadores detectados. Guardado data/debug_atp_draw.html")
        # Aun así seguimos para que veas qué sale.

    rows = []
    pos = 0

    for name, paren in matches:
        name = name.strip()

        # Ignora textos tipo H2H si se colaran (por seguridad)
        if name.upper() == "H2H":
            continue

        pos += 1
        seed, tag = ("", "")
        if name.upper() == "BYE":
            name = ""
            tag = "BYE"
        elif paren:
            seed, tag = parse_paren(paren)

        rows.append({
            "pos": pos,
            "player_name": name,
            "seed": seed,
            "tag": tag,
            "country": ""  # lo rellenas tú luego
        })

        if pos >= 128:
            break

    out = "ao_2026_draw.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["pos", "player_name", "seed", "tag", "country"])
        w.writeheader()
        w.writerows(rows)

    print(f"OK -> {out} ({len(rows)} filas)")

if __name__ == "__main__":
    main()
