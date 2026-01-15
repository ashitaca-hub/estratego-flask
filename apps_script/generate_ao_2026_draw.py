import re
import csv
import requests

URL = "https://www.atptour.com/es/scores/current/australian-open/580/draws"

TAG_SET = {"Q", "WC", "LL", "PR", "ALT"}

def parse_paren(x: str):
    x = x.strip()
    if x.isdigit():
        return x, ""
    if x in TAG_SET:
        return "", x
    return "", ""

def main():
    html = requests.get(URL, timeout=30).text

    # Captura el texto de los links del draw y un posible "(...)" detrás.
    # Ej:  (1)
    #      (Q)
    pattern = re.compile(r"†\s*([^\]【\n]+?)\s*】(?:\s*\(([^)]+)\))?", re.UNICODE)

    rows = []
    pos = 0

    for m in pattern.finditer(html):
        name = m.group(1).strip()
        paren = (m.group(2) or "").strip()

        # Filtros para evitar textos no-jugador que a veces aparecen
        if not name or name.lower() in {"h2h", "ver detalle h2h"}:
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

        if pos >= 128:  # main draw GS
            break

    out = "ao_2026_draw.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["pos","player_name","seed","tag","country"])
        w.writeheader()
        w.writerows(rows)

    print(f"OK -> {out} ({len(rows)} filas)")

if __name__ == "__main__":
    main()
