import pdfplumber
import pandas as pd
import sys
import tempfile
import requests
import re

VALID_TAGS = {"WC", "Qualifier", "BYE", "PR", "LL", "Q", "SE"}
MAX_POS = 32
ENTRY_PATTERN = re.compile(
    r"(?P<pos>\d{1,2})\s+(?P<body>.+?)\s+(?P<country>[A-Z]{3})(?=\s+\d{1,2}\s+|$)"
)

def clean_name(name: str):
    name = re.sub(r"\s+\d+(\s+\d+)*$", "", name)
    name = re.sub(r"\s*[|]+\s*$", "", name)
    name = name.replace(" ,", ",").strip()
    return name

def parse_line(line: str):
    entries = []
    for match in ENTRY_PATTERN.finditer(line):
        pos = int(match.group("pos"))
        if pos > MAX_POS:
            continue

        body = match.group("body").strip()
        country = match.group("country")

        tokens = body.split()
        if not tokens:
            continue

        if len(tokens) >= 1 and tokens[0].lower() == "bye":
            entries.append(
                {
                    "pos": pos,
                    "player_name": None,
                    "seed": None,
                    "tag": "BYE",
                    "country": None,
                }
            )
            continue

        tag = None
        seed = None
        idx = 0
        for _ in range(2):
            if idx < len(tokens) and tokens[idx] in VALID_TAGS:
                tag = tokens[idx]
                idx += 1
            elif idx < len(tokens) and tokens[idx].isdigit():
                seed = int(tokens[idx])
                idx += 1

        name_tokens = tokens[idx:]
        if not name_tokens:
            continue

        if any("," in tok for tok in name_tokens):
            player_name = " ".join(name_tokens).replace(" ,", ",").strip()
        elif len(name_tokens) >= 2:
            last_name = " ".join(name_tokens[:-1])
            first_name = name_tokens[-1]
            player_name = f"{last_name}, {first_name}".strip()
        else:
            continue

        player_name = clean_name(player_name)

        entries.append(
            {
                "pos": pos,
                "player_name": player_name if player_name else None,
                "seed": seed,
                "tag": tag,
                "country": country,
            }
        )
    return entries


def parse_line(line: str):
    entries = []
    for match in ENTRY_PATTERN.finditer(line):
        pos = int(match.group("pos"))
        if pos > MAX_POS:
            continue

        body = match.group("body").strip()
        country = match.group("country")

        tokens = body.split()
        parsed = parse_tokens(pos, tokens, country)
        if parsed:
            entries.append(parsed)

    if entries:
        return entries

    tokens = line.strip().split()
    if not tokens or not tokens[0].isdigit():
        return []

    pos = int(tokens[0])
    if pos > MAX_POS:
        return []

    if len(tokens) >= 2 and tokens[1].lower() == "bye":
        return [
            {
                "pos": pos,
                "player_name": None,
                "seed": None,
                "tag": "BYE",
                "country": None,
            }
        ]

    tag = None
    seed = None
    idx = 1
    for _ in range(2):
        if idx < len(tokens) and tokens[idx] in VALID_TAGS:
            tag = tokens[idx]
            idx += 1
        elif idx < len(tokens) and tokens[idx].isdigit():
            seed = int(tokens[idx])
            idx += 1

    country_idx = None
    for i in range(idx, len(tokens)):
        if re.fullmatch(r"[A-Z]{3}", tokens[i]):
            country_idx = i
            break

    if country_idx is None:
        return []

    name_tokens = tokens[idx:country_idx]
    country = tokens[country_idx]
    parsed = parse_tokens(pos, name_tokens, country)
    return [parsed] if parsed else []

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
                    entries.extend(parsed)

    df = pd.DataFrame(entries, columns=["pos", "player_name", "seed", "tag", "country"])
    df["seed"] = df["seed"].astype("Int64")
    df.to_csv(out_csv_file, index=False)
    print(f"✅ Generado CSV con {len(df)} filas: {out_csv_file}")
