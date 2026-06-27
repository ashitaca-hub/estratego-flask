import sys
import tempfile
import requests
import re

VALID_TAGS = {"WC", "Qualifier", "BYE", "PR", "LL", "Q", "SE"}
MAX_POS = 128
ENTRY_PATTERN = re.compile(
    r"(?<!\d)(?P<pos>\d{1,3})\s+(?P<body>.+?)\s+(?P<country>[A-Z]{3})(?=\s+\d{1,3}\s+|$)"
)
DATE_LINE_PATTERN = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\b.*\b\d{4}\b",
    re.IGNORECASE,
)

# Formato alternativo usado por los sitios oficiales de los Grand Slams
# (ej. wimbledon.com), una entrada por linea: "N. APELLIDO, Nombre PAIS [seed|(tag)]"
SLAM_LINE_START = re.compile(r"^(?P<pos>\d{1,3})\.\s+(?P<rest>.+)$")
SLAM_TAG_MAP = {"Q": "Q", "W": "WC", "LL": "LL", "PR": "PR", "SE": "SE"}


def parse_slam_line(line: str):
    m = SLAM_LINE_START.match(line.strip())
    if not m:
        return None
    pos = int(m.group("pos"))
    if pos > MAX_POS:
        return None
    rest = m.group("rest").strip()

    if rest.lower().startswith("bye"):
        return {"pos": pos, "player_name": None, "seed": None, "tag": "BYE", "country": None}

    # Pie de pagina mal extraido (ej. "... USA Champion:") pegado a la ultima fila
    rest = re.sub(r"\s+Champion:\s*$", "", rest, flags=re.IGNORECASE).strip()

    seed = None
    tag = None

    tag_match = re.search(r"\((\w+)\)\s*$", rest)
    if tag_match:
        tag = SLAM_TAG_MAP.get(tag_match.group(1).upper(), tag_match.group(1))
        rest = rest[: tag_match.start()].strip()
    else:
        seed_match = re.search(r"(?<!\w)(\d{1,3})\s*$", rest)
        if seed_match:
            seed = int(seed_match.group(1))
            rest = rest[: seed_match.start()].strip()

    # El codigo de pais a veces falta en el texto extraido (p.ej. cuando se
    # renderiza como bandera/imagen en lugar de texto para esa fila).
    country = None
    country_match = re.search(r"(?<![A-Za-z])([A-Z]{3})\s*$", rest)
    if country_match:
        country = country_match.group(1)
        rest = rest[: country_match.start()].strip()

    if "," not in rest:
        return None
    surname, given = rest.split(",", 1)
    surname = surname.strip()
    given = given.strip()
    if not surname or not given:
        return None

    return {
        "pos": pos,
        "player_name": f"{surname}, {given}",
        "seed": seed,
        "tag": tag,
        "country": country,
    }


def parse_slam_pdf(pdf_pages) -> list:
    entries = []
    for page in pdf_pages:
        txt = page.extract_text()
        if not txt:
            continue
        for line in txt.split("\n"):
            line = line.strip()
            if not line or not line[0].isdigit():
                continue
            parsed = parse_slam_line(line)
            if parsed:
                entries.append(parsed)
    return entries

def clean_name(name: str):
    name = re.sub(r"\s+\d+(\s+\d+)*$", "", name)
    name = re.sub(r"\s*[|]+\s*$", "", name)
    name = name.replace(" ,", ",").strip()
    return name

def parse_tokens(pos: int, tokens: list[str], country: str | None):
    if not tokens:
        return None

    if len(tokens) >= 1 and tokens[0].lower() == "bye":
        return {
            "pos": pos,
            "player_name": None,
            "seed": None,
            "tag": "BYE",
            "country": None,
        }

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
        return None

    if any("," in tok for tok in name_tokens):
        player_name = " ".join(name_tokens).replace(" ,", ",").strip()
    elif len(name_tokens) >= 2:
        last_name = " ".join(name_tokens[:-1])
        first_name = name_tokens[-1]
        player_name = f"{last_name}, {first_name}".strip()
    else:
        return None

    player_name = clean_name(player_name)

    return {
        "pos": pos,
        "player_name": player_name if player_name else None,
        "seed": seed,
        "tag": tag,
        "country": country,
    }


def parse_line(line: str):
    if DATE_LINE_PATTERN.search(line):
        return []

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

def main(pdf_url: str, out_csv_file: str):
    import pandas as pd
    import pdfplumber

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

        if not entries:
            # El formato "una entrada por linea" de los Grand Slams (ej.
            # wimbledon.com) no encaja con el parser anterior; lo intentamos
            # como formato alternativo antes de rendirnos.
            print("Formato ATP estandar no encontro filas, probando formato Slam...")
            entries = parse_slam_pdf(pdf.pages)

    df = pd.DataFrame(entries, columns=["pos", "player_name", "seed", "tag", "country"])
    df["seed"] = df["seed"].astype("Int64")
    df.to_csv(out_csv_file, index=False)
    print(f"✅ Generado CSV con {len(df)} filas: {out_csv_file}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python get_atp_draws.py <pdf_url> <out_csv_file>")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2])
