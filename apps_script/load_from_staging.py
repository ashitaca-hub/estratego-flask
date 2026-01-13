import os
import sys
import requests
import json
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}


def fetch_staging(tourney_id):
    url = f"{SUPABASE_URL}/rest/v1/stg_draw_entries_by_name?tourney_id=eq.{tourney_id}"
    res = requests.get(url, headers=HEADERS)
    if not res.ok:
        print("Error al consultar staging:", res.status_code, res.text)
        res.raise_for_status()
    return res.json()


def clear_existing_draw_entries(tourney_id):
    print(f"🧹 Borrando draw_entries y draw_matches para torneo {tourney_id}...")
    for table in ["draw_matches", "draw_entries"]:
        url = f"{SUPABASE_URL}/rest/v1/{table}?tourney_id=eq.{tourney_id}"
        res = requests.delete(url, headers=HEADERS)
        if not res.ok:
            print(f"Error borrando {table}: {res.status_code} {res.text}")
            res.raise_for_status()


import re

def normalize_name(name: str) -> str:
    # Eliminar puntuaciones, resultados y residuos al final
    name = re.sub(r"\d{2,}.*$", "", name)  # quita resultados tipo '63 64 ...'
    name = re.sub(r"\b\d+\b", "", name)  # quita números aislados
    name = re.sub(r"[|]", " ", name)  # separadores de tabla
    name = re.sub(r"\s*[.…]+$", "", name)  # quita puntos suspensivos finales
    name = re.sub(r"[^\w\s,\.]", "", name)  # limpia símbolos no deseados
    name = re.sub(r"\.{2,}", ".", name)  # normaliza puntos suspensivos
    name = re.sub(r"\s+", " ", name)  # normaliza espacios
    name = name.strip()

    if "," in name:
        surname, rest = name.split(",", 1)
        rest = rest.strip()
        name = f"{surname.strip()}, {rest}".strip().rstrip(",")

    return name

def resolve_player_id(player_name):
    if not player_name:
        return None, "nombre vacío"

    raw_has_comma = "," in player_name
    name_clean = normalize_name(player_name)

    name_variants = []

    def add_variant(variant):
        if not variant:
            return
        name_variants.append(variant)
        if "%" not in variant:
            name_variants.append(f"%{variant}")
            name_variants.append(f"{variant}%")
            name_variants.append(f"%{variant}%")

    add_variant(name_clean)
    if "," in name_clean:
        parts = name_clean.split(",", 1)
        firstname = parts[1].strip()
        surname = parts[0].strip()
        add_variant(f"{firstname} {surname}".strip())
        if firstname:
            add_variant(f"{surname}, {firstname}")
            add_variant(f"{firstname} {surname}")
        add_variant(surname)

        surname_tokens = surname.split()
        if len(surname_tokens) > 1:
            alt_surname = surname_tokens[-1]
            extra_given = " ".join(surname_tokens[:-1]).strip()
            alt_firstname = firstname
            if extra_given and extra_given != firstname:
                alt_firstname = f"{firstname} {extra_given}".strip()
            elif not firstname:
                alt_firstname = extra_given
            add_variant(f"{alt_surname}, {alt_firstname}".strip().rstrip(","))
            add_variant(f"{alt_firstname} {alt_surname}".strip())
        if " " in firstname:
            first_tokens = firstname.split()
            add_variant(f"{surname}, {first_tokens[0]}".strip())
            add_variant(f"{first_tokens[0]} {surname}".strip())
        if "-" in surname:
            add_variant(surname.replace("-", " ").strip())
        if "-" in firstname:
            add_variant(firstname.replace("-", " ").strip())
    elif raw_has_comma and name_clean:
        add_variant(name_clean)
        add_variant(f"{name_clean}, %")
        add_variant(f"{name_clean},%")
    else:
        tokens = name_clean.split()
        if len(tokens) > 1:
            surname = tokens[-1]
            firstname = " ".join(tokens[:-1]).strip()
            add_variant(f"{surname}, {firstname}")
            add_variant(f"{firstname} {surname}")

    multi_match_variants = []
    for variant in name_variants:
        url = f"{SUPABASE_URL}/rest/v1/players_min"
        params = {"select": "player_id", "name": f"ilike.{variant}"}
        res = requests.get(url, headers=HEADERS, params=params)
        if res.ok:
            matches = res.json()
            if len(matches) == 1:
                return matches[0]["player_id"], None
            if len(matches) > 1:
                multi_match_variants.append((variant, len(matches)))

    if multi_match_variants:
        sample = ", ".join(f"{variant} ({count})" for variant, count in multi_match_variants[:3])
        return None, f"coincidencias múltiples: {sample}"

    return None, "sin coincidencias en players_min"



def insert_draw_entry(tourney_id, row, player_id):
    payload = {
        "tourney_id": tourney_id,
        "pos": row["pos"],
        "player_id": player_id,
        "seed": row.get("seed"),
        "tag": row.get("tag") or ("UNRESOLVED" if not player_id else None)
    }
    url = f"{SUPABASE_URL}/rest/v1/draw_entries"
    res = requests.post(url, headers=HEADERS, data=json.dumps(payload))
    if not res.ok:
        print(f"Error insertando draw_entry en pos {row['pos']}: {res.status_code} {res.text}")
        res.raise_for_status()


def mark_as_processed(tourney_id, pos):
    now = datetime.utcnow().isoformat()
    url = f"{SUPABASE_URL}/rest/v1/stg_draw_entries_by_name?tourney_id=eq.{tourney_id}&pos=eq.{pos}"
    payload = {"processed_at": now}
    res = requests.patch(url, headers=HEADERS, data=json.dumps(payload))
    if not res.ok:
        print(f"Error marcando como procesado (tourney={tourney_id}, pos={pos}):", res.text)


def main():
    if len(sys.argv) < 2:
        print("Uso: python load_from_staging.py <tourney_id>")
        sys.exit(1)

    tourney_id = sys.argv[1]
    clear_existing_draw_entries(tourney_id)
    staging_rows = fetch_staging(tourney_id)

    print(f"📥 Filas a procesar para torneo {tourney_id}: {len(staging_rows)}")

    for row in staging_rows:
        player_id, resolve_reason = resolve_player_id(row.get("player_name"))
        insert_draw_entry(tourney_id, row, player_id)
        mark_as_processed(tourney_id, row["pos"])

        if not player_id and not row.get("tag"):
            detail = f" -> {resolve_reason}" if resolve_reason else ""
            print(f"[!] No se pudo resolver: {row['player_name']} (pos {row['pos']}){detail}")

    print("✅ Migración completada.")


if __name__ == "__main__":
    main()
