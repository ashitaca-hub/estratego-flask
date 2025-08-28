# apps_script/simulate_bracket_from_csv.py
import csv, json, os, random, unicodedata, urllib.request

API = os.environ.get("API_URL", "http://127.0.0.1:8080/matchup")
CSV_IN = os.environ.get("ENTRANTS_CSV", "data/entrants.csv")
MAP_CSV = os.environ.get("MAP_CSV", "data/players_sr_map.csv")  # Name;Player ID (sr:competitor:XXXXX)

TOURNAMENT = {"name": os.environ.get("TNAME", "Cincinnati"),
              "month": int(os.environ.get("TMONTH", "8"))}
YEARS_BACK = int(os.environ.get("YEARS_BACK", "4"))
MODE = (os.environ.get("MODE", "deterministic") or "deterministic").lower()
MC_RUNS = int(os.environ.get("MC_RUNS", "0") or 0)

def _norm(s: str) -> str:
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    out = []
    for ch in s:
        out.append(ch if ch.isalnum() or ch.isspace() else " ")
    return " ".join("".join(out).split())

def load_name_to_sr(path: str) -> dict:
    """Lee data/players_sr_map.csv (Name;Player ID) y devuelve {norm_name: sr_full}"""
    m = {}
    if not os.path.exists(path):
        return m
    with open(path, newline="", encoding="utf-8") as f:
        # Soporta separador ; (tu CSV) o , si alguna vez cambias
        sample = f.read(4096)
        f.seek(0)
        delim = ";" if sample.count(";") >= sample.count(",") else ","
        reader = csv.DictReader(f, delimiter=delim)
        # Columnas típicas: 'Name' y 'Player ID' (o 'name'/'sr_full')
        for r in reader:
            name = (r.get("Name") or r.get("name") or "").strip()
            sr = (r.get("Player ID") or r.get("sr_full") or "").strip()
            if name and sr:
                m[_norm(name)] = sr
    return m

NAME2SR = load_name_to_sr(MAP_CSV)

def call_matchup(payload: dict):
    clean = {k: v for k,v in payload.items() if v is not None}
    data = json.dumps(clean).encode("utf-8")
    req  = urllib.request.Request(API, data=data, headers={"Content-Type":"application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))

def read_entrants(path):
    rows=[]
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            seed = r.get("seed")
            seed = int(seed) if seed and seed.strip() else None
            pid  = (r.get("id") or "").strip()
            name = (r.get("name") or "").strip()
            if not pid and name:
                # ← mapear por nombre si hay SR en el CSV de mapeo
                sr = NAME2SR.get(_norm(name))
                if sr:
                    pid = sr
            rows.append({"seed": seed, "id": pid, "name": name})
    if not rows or len(rows) % 2 != 0:
        raise ValueError("El CSV debe tener un número par de jugadores (>0).")
    return rows

def split_identifier(s: str):
    if not s: return (None, None)
    s = s.strip()
    if s.startswith("sr:"): return ("sr", s)
    if s.isdigit(): return ("int", int(s))
    return ("name", s)

def build_participant(entry: dict):
    id_kind, id_val = split_identifier(entry["id"])
    if id_kind in ("sr","int"):
        return {"player_id": id_val, "player": None, "label": entry["name"] or str(id_val)}
    # fallback: mandar nombre
    name = entry["name"] or id_val
    return {"player_id": None, "player": name, "label": name}

def first_round_pairs_by_seed(players):
    ps = sorted(players, key=lambda x: (x["seed"] is None, x["seed"]))
    n = len(ps)
    return [(ps[i], ps[n-1-i]) for i in range(n//2)]

def round_pairs(players):
    return [(players[i], players[i+1]) for i in range(0,len(players),2)]

def play_round(players, use_seeds, sample=False):
    pairs = first_round_pairs_by_seed(players) if use_seeds else round_pairs(players)
    results=[]; winners=[]; unresolved=[]
    for a,b in pairs:
        pa = build_participant(a)
        pb = build_participant(b)
        payload = {
            "player_id": pa["player_id"], "player": pa["player"],
            "opponent_id": pb["player_id"], "opponent": pb["player"],
            "tournament": TOURNAMENT, "years_back": YEARS_BACK
        }
        r = call_matchup(payload)
        prob_a = float(r.get("prob_player", 0.5))

        # Diagnóstico: si el backend no resolvió a IDs internos, avisa
        inp = r.get("inputs", {})
        if not inp.get("player_id") or not inp.get("opponent_id"):
            unresolved.append((a.get("name") or a.get("id"), b.get("name") or b.get("id")))

        win_a = (random.random() < prob_a) if sample else (prob_a >= 0.5)
        winner = a if win_a else b
        results.append({
            "a": a.get("name") or a.get("id"),
            "b": b.get("name") or b.get("id"),
            "a_id": a.get("id"), "b_id": b.get("id"),
            "prob_a": round(prob_a,6), "winner": (winner.get("name") or winner.get("id")),
        })
        winners.append(winner)
    if unresolved:
        print("WARN no-resueltos:", unresolved)
    return results, winners

def simulate_once(entrants):
    rnd=1; bracket=[]; current=entrants[:]
    use_seeds = all(p.get("seed") is not None for p in current)
    while len(current)>1:
        res, winners = play_round(current, use_seeds if rnd==1 else False, sample=(MODE=="mc"))
        bracket.append({"round": rnd, "matches": res})
        current = winners; rnd += 1
    return bracket, current[0]

def write_matches_csv(bracket, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["round","a","b","a_id","b_id","prob_a","winner"])
        for rnd in bracket:
            for m in rnd["matches"]:
                w.writerow([rnd["round"], m["a"], m["b"], m["a_id"], m["b_id"], m["prob_a"], m["winner"]])

def main():
    entrants = read_entrants(CSV_IN)
    if MODE == "mc" and MC_RUNS > 0:
        wins = { (e["name"] or e["id"]):0 for e in entrants }
        example=None; champ0=None
        for _ in range(MC_RUNS):
            bracket, champ = simulate_once(entrants)
            wins[champ.get("name") or champ.get("id")] += 1
            if example is None: example, champ0 = bracket, champ
        probs = []
        for e in entrants:
            key = e.get("name") or e.get("id")
            probs.append({"id": e.get("id"), "name": e.get("name"), "seed": e.get("seed"),
                          "p_champion": wins[key]/MC_RUNS})
        out = {"ok": True, "mode": "mc", "mc_runs": MC_RUNS,
               "tournament": TOURNAMENT, "years_back": YEARS_BACK,
               "champion_probs": sorted(probs, key=lambda x: x["p_champion"], reverse=True),
               "example_bracket": example, "example_champion": champ0}
        print("== BRACKET (MC) =="); print(json.dumps(out, indent=2))
        write_matches_csv(example, "/tmp/bracket_matches.csv")
        with open("/tmp/bracket.json","w",encoding="utf-8") as f: json.dump(out,f,ensure_ascii=False,indent=2)
    else:
        bracket, champ = simulate_once(entrants)
        out = {"ok": True, "mode": "deterministic", "tournament": TOURNAMENT,
               "years_back": YEARS_BACK, "bracket": bracket, "champion": champ}
        print("== BRACKET =="); print(json.dumps(out, indent=2))
        write_matches_csv(bracket, "/tmp/bracket_matches.csv")
        with open("/tmp/bracket.json","w",encoding="utf-8") as f: json.dump(out,f,ensure_ascii=False,indent=2)

if __name__ == "__main__":
    main()

