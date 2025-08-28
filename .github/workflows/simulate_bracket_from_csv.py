# apps_script/simulate_bracket_from_csv.py
import csv, json, os, random, urllib.request

API = os.environ.get("API_URL", "http://127.0.0.1:8080/matchup")
CSV = os.environ.get("ENTRANTS_CSV", "data/entrants.csv")

TOURNAMENT = {
    "name": os.environ.get("TNAME", "Cincinnati"),
    "month": int(os.environ.get("TMONTH", "8"))
}
YEARS_BACK = int(os.environ.get("YEARS_BACK", "4"))

MODE = os.environ.get("MODE", "deterministic").lower()  # 'deterministic' | 'mc'
MC_RUNS = int(os.environ.get("MC_RUNS", "0") or 0)       # solo si MODE=='mc'

def call_matchup(payload: dict):
    # Limpia claves con None para no confundir al backend
    clean = {k: v for k, v in payload.items() if v is not None}
    data = json.dumps(clean).encode("utf-8")
    req  = urllib.request.Request(API, data=data, headers={"Content-Type":"application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))

def read_entrants(path):
    rows=[]
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            seed = r.get("seed")
            seed = int(seed) if seed and seed.strip() else None
            _id  = (r.get("id") or "").strip()
            name = (r.get("name") or "").strip()
            if not _id and not name:
                raise ValueError("Fila sin id ni name.")
            rows.append({"seed": seed, "id": _id, "name": name})
    if not rows or len(rows)%2!=0:
        raise ValueError("El CSV debe tener un número par de jugadores (>0).")
    return rows

def split_identifier(s: str):
    """
    Devuelve tupla (kind, value):
      - ('sr', 'sr:competitor:225050') si es SR
      - ('int', 206173) si es entero
      - ('name', 'Jannik Sinner') si es otro texto
      - (None, None) si cadena vacía
    """
    if not s:
        return (None, None)
    ss = s.strip()
    if ss.startswith("sr:"):
        return ("sr", ss)
    if ss.isdigit():
        return ("int", int(ss))
    return ("name", ss)

def build_participant(entry: dict):
    """
    Para cada participante decide qué enviar:
    - Si id es SR o int -> usar player_id
    - Si id es nombre (o está vacío) -> usar player (nombre)
    """
    id_kind, id_val = split_identifier(entry["id"])
    if id_kind in ("sr", "int"):
        return {"player_id": id_val, "player": None, "label": entry["name"] or str(id_val)}
    # Fallback al nombre:
    name = entry["name"] or id_val  # por si venía el nombre en 'id'
    return {"player_id": None, "player": name, "label": name}

def first_round_pairs_by_seed(players):
    ps = sorted(players, key=lambda x: (x["seed"] is None, x["seed"]))
    n = len(ps)
    return [(ps[i], ps[n-1-i]) for i in range(n//2)]

def round_pairs(players):
    return [(players[i], players[i+1]) for i in range(0,len(players),2)]

def play_round(players, use_seeds, sample=False):
    pairs = first_round_pairs_by_seed(players) if use_seeds else round_pairs(players)
    results=[]; winners=[]
    for a,b in pairs:
        pa = build_participant(a)
        pb = build_participant(b)
        payload = {
            "player_id": pa["player_id"],
            "player":    pa["player"],
            "opponent_id": pb["player_id"],
            "opponent":    pb["player"],
            "tournament": TOURNAMENT,
            "years_back": YEARS_BACK
        }
        r = call_matchup(payload)
        prob_a = float(r.get("prob_player", 0.5))
        win_a = (random.random() < prob_a) if sample else (prob_a >= 0.5)
        winner = a if win_a else b
        results.append({
            "a": a.get("name") or a.get("id"),
            "b": b.get("name") or b.get("id"),
            "a_id": a.get("id"),
            "b_id": b.get("id"),
            "prob_a": round(prob_a,6),
            "winner": winner.get("name") or winner.get("id"),
        })
        winners.append(winner)
    return results, winners

def simulate_once(entrants):
    round_num=1; bracket=[]; current=entrants[:]
    use_seeds = all(p.get("seed") is not None for p in current)
    while len(current)>1:
        res, winners = play_round(current, use_seeds if round_num==1 else False, sample=(MODE=="mc"))
        bracket.append({"round": round_num, "matches": res})
        current = winners
        round_num += 1
    return bracket, current[0]

def write_matches_csv(bracket, path):
    import csv
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["round","a","b","a_id","b_id","prob_a","winner"])
        for rnd in bracket:
            rnum = rnd["round"]
            for m in rnd["matches"]:
                w.writerow([rnum,m["a"],m["b"],m["a_id"],m["b_id"],m["prob_a"],m["winner"]])

def main():
    entrants = read_entrants(CSV)
    if MODE == "mc" and MC_RUNS > 0:
        wins = { (e["name"] or e["id"]):0 for e in entrants }
        example_bracket=None; example_champ=None
        for _ in range(MC_RUNS):
            bracket, champ = simulate_once(entrants)
            wins[champ.get("name") or champ.get("id")] += 1
            if example_bracket is None:
                example_bracket, example_champ = bracket, champ
        probs = []
        for e in entrants:
            key = e.get("name") or e.get("id")
            probs.append({
                "id": e.get("id"),
                "name": e.get("name"),
                "seed": e.get("seed"),
                "p_champion": wins[key]/MC_RUNS
            })
        out = {
            "ok": True,
            "mode": "mc",
            "mc_runs": MC_RUNS,
            "tournament": TOURNAMENT,
            "years_back": YEARS_BACK,
            "champion_probs": sorted(probs, key=lambda x: x["p_champion"], reverse=True),
            "example_bracket": example_bracket,
            "example_champion": example_champ
        }
        print("== BRACKET (MC) ==")
        print(json.dumps(out, indent=2))
        write_matches_csv(example_bracket, "/tmp/bracket_matches.csv")
        with open("/tmp/bracket.json","w",encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
    else:
        bracket, champ = simulate_once(entrants)
        out = {
            "ok": True,
            "mode": "deterministic",
            "tournament": TOURNAMENT,
            "years_back": YEARS_BACK,
            "bracket": bracket,
            "champion": champ
        }
        print("== BRACKET ==")
        print(json.dumps(out, indent=2))
        write_matches_csv(bracket, "/tmp/bracket_matches.csv")
        with open("/tmp/bracket.json","w",encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
