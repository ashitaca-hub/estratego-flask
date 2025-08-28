# scripts/simulate_bracket_from_csv.py
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

def call_matchup(pid, oid):
    payload = {
        "player_id": pid,
        "opponent_id": oid,
        "tournament": TOURNAMENT,
        "years_back": YEARS_BACK
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(API, data=data, headers={"Content-Type":"application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))

def read_entrants(path):
    rows=[]
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            seed = r.get("seed")
            seed = int(seed) if seed and seed.strip() else None
            rows.append({"seed": seed, "id": r["id"].strip(), "name": r.get("name","").strip()})
    if not rows or len(rows)%2!=0:
        raise ValueError("El CSV debe tener un número par de jugadores (>0).")
    return rows

def first_round_pairs_by_seed(players):
    ps = sorted(players, key=lambda x: x["seed"])
    n = len(ps)
    return [(ps[i], ps[n-1-i]) for i in range(n//2)]

def round_pairs(players):
    return [(players[i], players[i+1]) for i in range(0,len(players),2)]

def play_round(players, use_seeds, sample=False):
    pairs = first_round_pairs_by_seed(players) if use_seeds else round_pairs(players)
    results=[]; winners=[]
    for a,b in pairs:
        r = call_matchup(a["id"], b["id"])
        prob_a = float(r.get("prob_player", 0.5))
        if sample:
            win_a = random.random() < prob_a
        else:
            win_a = prob_a >= 0.5
        winner = a if win_a else b
        results.append({
            "a": a["name"], "b": b["name"],
            "a_id": a["id"], "b_id": b["id"],
            "prob_a": round(prob_a,6), "winner": winner["name"]
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
        wins = {e["id"]:0 for e in entrants}
        example_bracket=None; example_champ=None
        for _ in range(MC_RUNS):
            bracket, champ = simulate_once(entrants)
            wins[champ["id"]] += 1
            if example_bracket is None:
                example_bracket, example_champ = bracket, champ
        probs = []
        for e in entrants:
            probs.append({
                "id": e["id"],
                "name": e["name"],
                "seed": e.get("seed"),
                "p_champion": wins[e["id"]]/MC_RUNS
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
        # artefactos
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
        # artefactos
        write_matches_csv(bracket, "/tmp/bracket_matches.csv")
        with open("/tmp/bracket.json","w",encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
