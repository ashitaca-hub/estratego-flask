# scripts/simulate_bracket.py
import json, urllib.request

API = "http://127.0.0.1:8080/matchup"

# 8 jugadores (puedes ampliarlo luego)
ENTRANTS = [
  {"seed":1, "id":"sr:competitor:407573", "name":"Carlos Alcaraz"},
  {"seed":8, "id":"sr:competitor:214182", "name":"Alex de Minaur"},
  {"seed":4, "id":"sr:competitor:136042", "name":"Taylor Fritz"},
  {"seed":5, "id":"sr:competitor:352776", "name":"Jack Draper"},
  {"seed":2, "id":"sr:competitor:225050", "name":"Jannik Sinner"},
  {"seed":7, "id":"sr:competitor:808628", "name":"Ben Shelton"},
  {"seed":3, "id":"sr:competitor:57163",  "name":"Alexander Zverev"},
  {"seed":6, "id":"sr:competitor:14882",  "name":"Novak Djokovic"},
]

TOURNAMENT = {"name":"Cincinnati","month":8}
YEARS_BACK = 4

def call_matchup(p, o):
    payload = {
      "player_id": p,
      "opponent_id": o,
      "tournament": TOURNAMENT,
      "years_back": YEARS_BACK
    }
    data = json.dumps(payload).encode("utf-8")
    req  = urllib.request.Request(API, data=data, headers={"Content-Type":"application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))

def first_round_pairs_by_seed(players):
    ps = sorted(players, key=lambda x: x["seed"])
    n = len(ps)
    return [(ps[i], ps[n-1-i]) for i in range(n//2)]

def round_pairs(players):
    return [(players[i], players[i+1]) for i in range(0, len(players), 2)]

def play_round(players, use_seeds):
    pairs = first_round_pairs_by_seed(players) if use_seeds else round_pairs(players)
    results, winners = [], []
    for a, b in pairs:
        r = call_matchup(a["id"], b["id"])
        prob = float(r.get("prob_player", 0.5))
        winner = a if prob >= 0.5 else b
        results.append({
            "a": a["name"], "b": b["name"],
            "a_id": a["id"], "b_id": b["id"],
            "prob_a": round(prob, 6), "winner": winner["name"]
        })
        winners.append(winner)
    return results, winners

def main():
    round_num = 1
    bracket = []
    current = ENTRANTS[:]
    use_seeds = all("seed" in p for p in current)
    while len(current) > 1:
        res, winners = play_round(current, use_seeds if round_num == 1 else False)
        bracket.append({"round": round_num, "matches": res})
        current = winners
        round_num += 1

    champion = current[0]
    out = {
        "ok": True,
        "tournament": TOURNAMENT,
        "years_back": YEARS_BACK,
        "bracket": bracket,
        "champion": champion
    }
    print("== BRACKET ==")
    print(json.dumps(out, indent=2))

if __name__ == "__main__":
    main()
