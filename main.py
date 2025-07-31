from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

API_KEY = "e4ufC11rvWZ7OXEKFhI1yKAiSsfH3Rv65viqBmJv"  # ← Reemplaza con tu API KEY de Sportradar

@app.route('/', methods=['POST'])
def evaluar():
    data = request.get_json()
    jugador = data.get("jugador", "").strip()
    rival = data.get("rival", "").strip()

    if not jugador or not rival:
        return jsonify({"error": "Faltan jugador o rival"}), 400

    player_id = buscar_id_por_nombre(jugador)
    rival_id = buscar_id_por_nombre(rival)

    if not player_id or not rival_id:
        return jsonify({"error": "Jugador o rival no encontrados"}), 404

    ganados = obtener_ultimos_resultados(player_id)
    h2h = obtener_h2h(player_id, rival_id)

    return jsonify({
        "jugador": jugador,
        "rival": rival,
        "ganados_ultimos5": ganados,
        "h2h": h2h
    })

def buscar_id_por_nombre(nombre):
    url = f"https://api.sportradar.com/tennis/trial/v3/en/players.json?api_key={API_KEY}"
    r = requests.get(url)
    if r.status_code != 200:
        return None
    for p in r.json().get("players", []):
        if nombre.lower() in p["name"].lower():
            return p["id"]
    return None

def obtener_ultimos_resultados(player_id):
    url = f"https://api.sportradar.com/tennis/trial/v3/en/players/{player_id}/profile.json?api_key={API_KEY}"
    r = requests.get(url)
    if r.status_code != 200:
        return "No data"
    matches = r.json().get("matches", [])[:5]
    return f"{sum(1 for m in matches if m.get('winner_id') == player_id)}/5"

def obtener_h2h(player_id, rival_id):
    url = f"https://api.sportradar.com/tennis/trial/v3/en/head_to_head/{player_id}/{rival_id}.json?api_key={API_KEY}"
    r = requests.get(url)
    if r.status_code != 200:
        return "Sin datos"
    data = r.json()
    return f"{data.get('player1_wins', 0)} - {data.get('player2_wins', 0)}"

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=10000)
