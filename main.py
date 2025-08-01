from flask import Flask, request, jsonify
import requests
import unicodedata

app = Flask(__name__)

API_KEY = "e4ufC11rvWZ7OXEKFhI1yKAiSsfH3Rv65viqBmJv"  # Reemplaza esto con tu API Key real de Sportradar

@app.route('/', methods=['POST'])
def evaluar():
    data = request.get_json()
    jugador_id = data.get("jugador")
    rival_id = data.get("rival")

    if not jugador_id or not rival_id:
        return jsonify({"error": "Faltan IDs de jugador o rival"}), 400

    try:
        jugador_stats = obtener_estadisticas_jugador(jugador_id)
        ultimos5, detalle5 = obtener_ultimos5_winnerid(jugador_id)
        h2h = obtener_h2h_extend(jugador_id, rival_id)

        return jsonify({
            "jugador_id": jugador_id,
            "rival_id": rival_id,
            "ranking": jugador_stats["ranking"],
            "victorias_totales_2025": jugador_stats["victorias_totales"],
            "partidos_totales_2025": jugador_stats["partidos_totales"],
            "victorias_porcentaje": jugador_stats["porcentaje_victorias"],
            "victorias_en_superficie": jugador_stats["victorias_en_superficie"],
            "partidos_en_superficie": jugador_stats["partidos_en_superficie"],
            "porcentaje_superficie": jugador_stats["porcentaje_superficie"],
            "ultimos_5_ganados": ultimos5,
            "ultimos_5_detalle": detalle5,
            "h2h": h2h
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

def obtener_estadisticas_jugador(player_id):
    url = f"https://api.sportradar.com/tennis/trial/v3/en/competitors/{player_id}/profile.json?api_key={API_KEY}"
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception("No se pudo obtener el perfil del jugador")

    data = r.json()
    ranking = data["competitor_rankings"][0]["rank"]
    total_wins = 0
    total_matches = 0
    clay_wins = 0
    clay_matches = 0

    for periodo in data.get("periods", []):
        if periodo["year"] == 2025:
            for surface in periodo["surfaces"]:
                stats = surface["statistics"]
                wins = stats.get("matches_won", 0)
                played = stats.get("matches_played", 0)
                total_wins += wins
                total_matches += played
                if "clay" in surface["type"]:
                    clay_wins += wins
                    clay_matches += played

    porcentaje_total = (total_wins / total_matches * 100) if total_matches else 0
    porcentaje_clay = (clay_wins / clay_matches * 100) if clay_matches else 0

    return {
        "ranking": ranking,
        "victorias_totales": total_wins,
        "partidos_totales": total_matches,
        "porcentaje_victorias": round(porcentaje_total, 1),
        "victorias_en_superficie": clay_wins,
        "partidos_en_superficie": clay_matches,
        "porcentaje_superficie": round(porcentaje_clay, 1)
    }

def obtener_ultimos5_winnerid(player_id):
    url = f"https://api.sportradar.com/tennis/trial/v3/en/competitors/{player_id}/summaries.json"
    headers = {"accept": "application/json", "x-api-key": API_KEY}
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        return -1, ["❌ Error al consultar API"]

    data = r.json()
    summaries = data.get("summaries", [])[:5]
    ganados = 0
    detalle = []

    for s in summaries:
        winner_id = s.get("sport_event_status", {}).get("winner_id")
        if not winner_id:
            resultado = "—"
        elif winner_id == player_id:
            resultado = "✔ Ganado"
            ganados += 1
        else:
            resultado = "✘ Perdido"

        rival = next((c for c in s["sport_event"]["competitors"] if c["id"] != player_id), {}).get("name", "¿?")
        detalle.append(f"{resultado} vs {rival}")

    return ganados, detalle



def obtener_h2h_extend(jugador_id, rival_id):
    url = f"https://api.sportradar.com/tennis/trial/v3/en/competitors/{jugador_id}/versus/{rival_id}/summaries.json"
    headers = {"accept": "application/json", "x-api-key": API_KEY}
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        return "Sin datos"
    data = r.json()
    partidos = data.get("summaries", [])
    ganados = sum(1 for p in partidos if p.get("winner_id") == jugador_id)
    perdidos = sum(1 for p in partidos if p.get("winner_id") == rival_id)
    return f"{ganados} - {perdidos}"

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=10000)

  



