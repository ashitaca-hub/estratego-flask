from flask import Flask, request, jsonify
from datetime import datetime, timezone
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
        torneo_local, nombre_torneo = evaluar_torneo_favorito(jugador_id)
        h2h = obtener_h2h_extend(jugador_id, rival_id)
        estado_fisico, dias_sin_jugar = evaluar_actividad_reciente(jugador_id)
        puntos_defendidos, torneo_actual, motivacion_por_puntos, ronda_maxima = obtener_puntos_defendidos(jugador_id)

 
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
            "torneo_local": torneo_local,
            "torneo_nombre": nombre_torneo,
            "estado_fisico": estado_fisico,
            "dias_sin_jugar": dias_sin_jugar,
            "puntos_defendidos": puntos_defendidos,
            "torneo_actual": torneo_actual,
            "motivacion_por_puntos": motivacion_por_puntos,
            "ronda_maxima": ronda_maxima,
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
    partidos = data.get("last_meetings", [])  # ✅ CORREGIDO

    ganados = 0
    perdidos = 0

    for p in partidos:
        winner_id = p.get("sport_event_status", {}).get("winner_id")  # ✅ ACCESO CORRECTO
        if winner_id == jugador_id:
            ganados += 1
        elif winner_id == rival_id:
            perdidos += 1

    return f"{ganados} - {perdidos}"


def evaluar_torneo_favorito(player_id):
    # Obtener país del jugador
    perfil_url = f"https://api.sportradar.com/tennis/trial/v3/en/competitors/{player_id}/profile.json"
    headers = {"accept": "application/json", "x-api-key": API_KEY}
    perfil = requests.get(perfil_url, headers=headers)
    if perfil.status_code != 200:
        return "❌", "Error perfil"

    jugador = perfil.json().get("competitor", {})
    jugador_pais = jugador.get("country", "").lower()

    # Obtener torneo del último partido
    resumen_url = f"https://api.sportradar.com/tennis/trial/v3/en/competitors/{player_id}/summaries.json"
    resumen = requests.get(resumen_url, headers=headers)
    if resumen.status_code != 200:
        return "❌", "Error torneo"

    summaries = resumen.json().get("summaries", [])
    if not summaries:
        return "❌", "Sin partidos"

    grupo = summaries[0].get("sport_event", {}).get("sport_event_context", {}).get("groups", [{}])[0]
    torneo = grupo.get("name", "").lower()

    resultado = "✔" if jugador_pais and jugador_pais in torneo else "✘"
    return resultado, torneo

def evaluar_actividad_reciente(player_id):
    url = f"https://api.sportradar.com/tennis/trial/v3/en/competitors/{player_id}/summaries.json"
    headers = {"accept": "application/json", "x-api-key": API_KEY}
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        return "❌", "Error resumen"

    summaries = r.json().get("summaries", [])
    if not summaries:
        return "❌", "Sin partidos"

    for e in summaries:
        fecha_str = e.get("sport_event", {}).get("start_time")
        if fecha_str:
            try:
                fecha = datetime.fromisoformat(fecha_str.replace("Z", "+00:00"))
                ahora = datetime.now(timezone.utc)
                dias = (ahora - fecha).days
                return "✔" if dias <= 30 else "✘", f"{dias} días sin competir"
            except Exception:
                continue

    return "❌", "Fecha inválida"


def obtener_puntos_defendidos(player_id):
    from datetime import datetime
    headers = {"accept": "application/json", "x-api-key": API_KEY}

    # 1. Obtener seasons
    r_seasons = requests.get("https://api.sportradar.com/tennis/trial/v3/en/seasons.json", headers=headers)
    if r_seasons.status_code != 200:
        return 0, "Error temporadas", "✘", "—"
    seasons = r_seasons.json().get("seasons", [])

    # 2. Obtener torneo actual desde últimos partidos
    resumen_url = f"https://api.sportradar.com/tennis/trial/v3/en/competitors/{player_id}/summaries.json"
    r_resumen = requests.get(resumen_url, headers=headers)
    if r_resumen.status_code != 200:
        return 0, "Error resumen", "✘", "—"

    summaries = r_resumen.json().get("summaries", [])
    if not summaries:
        return 0, "Sin partidos", "✘", "—"

    contexto = summaries[0].get("sport_event", {}).get("sport_event_context", {})
    competition = contexto.get("competition", {})
    torneo_nombre = competition.get("name", "Desconocido")
    competition_id = competition.get("id", "")

    # 3. Buscar edición anterior del torneo
    hoy = datetime.today()
    año_pasado = str(hoy.year - 1)
    season_anterior = next((s for s in seasons if s["year"] == año_pasado and s["competition_id"] == competition_id), None)

    if not season_anterior:
        return 0, torneo_nombre, "✘", "—"

    season_id = season_anterior["id"]

    # 4. Obtener todos los partidos del torneo anterior
    url_torneo = f"https://api.sportradar.com/tennis/trial/v3/en/seasons/{season_id}/summaries.json"
    r_torneo = requests.get(url_torneo, headers=headers)
    if r_torneo.status_code != 200:
        return 0, torneo_nombre, "✘", "—"

    data = r_torneo.json().get("summaries", [])
    ronda_maxima = None

    puntos_por_ronda = {
        "qualification_round_1": 0,
        "qualification_round_2": 0,
        "1st_round": 10,
        "2nd_round": 45,
        "round_of_16": 90,
        "quarterfinal": 180,
        "semifinal": 360,
        "final": 720,
        "champion": 1000
    }

    orden_rondas = list(puntos_por_ronda.keys())

    for match in data:
        ganador = match.get("sport_event_status", {}).get("winner_id")
        if ganador != player_id:
            continue

        ronda = match.get("sport_event", {}).get("sport_event_context", {}).get("round", {}).get("name", "")
        if ronda in orden_rondas:
            if not ronda_maxima or orden_rondas.index(ronda) > orden_rondas.index(ronda_maxima):
                ronda_maxima = ronda

    puntos = puntos_por_ronda.get(ronda_maxima, 0)
    motivacion = "✔" if puntos >= 45 else "✘"
    ronda_str = ronda_maxima if ronda_maxima else "—"

    return puntos, torneo_nombre, motivacion, ronda_str


    
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=10000)

  











