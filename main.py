from __future__ import annotations
from flask import Flask, request, jsonify, Response
from datetime import datetime, timezone
import os, re, json, logging, urllib.parse, requests
from typing import Any

# Servicios/Utilidades
from services import supabase_fs as FS
from services import sportradar_now as SR
from utils.scoring import logistic, clamp, WEIGHTS, ADJUSTS


# -----------------------------------------------------------------------------
# App / Logging
# -----------------------------------------------------------------------------
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

@app.errorhandler(Exception)
def handle_exception(e):
    return jsonify({"error": str(e)}), 500

@app.get("/health")
def health():
    return jsonify({"ok": True}), 200

@app.get("/healthz")
def healthz():
    return health()  # alias

# -----------------------------------------------------------------------------
# Sportradar config
# -----------------------------------------------------------------------------
SR_API_KEY = os.environ.get("SR_API_KEY", "").strip()
SR_BASE = "https://api.sportradar.com/tennis/trial/v3/en"

def _sr_url(path: str, params: dict[str, Any] | None = None) -> str:
    if not SR_API_KEY:
        app.logger.warning("SR_API_KEY no configurada (modo NOW desactivado).")
    params = (params or {}).copy()
    params["api_key"] = SR_API_KEY or "REPLACE_ME"
    return f"{SR_BASE}/{path}?{urllib.parse.urlencode(params)}"

def _sr_get(path: str, params: dict[str, Any] | None = None, timeout=15) -> requests.Response:
    url = _sr_url(path, params)
    redacted = re.sub(r'api_key=[^&]+', 'api_key=***', url)
    app.logger.info("SR GET %s", redacted)
    r = requests.get(url, timeout=timeout, headers={"accept": "application/json"})
    app.logger.info("SR RESP %s (ratelimit-remaining=%s)", r.status_code, r.headers.get("x-ratelimit-remaining"))
    return r

# -----------------------------------------------------------------------------
# ENDPOINT '/' (evaluador original)
# -----------------------------------------------------------------------------
@app.route('/', methods=['POST'])
def evaluar():
    data = request.get_json()
    if data is None:
        return jsonify({"error": "No se proporcionaron datos JSON en la solicitud"}), 400

    jugador_id = data.get("jugador")
    rival_id = data.get("rival")
    superficie_objetivo = data.get("superficie_objetivo")

    if not jugador_id or not rival_id:
        return jsonify({"error": "Faltan IDs de jugador o rival"}), 400

    try:
        r_resumen = _sr_get(f"competitors/{jugador_id}/summaries.json")
        if r_resumen.status_code != 200:
            return jsonify({"error": "❌ Error al obtener summaries.json"}), 500
        resumen_data = r_resumen.json()

        jugador_stats = obtener_estadisticas_jugador(jugador_id)
        superficie_favorita, porcentaje_superficie_favorita = calcular_superficie_favorita(jugador_id)
        ultimos5, detalle5 = obtener_ultimos5_winnerid(jugador_id, resumen_data)
        torneo_local, nombre_torneo = evaluar_torneo_favorito(jugador_id, resumen_data)
        h2h = obtener_h2h_extend(jugador_id, rival_id)
        estado_fisico, dias_sin_jugar = evaluar_actividad_reciente(jugador_id, resumen_data)
        puntos_defendidos, torneo_actual, motivacion_por_puntos, ronda_maxima, log_debug, _ = obtener_puntos_defendidos(jugador_id)
        cambio_superficie_bool = False
        if superficie_objetivo:
            cambio_superficie_bool = viene_de_cambio_de_superficie(jugador_id, superficie_objetivo)

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
            "superficie_favorita": superficie_favorita,
            "porcentaje_superficie_favorita": porcentaje_superficie_favorita,
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
            "log_debug": log_debug,
            "h2h": h2h,
            "cambio_superficie": "✔" if cambio_superficie_bool else "✘"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# -----------------------------------------------------------------------------
# Helpers Sportradar (perfil, últimos, h2h, etc.)
# -----------------------------------------------------------------------------
def obtener_estadisticas_jugador(player_id, year=datetime.now().year):
    r = _sr_get(f"competitors/{player_id}/profile.json")
    if r.status_code != 200:
        raise Exception("No se pudo obtener el perfil del jugador")

    data = r.json()
    ranking = data.get("competitor_rankings", [{}])[0].get("rank", None)
    total_wins = 0
    total_matches = 0
    clay_wins = 0
    clay_matches = 0

    for periodo in data.get("periods", []):
        if periodo.get("year") == year:
            for surface in periodo.get("surfaces", []):
                stats = surface.get("statistics", {})
                wins = stats.get("matches_won", 0)
                played = stats.get("matches_played", 0)
                total_wins += wins
                total_matches += played
                if "clay" in surface.get("type", ""):
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
        "porcentaje_superficie": round(porcentaje_clay, 1),
    }

def calcular_superficie_favorita(player_id):
    r = _sr_get(f"competitors/{player_id}/profile.json")
    if r.status_code != 200:
        raise Exception("No se pudo obtener el perfil del jugador")
    data = r.json()

    superficie_stats = {}
    for periodo in data.get("periods", []):
        for surface in periodo.get("surfaces", []):
            nombre = surface.get("type")
            stats = surface.get("statistics", {})
            wins = stats.get("matches_won", 0)
            played = stats.get("matches_played", 0)
            if nombre not in superficie_stats:
                superficie_stats[nombre] = {"won": 0, "played": 0}
            superficie_stats[nombre]["won"] += wins
            superficie_stats[nombre]["played"] += played

    mejor_superficie, mejor_porcentaje = None, -1
    for nombre, stats in superficie_stats.items():
        played = stats["played"]
        porcentaje = (stats["won"] / played * 100) if played else 0
        if porcentaje > mejor_porcentaje:
            mejor_superficie, mejor_porcentaje = nombre, porcentaje

    return mejor_superficie, round(mejor_porcentaje, 1)

def obtener_ultimos5_winnerid(player_id, resumen_data):
    summaries = resumen_data.get("summaries", [])[:5]
    ganados, detalle = 0, []
    for s in summaries:
        winner_id = s.get("sport_event_status", {}).get("winner_id")
        if not winner_id:
            resultado = "—"
        elif winner_id == player_id:
            resultado = "✔ Ganado"; ganados += 1
        else:
            resultado = "✘ Perdido"
        rival = next((c for c in s.get("sport_event", {}).get("competitors", []) if c.get("id") != player_id), {}).get("name", "¿?")
        detalle.append(f"{resultado} vs {rival}")
    return ganados, detalle

def obtener_h2h_extend(jugador_id, rival_id):
    r = _sr_get(f"competitors/{jugador_id}/versus/{rival_id}/summaries.json")
    if r.status_code != 200:
        return "Sin datos"
    data = r.json()
    partidos = data.get("last_meetings", [])
    ganados = sum(1 for p in partidos if p.get("sport_event_status", {}).get("winner_id") == jugador_id)
    perdidos = sum(1 for p in partidos if p.get("sport_event_status", {}).get("winner_id") == rival_id)
    return f"{ganados} - {perdidos}"

def viene_de_cambio_de_superficie(jugador_id, superficie_objetivo):
    r = _sr_get(f"competitors/{jugador_id}/summaries.json")
    if r.status_code != 200:
        return False
    data = r.json()
    summaries = data.get("summaries", [])
    if not summaries:
        return False
    surface_actual = (
        summaries[0].get("sport_event", {}).get("sport_event_context", {}).get("surface", {}).get("name")
    )
    if not surface_actual or not superficie_objetivo:
        return False
    return (surface_actual or "").lower() != (superficie_objetivo or "").lower()

def evaluar_torneo_favorito(player_id, resumen_data):
    perfil = _sr_get(f"competitors/{player_id}/profile.json")
    if perfil.status_code != 200:
        return "❌", "Error perfil"
    jugador = perfil.json().get("competitor", {})
    jugador_pais = (jugador.get("country") or "").lower()

    summaries = resumen_data.get("summaries", [])
    if not summaries:
        return "❌", "Sin partidos"

    grupo = summaries[0].get("sport_event", {}).get("sport_event_context", {}).get("groups", [{}])[0]
    torneo = (grupo.get("name") or "").lower()

    # Fijamos si el torneo es “local” por país en el nombre del torneo
    si_juega = bool(jugador_pais and (jugador_pais in torneo))
    resultado = "✔" if si_juega else "✘"
    return resultado, torneo


def evaluar_actividad_reciente(player_id, resumen_data):
    summaries = resumen_data.get("summaries", [])
    if not summaries:
        return "❌", "Sin partidos"
    for e in summaries:
        fecha_str = e.get("sport_event", {}).get("start_time")
        if fecha_str:
            try:
                fecha = datetime.fromisoformat(fecha_str.replace("Z", "+00:00"))
                ahora = datetime.now(timezone.utc)
                dias = (ahora - fecha).days
                return ("✔" if dias <= 30 else "✘"), f"{dias} días sin competir"
            except Exception:
                continue
    return "❌", "Fecha inválida"

def obtener_puntos_defendidos(player_id):
    season_id = None

    r_seasons = _sr_get("seasons.json")
    if r_seasons.status_code != 200:
        logging.error("❌ Error al obtener seasons")
        return 0, "Error temporadas", "✘", "—", "❌ Error al obtener seasons", season_id
    seasons = r_seasons.json().get("seasons", [])

    r_resumen = _sr_get(f"competitors/{player_id}/summaries.json")
    if r_resumen.status_code != 200:
        logging.error("❌ Error al obtener summaries del jugador")
        return 0, "Error resumen", "✘", "—", "❌ Error al obtener summaries del jugador", season_id

    summaries = r_resumen.json().get("summaries", [])
    if not summaries:
        logging.warning("⚠️ No se encontraron partidos recientes para el jugador")
        return 0, "Sin partidos", "✘", "—", "⚠️ No se encontraron partidos recientes para el jugador", season_id

    contexto = summaries[0].get("sport_event", {}).get("sport_event_context", {})
    competition = contexto.get("competition", {})
    torneo_nombre = competition.get("name", "Desconocido")
    competition_id = competition.get("id", "")
    logging.info("🎾 Torneo actual detectado: %s", torneo_nombre)

    hoy = datetime.now(timezone.utc)
    año_pasado = str(hoy.year - 1)

    season_equivalencias = {
        "sr:season:124689": "sr:season:111494"
    }

    season_id_actual = contexto.get("season", {}).get("id", "")
    season_id_directa = season_equivalencias.get(season_id_actual)

    if season_id_directa is not None:
        season_id = season_id_directa
        log_debug = f"🎯 Usando season equivalente: {season_id_actual} → {season_id}"
        season_anterior = next((s for s in seasons if s["id"] == season_id), None)
    else:
        season_anterior = next(
            (s for s in seasons if str(s.get("year")) == año_pasado and s.get("competition_id") == competition_id),
            None
        )
        if not season_anterior:
            return 0, torneo_nombre, "✘", "—", "❌ No se encontró torneo del año pasado para este competition_id", season_id
        season_id = season_anterior["id"]
        log_debug = f"🔁 Usando season encontrada: {season_id}"

    if not season_anterior:
        logging.error("❌ No se encontró torneo del año pasado para este competition_id")
        return 0, torneo_nombre, "✘", "—", "❌ No se encontró torneo del año pasado para este competition_id", season_id

    r_torneo = _sr_get(f"seasons/{season_id}/summaries.json")
    if r_torneo.status_code != 200:
        logging.error("❌ Error al obtener partidos del torneo anterior")
        return 0, torneo_nombre, "✘", "—", "❌ No se encontró torneo del año pasado para este competition_id", season_id

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
        winner = (match.get("sport_event_status", {}) or {}).get("winner_id", "").lower()
        ronda = (match.get("sport_event", {}) or {}).get("sport_event_context", {}).get("round", {}).get("name", "").lower()
        if not winner or not ronda:
            continue
        if winner == str(player_id).lower() and ronda in orden_rondas:
            if not ronda_maxima or orden_rondas.index(ronda) > orden_rondas.index(ronda_maxima):
                ronda_maxima = ronda

    puntos = puntos_por_ronda.get(ronda_maxima, 0)
    motivacion = "✔" if puntos >= 45 else "✘"
    ronda_str = ronda_maxima if ronda_maxima else "—"
    log_debug = f"📣 Jugador {player_id} jugando en {torneo_nombre} llegó a la ronda {ronda_str}"
    return puntos, torneo_nombre, motivacion, ronda_str, log_debug, season_id

@app.route('/proximos_partidos', methods=['POST'])
def proximos_partidos():
    data = request.get_json()
    jugador_id = data.get("jugador")
    if not jugador_id:
        return jsonify({"error": "Falta ID de jugador"}), 400

    try:
        _, _, _, _, _, season_id = obtener_puntos_defendidos(jugador_id)
        if not season_id:
            return jsonify({"error": "No se encontró season_id"}), 500
        partidos = obtener_proximos_partidos(season_id)
        return jsonify({"season_id": season_id, "partidos": partidos})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/proximos_partidos_por_torneo', methods=['POST'])
def proximos_partidos_por_torneo():
    data = request.get_json()
    if not data or "torneo" not in data:
        return jsonify({"error": "Falta 'torneo' en la solicitud"}), 400
    torneo_full = data["torneo"]
    try:
        season_id = buscar_season_id_por_nombre(torneo_full)
        if not season_id:
            return jsonify({"error": "Torneo no encontrado"}), 404
        partidos = obtener_proximos_partidos(season_id)
        return jsonify({"season_id": season_id, "partidos": partidos})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def buscar_season_id_por_nombre(torneo_full: str) -> str | None:
    r = _sr_get("seasons.json")
    r.raise_for_status()
    tokens = re.findall(r"[a-z0-9]+", torneo_full.casefold())
    year = None
    tokens_without_year = []
    for tok in tokens:
        if len(tok) == 4 and tok.isdigit():
            year = tok
        else:
            tokens_without_year.append(tok)
    matches = []
    for season in r.json().get("seasons", []):
        season_name = season.get("name", "")
        season_cf = season_name.casefold()
        season_year = str(season.get("year"))
        if year and season_year != year:
            continue
        if all(tok in season_cf for tok in tokens_without_year):
            matches.append(season)
    if not matches:
        return None
    if year:
        return matches[0].get("id")
    latest = max(matches, key=lambda s: s.get("year", 0))
    return latest.get("id")

def obtener_proximos_partidos(season_id: str) -> list[dict]:
    r = _sr_get(f"seasons/{season_id}/summaries.json")
    r.raise_for_status()
    proximos = []
    for evento in r.json().get("summaries", []):
        status = evento.get("sport_event_status", {}).get("status")
        if status != "not_started":
            continue
        sport_event = evento.get("sport_event", {})
        start_time = sport_event.get("start_time")
        competitors = [c.get("name") for c in sport_event.get("competitors", [])]
        round_name = sport_event.get("sport_event_context", {}).get("round", {}).get("name")
        proximos.append({"start_time": start_time, "competitors": competitors, "round": round_name})
    proximos.sort(key=lambda p: (p["start_time"] is None, p["start_time"] or ""))
    return proximos

# -----------------------------------------------------------------------------
# ======== Estratego: /matchup (usa IDs INT canónicos; resuelve SR/nombre) ====
# -----------------------------------------------------------------------------
# Tablas/vistas canónicas para resolver IDs
PLAYER_TABLE_SRID = "players_lookup"  # public -> (player_id INT, name, ext_sportradar_id)
PLAYER_TABLE_NAME = "players_min"     # public -> (player_id INT, name)

# Pesos HIST calibrados (se pueden sobreescribir por ENV)
HIST_W_MONTH = float(os.getenv("HIST_W_MONTH", "0.5"))
HIST_W_SURF  = float(os.getenv("HIST_W_SURF",  "2.0"))
HIST_W_SPEED = float(os.getenv("HIST_W_SPEED", "2.0"))
_HIST_DENOM  = max(1.0, abs(HIST_W_MONTH) + abs(HIST_W_SURF) + abs(HIST_W_SPEED))

def _sr_short_to_int_any(v):
    try:
        if isinstance(v, int):
            return v
        if isinstance(v, str) and v.startswith("sr:competitor:"):
            return int(v.rsplit(":", 1)[1])
    except:
        pass
    return None

def _rest_get(table: str, params: dict, select: str = "*"):
    base = FS.SUPABASE_URL.rstrip("/") + "/rest/v1/" + table
    q = {"select": select}; q.update(params or {})
    url = base + "?" + urllib.parse.urlencode(q, doseq=True)
    r = requests.get(url, headers=FS.HEADERS_SB, timeout=FS.HTTP_TIMEOUT)
    r.raise_for_status()
    return r.json()

def _normalize_sr_id(val: str | None) -> str | None:
    if not val:
        return None
    s = str(val)
    return s if s.startswith("sr:") else f"sr:competitor:{s}"

def _resolve_player_int_by_name(name: str | None) -> int | None:
    if not name:
        return None
    rows = _rest_get(PLAYER_TABLE_NAME, {"name": f"ilike.*{name}*", "limit": 1}, select="player_id,name")
    return int(rows[0]["player_id"]) if rows else None

def _resolve_player_int_by_sr(sr_id: str | None) -> int | None:
    if not sr_id:
        return None
    short = sr_id.split(":")[-1]
    rows = _rest_get(PLAYER_TABLE_SRID, {"ext_sportradar_id": f"eq.{short}", "limit": 1}, select="player_id,name,ext_sportradar_id")
    return int(rows[0]["player_id"]) if rows else None

def _resolve_id(pid, pname, psrid) -> int | None:
    if isinstance(pid, int) or (isinstance(pid, str) and pid.isdigit()):
        return int(pid)
    sr = psrid or (pid if (isinstance(pid, str) and pid.startswith("sr:")) else None)
    if sr:
        rid = _resolve_player_int_by_sr(sr)
        if rid is not None:
            return rid
    if pname:
        rid = _resolve_player_int_by_name(pname)
        if rid is not None:
            return rid
    return None

def _tourney_meta_fallback(tname: str) -> dict:
    if not tname:
        return {}
    try:
        rows = _rest_get(
            "court_speed_rankig_norm",
            {"tournament_name": f"ilike.*{tname}*", "limit": 1},
            select="tournament_name,surface,speed_rank,speed_bucket,category"
        )
        if not rows:
            return {}
        row = rows[0]
        if not row.get("speed_bucket") and row.get("speed_rank") is not None:
            r = int(row["speed_rank"])
            row["speed_bucket"] = "Fast" if r <= 33 else ("Medium" if r <= 66 else "Slow")
        return row
    except Exception:
        return {}

def _compute_matchup_payload(body: dict) -> dict:
    years_back = int(body.get("years_back", 4))
    tourney = body.get("tournament", {}) or {}
    tname = tourney.get("name") or tourney.get("tourney_name") or ""
    month = int(tourney.get("month") or 1)

    p_id_in = body.get("player_id")
    o_id_in = body.get("opponent_id")
    p_sr_id = body.get("player_sr_id")
    o_sr_id = body.get("opponent_sr_id")
    player  = body.get("player")
    opponent= body.get("opponent")

    p_int = _resolve_id(p_id_in, player, p_sr_id)
    o_int = _resolve_id(o_id_in, opponent, o_sr_id)

    try:
        meta = FS.get_tourney_meta(tname) or {}
    except Exception:
        meta = {}
    if not meta:
        meta = _tourney_meta_fallback(tname) or {}
    surface_default = (meta.get("surface") or "hard").lower()
    speed_bucket_meta = meta.get("speed_bucket") or "Medium"

    using_sr = bool(SR_API_KEY)
    ttl_seconds = int(os.getenv("CACHE_TTL_SR_SECS", str(12*3600))) if using_sr else int(os.getenv("CACHE_TTL_HIST_SECS", str(30*24*3600)))

    if p_int is not None and o_int is not None:
        try:
            _tkey, cached = FS.get_matchup_cache_json(
                player_id=p_int, opponent_id=o_int,
                tournament_name=tname, mon=month,
                speed_bucket=speed_bucket_meta or "",
                years_back=years_back, using_sr=using_sr
            )
        except Exception as e:
            app.logger.warning(f"cache get failed: {e}")
            cached = None
        if cached:
            if isinstance(cached, str):
                try:
                    cached = json.loads(cached)
                except Exception:
                    cached = {}
            prob_cached = float(cached.get("prob_player", 0.5))
            features_cached = cached.get("features", {})
            flags_cached = cached.get("flags", {})
            out_cached = {
                "ok": True,
                "prob_player": prob_cached,
                "surface": surface_default,
                "speed_bucket": speed_bucket_meta,
                "inputs": {
                    "player": player, "opponent": opponent,
                    "player_id": p_int if p_int is not None else p_id_in,
                    "opponent_id": o_int if o_int is not None else o_id_in,
                    "player_sr_id": _normalize_sr_id(p_sr_id or (p_id_in if (isinstance(p_id_in, str) and p_id_in.startswith("sr:")) else None)),
                    "opponent_sr_id": _normalize_sr_id(o_sr_id or (o_id_in if (isinstance(o_id_in, str) and o_id_in.startswith("sr:")) else None)),
                    "tournament": {"name": tname, "month": month},
                    "years_back": years_back
                },
                "features": {
                    "deltas": features_cached.get("deltas", {}),
                    "flags":  features_cached.get("flags",  flags_cached)
                },
                "weights_hist": cached.get("weights_hist"),
                "components": {"cached": True}
            }
            return out_cached

    hist = {}
    if p_int is not None and o_int is not None:
        try:
            hist = FS.get_matchup_hist_vector(
                p_id=p_int, o_id=o_int, yrs=years_back, tname=tname, month=month
            ) or {}
        except Exception:
            hist = {}
    if not hist:
        hist = {
            "surface": surface_default,
            "speed_bucket": speed_bucket_meta,
            "d_hist_surface": 0.0,
            "d_hist_speed":   0.0,
            "d_hist_month":   0.0
        }

    p_sr_norm = _normalize_sr_id(p_sr_id or (p_id_in if (isinstance(p_id_in, str) and p_id_in.startswith("sr:")) else None))
    o_sr_norm = _normalize_sr_id(o_sr_id or (o_id_in if (isinstance(o_id_in, str) and o_id_in.startswith("sr:")) else None))

    if p_sr_norm is None and isinstance(p_int, int):
        try:
            p_sr_norm = FS.get_sr_id_from_player_int(p_int)
        except Exception:
            pass
    if o_sr_norm is None and isinstance(o_int, int):
        try:
            o_sr_norm = FS.get_sr_id_from_player_int(o_int)
        except Exception:
            pass

    try:
        profile_p = SR.get_profile(p_sr_norm) if p_sr_norm else {}
        profile_o = SR.get_profile(o_sr_norm) if o_sr_norm else {}
        last10_p  = SR.get_last10(p_sr_norm)  if p_sr_norm else []
        last10_o  = SR.get_last10(o_sr_norm)  if o_sr_norm else []
        ytd_p     = SR.get_ytd_record(p_sr_norm) if p_sr_norm else {"wins":0, "losses":0}
        ytd_o     = SR.get_ytd_record(o_sr_norm) if o_sr_norm else {"wins":0, "losses":0}
        h2h_w, h2h_l = SR.get_h2h(p_sr_norm, o_sr_norm) if p_sr_norm and o_sr_norm else (0, 0)
    except Exception:
        profile_p, profile_o, last10_p, last10_o = {}, {}, [], []
        ytd_p, ytd_o = {"wins":0, "losses":0}, {"wins":0, "losses":0}
        h2h_w, h2h_l = 0, 0

    now_p = SR.compute_now_features(profile_p, last10_p, ytd_p)
    now_o = SR.compute_now_features(profile_o, last10_o, ytd_o)

    surf_change_p = 1 if (now_p.get("last_surface") and now_p["last_surface"] != str(hist.get("surface","")).lower()) else 0
    surf_change_o = 1 if (now_o.get("last_surface") and now_o["last_surface"] != str(hist.get("surface","")).lower()) else 0
    is_local_p = 1 if body.get("country") and body.get("player_country") and body["country"] == body["player_country"] else 0
    is_local_o = 1 if body.get("country") and body.get("opponent_country") and body["country"] == body["opponent_country"] else 0
    mot_p = int(body.get("mot_points_p") or 0)
    mot_o = int(body.get("mot_points_o") or 0)

    rank_p = now_p.get("ranking_now") or 999
    rank_o = now_o.get("ranking_now") or 999
    d_rank_norm   = clamp((rank_o - rank_p) / 100.0, -1, 1)
    d_ytd         = clamp(now_p["winrate_ytd"]    - now_o["winrate_ytd"],    -0.25, 0.25)
    d_last10      = clamp(now_p["winrate_last10"] - now_o["winrate_last10"], -0.25, 0.25)
    d_h2h         = clamp(((h2h_w + 5) / max(1, (h2h_w + h2h_l + 10))) - ((h2h_l + 5) / max(1, (h2h_w + h2h_l + 10))), -0.25, 0.25)
    d_inactive    = clamp(-(now_p["days_inactive"] - now_o["days_inactive"]) / 30.0, -0.25, 0.25)

    d_hist_surface = clamp(hist.get("d_hist_surface", 0.0), -0.25, 0.25)
    d_hist_speed   = clamp(hist.get("d_hist_speed",   0.0), -0.25, 0.25)
    d_hist_month   = clamp(hist.get("d_hist_month",   0.0), -0.25, 0.25)

    now_linear = (
        WEIGHTS["rank_norm"]   * d_rank_norm   +
        WEIGHTS["ytd"]         * d_ytd         +
        WEIGHTS["last10"]      * d_last10      +
        WEIGHTS["h2h"]         * d_h2h         +
        WEIGHTS["inactive"]    * d_inactive
    )
    hist_linear = (
        HIST_W_MONTH * d_hist_month +
        HIST_W_SURF  * d_hist_surface +
        HIST_W_SPEED * d_hist_speed
    ) / _HIST_DENOM

    adj = 0.0
    adj += ADJUSTS["surf_change"] * (surf_change_p - surf_change_o)
    adj += ADJUSTS["local"]       * (is_local_p - is_local_o)
    adj += ADJUSTS["mot_points"]  * (mot_p - mot_o)

    z = now_linear + hist_linear + adj
    prob_player = logistic(z)

    features = {
        "deltas": {
            "rank_norm": d_rank_norm,
            "ytd": d_ytd, "last10": d_last10, "h2h": d_h2h, "inactive": d_inactive,
            "hist_surface": d_hist_surface, "hist_speed": d_hist_speed, "hist_month": d_hist_month
        },
        "flags": {
            "surf_change_p": surf_change_p, "surf_change_o": surf_change_o,
            "is_local_p": is_local_p, "is_local_o": is_local_o, "mot_p": mot_p, "mot_o": mot_o
        }
    }

    if p_int is not None and o_int is not None:
        try:
            FS.put_matchup_cache_json(
                player_id=p_int, opponent_id=o_int,
                tournament_name=tname, mon=month,
                surface=str(hist.get("surface", surface_default)).lower(),
                speed_bucket=str(hist.get("speed_bucket", speed_bucket_meta)),
                years_back=years_back, using_sr=bool(SR_API_KEY),
                prob_player=float(prob_player),
                features=features, flags=features["flags"],
                weights_hist={"month": HIST_W_MONTH, "surface": HIST_W_SURF, "speed": HIST_W_SPEED, "denom": _HIST_DENOM},
                sources={"source": "api", "ver": "v1"},
                ttl_seconds=ttl_seconds
            )
        except Exception as e:
            app.logger.warning(f"matchup_cache upsert failed: {e}")

    return {
        "ok": True,
        "prob_player": prob_player,
        "surface": hist.get("surface", surface_default),
        "speed_bucket": hist.get("speed_bucket", speed_bucket_meta),
        "inputs": {
            "player": player, "opponent": opponent,
            "player_id": p_int if p_int is not None else p_id_in,
            "opponent_id": o_int if o_int is not None else o_id_in,
            "player_sr_id": p_sr_norm,
            "opponent_sr_id": o_sr_norm,
            "tournament": {"name": tname, "month": month},
            "years_back": years_back
        },
        "features": features,
        "weights_hist": { "month": HIST_W_MONTH, "surface": HIST_W_SURF, "speed": HIST_W_SPEED, "denom": _HIST_DENOM },
        "components": { "now_linear": now_linear, "hist_linear": hist_linear, "adj": adj, "z": z, "cached": False }
    }

@app.post("/matchup")
def matchup():
    body = request.get_json(force=True, silent=True) or {}
    out = _compute_matchup_payload(body)
    resp = {
        "ok": out["ok"],
        "prob_player": out["prob_player"],
        "surface": out["surface"],
        "speed_bucket": out["speed_bucket"],
        "inputs": out["inputs"],
        "features": out["features"],
        "weights_hist": out.get("weights_hist"),
    }
    return jsonify(resp), 200

@app.post("/matchup/features")
def matchup_features():
    body = request.get_json(force=True, silent=True) or {}
    out = _compute_matchup_payload(body)
    return jsonify(out), 200

# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------




BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def _json_for_js(obj: dict) -> str:
    s = json.dumps(obj, ensure_ascii=False)
    return s.replace("</", "<\\/")  # evita cerrar <script> por accidente

def _render_prematch_with_template(resp_dict: dict) -> str | None:
    """
    Carga apps_script/prematch_template.html (o la ruta de PREMATCH_TEMPLATE),
    e inyecta:  const resp = {...};
    """
    tpl_path = os.environ.get(
        "PREMATCH_TEMPLATE",
        os.path.join(BASE_DIR, "apps_script", "prematch_template.html")
    )
    try:
        with open(tpl_path, "r", encoding="utf-8") as f:
            tpl = f.read()
    except Exception:
        return None

    js = f"const resp = {_json_for_js(resp_dict)};"
    if re.search(r"//\s*const\s+resp\s*=", tpl):
        tpl = re.sub(r"//\s*const\s+resp\s*=\s*\{[\s\S]*?\};?", js, tpl, count=1)
    else:
        if "</head>" in tpl:
            tpl = tpl.replace("</head>", f"<script>{js}</script>\n</head>", 1)
        elif "</body>" in tpl:
            tpl = tpl.replace("</body>", f"<script>{js}</script>\n</body>", 1)
        else:
            tpl = tpl + f"\n<script>{js}</script>\n"
    return tpl

def _as_dict(x):
    """Convierte lo que devuelva tu lógica (/matchup) a dict."""
    from flask import Response as FlaskResp
    if isinstance(x, FlaskResp):
        try:
            return x.get_json(force=True) or {}
        except Exception:
            return {}
    if isinstance(x, tuple):
        return _as_dict(x[0])
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            return {}
    return {}
# --- fin helpers ---

def _delta_sides(delta: float) -> tuple[float, float]:
    """
    Devuelve (left%, right%) para una barra divergente centrada en 0.
    left = ventaja de B (negativo), right = ventaja de A (positivo).
    Cada lado se escala a 0..50 (% del ancho total).
    """
    try:
        d = max(-1.0, min(1.0, float(delta)))
    except Exception:
        d = 0.0
    left  = max(0.0, -d) * 50.0
    right = max(0.0,  d) * 50.0
    return (left, right)

def _pp(x: float | None) -> str:
    # 'percentage points' (puntos porcentuales)
    try:
        return f"{float(x)*100:.1f} pp"
    except Exception:
        return "—"

def _fmt_pct(x):
    try: return f"{100.0*float(x):.1f}%"
    except: return "—"

def _style_bar(v01: float) -> str:
    try: v = max(0.0, min(1.0, float(v01)))
    except: v = 0.0
    return f"width:{v*100:.1f}%;"

def _flag_from_cc(cc: str | None) -> str:
    # convierte "ES" -> 🇪🇸 (si no hay, devuelve "")
    if not cc or len(cc) != 2: return ""
    base = 127397
    return chr(ord(cc[0].upper())+base) + chr(ord(cc[1].upper())+base)

def _ensure_int_id(x):
    # el /matchup ya suele devolver IDs internos en inputs.player_id/opponent_id;
    # si vinieran SR (str), aquí podríamos mapearlos (players_lookup), pero
    # usamos directamente lo que entrega /matchup.
    return int(x) if isinstance(x, int) or (isinstance(x,str) and x.isdigit()) else None

def _hist_wr_asof_bundle(pid: int, mon: int, surface: str, speed_bucket: str, years_back: int):
    """
    Devuelve dict con wr_month, wr_surface, wr_speed (0..1 o None) usando funciones as-of del FS.
    """
    try:
        sql = """
        SELECT
          public.fs_month_winrate_asof($1,$2,$3,current_date)::float  AS wr_month,
          public.fs_surface_winrate_asof($1,$4,$3,current_date)::float AS wr_surface,
          public.fs_speed_winrate_asof($1,$5,$3,current_date)::float   AS wr_speed
        """
        rows = FS.pg_query(sql, (pid, mon, years_back, surface, speed_bucket)) or []
        return rows[0] if rows else {}
    except Exception:
        return {}

def _safe(val, default="—"):
    return default if val is None else val

def _fetch_now_metrics(sr_id: str | None) -> tuple[dict, str]:
    """
    Devuelve (metrics, display_name) para un sr:competitor:X.
    metrics: {ranking_now, winrate_ytd, wins_ytd, losses_ytd, winrate_last10, days_inactive, last_surface}
    display_name: nombre desde el profile si existe, o ''.
    """
    if not SR_API_KEY or not sr_id:
        return ({}, "")
    try:
        prof = SR.get_profile(sr_id) or {}
        last10 = SR.get_last10(sr_id) or []
        ytd = SR.get_ytd_record(sr_id) or {"wins": 0, "losses": 0}
        now = SR.compute_now_features(prof, last10, ytd) or {}
        name = ((prof.get("competitor") or {}).get("name")) or ""
        # completa wins/losses para mostrar
        now["wins_ytd"] = int(ytd.get("wins", 0))
        now["losses_ytd"] = int(ytd.get("losses", 0))
        return (now, name)
    except Exception:
        return ({}, "")

def _style_bar(value01: float) -> str:
    """Devuelve estilo width para una barra 0..1 (sanea límites)."""
    try:
        v = max(0.0, min(1.0, float(value01)))
    except Exception:
        v = 0.0
    return f"width:{v*100:.1f}%;"

@app.post("/matchup/prematch")
def prematch_html():
    payload = request.get_json(silent=True) or {}

    # 1) Calcula el mismo dict que devuelves en /matchup (JSON)
    try:
        resp_json = handle_matchup(payload)  # <-- usa tu función real
    except NameError:
        # Si no existe ese nombre en tu código, devuelve algo razonable
        resp_json = {"ok": False, "error": "compute function not wired"}

    resp_dict = _as_dict(resp_json)

    # 2) Intenta usar el template externo
    html = _render_prematch_with_template(resp_dict)
    if html is None:
        # 3) Fallback: siempre HTML (aunque sea simple) para que el workflow no falle
        html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Prematch</title></head>
<body>
<pre id="json"></pre>
<script>
const resp = {_json_for_js(resp_dict)};
document.getElementById('json').textContent = JSON.stringify(resp, null, 2);
</script>
</body></html>"""

    return Response(html, mimetype="text/html; charset=utf-8", status=200)





# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)







