function evaluarValueBetConEstadisticas() {
  const hojaFiltro = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Filtro Value Bet");
  const hojaIDs = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("player_ids");
  const row = hojaFiltro.getActiveRange().getRow();

  const nombreJugador = hojaFiltro.getRange(row, 1).getValue().toString().trim();
  const nombreRival = hojaFiltro.getRange(row, 2).getValue().toString().trim();

  if (!nombreJugador || !nombreRival) {
    SpreadsheetApp.getUi().alert("Completa Jugador y Rival.");
    return;
  }

  const idJugador = buscarPlayerID(nombreJugador, hojaIDs);
  const idRival = buscarPlayerID(nombreRival, hojaIDs);

  if (!idJugador || !idRival) {
    hojaFiltro.getRange(row, 3).setValue("❌ ID no encontrado");
    return;
  }

  const url = "https://estratego-api.onrender.com/";
  const payload = {
    "jugador": idJugador,
    "rival": idRival
  };

  const options = {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
    followRedirects: true,
    validateHttpsCertificates: true,
    escaping: false,
    timeout: 60000  // 60 segundos de espera
  };

  const response = UrlFetchApp.fetch(url, options);
  const result = JSON.parse(response.getContentText());
  Logger.log(result.ultimos_5_detalle);


  if (result.error) {
    hojaFiltro.getRange(row, 3).setValue("❌ " + result.error);
    return;
  }

  // Criterios reales basados en los nuevos datos
  const [winsJugador, winsRival] = result.h2h.split(" - ").map(n => parseInt(n));

const criterios = [
  parseInt(result.ultimos_5_ganados) >= 3 ? "✔" : "✘",                      // Criterio 1
  result.victorias_porcentaje > 60 ? "✔" : "✘",                   // Criterio 2
  result.porcentaje_superficie > 60 ? "✔" : "✘",                  // Criterio 3
  result.ranking <= 30 ? "✔" : "✘",                                // Criterio 4
  winsJugador > winsRival ? "✔" : "✘",                            // Criterio 5 (nuevo H2H)
  result.motivacion_por_puntos === "✔" ? "✔" : "✘",  // Motivación
  result.cambio_superficie === "✔" ? "✔" : "✘",  // Cambio de superficie
  result.estado_fisico === "✔" ? "✔" : "✘",
  result.torneo_local === "✔" ? "✔" : "✘"
];

  // Escribir criterios
  for (let i = 0; i < criterios.length; i++) {
    hojaFiltro.getRange(row, i + 3).setValue(criterios[i]);
  }

  const total = criterios.filter(c => c === "✔").length;
  hojaFiltro.getRange(row, 12).setValue(total);
  const decision = total >= 7 ? "✅ Apostar" : total === 6 ? "⚠️ Revisar" : "❌ No apostar";
  hojaFiltro.getRange(row, 13).setValue(decision);

// Mostrar resumen visual de los últimos 5 partidos (columna 15)
try {
  if (Array.isArray(result.ultimos_5_detalle) && result.ultimos_5_detalle.length > 0) {
    const resumenPartidos = result.ultimos_5_detalle.join("\n");
    hojaFiltro.getRange(row, 15).setValue(resumenPartidos);
  } else {
    hojaFiltro.getRange(row, 15).setValue("⛔ No se recibió detalle");
  }
} catch (err) {
  hojaFiltro.getRange(row, 15).setValue("❌ Error en resumen: " + err.message);
}

// Columna 14: resumen textual
const resumenTexto = [
  `${parseInt(result.ultimos_5_ganados) >= 3 ? "✔" : "✘"} Ganó ${result.ultimos_5_ganados}/5`,
  `${result.victorias_porcentaje > 60 ? "✔" : "✘"} Win% anual: ${result.victorias_porcentaje}%`,
  `${result.porcentaje_superficie > 60 ? "✔" : "✘"} Superficie: ${result.porcentaje_superficie}%`,
  `${result.ranking <= 30 ? "✔" : "✘"} Ranking: ${result.ranking}`,
  `${winsJugador > winsRival ? "✔" : "✘"} H2H: ${result.h2h}`,
  `${result.torneo_local === "✔" ? "✔" : "✘"} Torneo: ${result.torneo_nombre}`,
  `${result.estado_fisico === "✔" ? "✔" : "✘"} Físico: ${result.dias_sin_jugar}`,
  `${result.cambio_superficie === "✔" ? "✔" : "✘"} Cambio de superficie`,
  `${result.motivacion_por_puntos === "✔" ? "✔" : "✘"} Puntos defendidos: ${result.puntos_defendidos} (${result.ronda_maxima || "—"}, ${result.torneo_actual})`
].join("\n");

hojaFiltro.getRange(row, 14).setValue(resumenTexto);

// 📥 Registrar log en hoja 'Log'
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let hojaLog = ss.getSheetByName("Log");
    if (!hojaLog) {
      hojaLog = ss.insertSheet("Log");
      hojaLog.appendRow(["Fecha", "Jugador", "Log técnico"]);
    }
    const fecha = new Date();
    hojaLog.appendRow([fecha, nombreJugador, result.log_debug || "—"]);
  } catch (e) {
    Logger.log("❌ Error escribiendo en hoja Log: " + e.message);
  }

}

function buscarPlayerID(nombre, hoja) {
  const datos = hoja.getRange(2, 1, hoja.getLastRow() - 1, 2).getValues();
  const normalizado = nombre.toLowerCase().trim();
  for (let i = 0; i < datos.length; i++) {
    if (datos[i][0].toLowerCase().trim() === normalizado) {
      return datos[i][1];
    }
  }
  return null;
}
