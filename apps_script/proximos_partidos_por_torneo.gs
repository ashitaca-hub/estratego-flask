function insertarProximosPartidosDesdeCelda() {
  const ss     = SpreadsheetApp.getActiveSpreadsheet();
  const sheet  = ss.getActiveSheet();
  let torneo   = sheet.getActiveCell().getValue().trim();

  if (!/\d{4}/.test(torneo)) {
    const year = new Date().getFullYear();
    torneo = `${torneo} ${year}`;
    SpreadsheetApp.getUi().alert(`Año ${year} añadido automáticamente.`);
  }

  const url = 'https://estratego-api.onrender.com/proximos_partidos_por_torneo';
  const params = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify({ torneo }),
    muteHttpExceptions: true
  };

  const resp = UrlFetchApp.fetch(url, params);
  Logger.log(resp.getResponseCode());
  Logger.log(resp.getContentText());
  const code = resp.getResponseCode();
  const data = JSON.parse(resp.getContentText());
  if (data.error || code !== 200) {
    const mensaje = data.error || `Error HTTP ${code}`;
    Logger.log(mensaje);
    SpreadsheetApp.getUi().alert(mensaje);
    return;
  }
  const partidos = data.partidos || [];

  // --- evitar conflicto de nombre ---
  let nuevaHoja = ss.getSheetByName('ProximosPartidos');
  if (nuevaHoja) {
    nuevaHoja.clear();                    // o ss.deleteSheet(nuevaHoja);
  } else {
    nuevaHoja = ss.insertSheet('ProximosPartidos');
  }
  // -----------------------------------

  nuevaHoja.appendRow(['start_time', 'competidor1', 'competidor2', 'round', 'torneo']);

  const formatName = n => {
    const parts = n.split(',');
    return parts.length === 2 ? `${parts[1].trim()} ${parts[0].trim()}` : n;
  };

  partidos.forEach(p => {
    const [c1, c2] = p.competitors.map(formatName);
    nuevaHoja.appendRow([p.start_time, c1, c2, p.round, torneo]);
  });

}
