function fetchProximosPartidosPorTorneo(torneo) {
  var url = 'https://example.com/proximos_partidos_por_torneo';
  var payload = { torneo: torneo };
  var options = {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
  };
  var resp = UrlFetchApp.fetch(url, options);
  var code = resp.getResponseCode();
  if (code !== 200) {
    console.error(resp.getContentText());
    return null;
  }
  var contentType = resp.getHeaders()['Content-Type'] || '';
  if (!contentType.includes('application/json')) {
    console.error('Respuesta no JSON: ' + contentType);
    return null;
  }
  return JSON.parse(resp.getContentText());
}
