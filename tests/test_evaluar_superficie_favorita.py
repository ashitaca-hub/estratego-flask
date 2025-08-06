import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import main


class MockResp:
    def __init__(self, data):
        self.status_code = 200
        self._data = data

    def json(self):
        return self._data


def test_evaluar_includes_superficie_favorita(monkeypatch):
    def mock_get(url, headers=None):
        return MockResp({})

    monkeypatch.setattr(main.requests, "get", mock_get)
    monkeypatch.setattr(
        main,
        "obtener_estadisticas_jugador",
        lambda pid: {
            "ranking": 1,
            "victorias_totales": 0,
            "partidos_totales": 0,
            "porcentaje_victorias": 0,
            "victorias_en_superficie": 0,
            "partidos_en_superficie": 0,
            "porcentaje_superficie": 0,
        },
    )
    monkeypatch.setattr(
        main,
        "calcular_superficie_favorita",
        lambda pid: ("clay", 70.0),
    )
    monkeypatch.setattr(
        main,
        "obtener_ultimos5_winnerid",
        lambda pid, data: (3, []),
    )
    monkeypatch.setattr(
        main,
        "evaluar_torneo_favorito",
        lambda pid, data: ("✔", "Roland Garros"),
    )
    monkeypatch.setattr(main, "obtener_h2h_extend", lambda j, r: "1 - 0")
    monkeypatch.setattr(
        main,
        "evaluar_actividad_reciente",
        lambda pid, data: ("✔", 10),
    )
    monkeypatch.setattr(
        main,
        "obtener_puntos_defendidos",
        lambda pid: (0, "", "✘", "", "", None),
    )
    monkeypatch.setattr(
        main,
        "viene_de_cambio_de_superficie",
        lambda pid, sup: False,
    )

    client = main.app.test_client()
    resp = client.post("/", json={"jugador": "j1", "rival": "j2"})
    data = resp.get_json()

    assert resp.status_code == 200
    assert data["superficie_favorita"] == "clay"
    assert data["porcentaje_superficie_favorita"] == 70.0

