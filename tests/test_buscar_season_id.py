import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import main


class MockResp:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data

    def raise_for_status(self):
        pass

def test_buscar_season_id_por_nombre_con_ano(monkeypatch):
    sample = {
        "seasons": [
            {"id": "sr:season:1", "name": "ATP Toronto, Canada Men Singles 2024", "year": 2024},
            {"id": "sr:season:2", "name": "ATP Toronto, Canada Men Singles 2025", "year": 2025},
        ]
    }

    def mock_get(url, headers, timeout):
        return MockResp(sample)

    monkeypatch.setattr(main.requests, "get", mock_get)

    assert main.buscar_season_id_por_nombre("toronto 2024") == "sr:season:1"


def test_buscar_season_id_por_nombre_mas_reciente(monkeypatch):
    sample = {
        "seasons": [
            {"id": "sr:season:1", "name": "ATP Toronto, Canada Men Singles 2023", "year": 2023},
            {"id": "sr:season:2", "name": "ATP Toronto, Canada Men Singles 2024", "year": 2024},
            {"id": "sr:season:3", "name": "ATP Toronto, Canada Men Singles 2025", "year": 2025},
        ]
    }

    def mock_get(url, headers, timeout):
        return MockResp(sample)

    monkeypatch.setattr(main.requests, "get", mock_get)

    assert main.buscar_season_id_por_nombre("toronto") == "sr:season:3"


def test_buscar_season_id_por_nombre_not_found(monkeypatch):
    sample = {
        "seasons": [
            {"id": "sr:season:1", "name": "ATP Toronto, Canada Men Singles 2025", "year": 2025}
        ]
    }

    def mock_get(url, headers, timeout):
        return MockResp(sample)

    monkeypatch.setattr(main.requests, "get", mock_get)

    assert main.buscar_season_id_por_nombre("madrid") is None
