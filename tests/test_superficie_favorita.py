import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import main


class MockResp:
    def __init__(self, data):
        self.status_code = 200
        self._data = data
        self.headers = {}

    def json(self):
        return self._data

    def raise_for_status(self):
        pass


def test_calcular_superficie_favorita(monkeypatch):
    sample = {
        "periods": [
            {
                "surfaces": [
                    {"type": "clay", "statistics": {"matches_won": 3, "matches_played": 5}},
                    {"type": "hard", "statistics": {"matches_won": 2, "matches_played": 4}},
                    {"type": "grass", "statistics": {"matches_won": 1, "matches_played": 1}},
                ]
            }
        ]
    }

    def mock_get(url, timeout=None, headers=None):
        return MockResp(sample)

    monkeypatch.setattr(main.requests, "get", mock_get)

    superficie, porcentaje = main.calcular_superficie_favorita("player")
    assert superficie == "grass"
    assert porcentaje == 100.0


def test_calcular_superficie_favorita_zero(monkeypatch):
    sample = {
        "periods": [
            {
                "surfaces": [
                    {"type": "clay", "statistics": {"matches_won": 0, "matches_played": 0}},
                    {"type": "hard", "statistics": {"matches_won": 1, "matches_played": 2}},
                ]
            }
        ]
    }

    def mock_get(url, timeout=None, headers=None):
        return MockResp(sample)

    monkeypatch.setattr(main.requests, "get", mock_get)

    superficie, porcentaje = main.calcular_superficie_favorita("player")
    assert superficie == "hard"
    assert porcentaje == 50.0
