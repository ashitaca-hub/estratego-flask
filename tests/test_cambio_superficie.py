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


def test_viene_de_cambio_de_superficie_true(monkeypatch):
    sample = {
        "summaries": [
            {
                "sport_event": {
                    "sport_event_context": {"surface": {"name": "Clay"}}
                }
            }
        ]
    }

    def mock_get(url, headers=None, timeout=None):
        return MockResp(sample)

    monkeypatch.setattr(main.requests, "get", mock_get)

    assert main.viene_de_cambio_de_superficie("player", "hard") is True


def test_viene_de_cambio_de_superficie_false(monkeypatch):
    sample = {
        "summaries": [
            {
                "sport_event": {
                    "sport_event_context": {"surface": {"name": "Clay"}}
                }
            }
        ]
    }

    def mock_get(url, headers=None, timeout=None):
        return MockResp(sample)

    monkeypatch.setattr(main.requests, "get", mock_get)

    assert main.viene_de_cambio_de_superficie("player", "Clay") is False

