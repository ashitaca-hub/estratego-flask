import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import services.supabase_fs as fs
import pytest


def test_get_matchup_hist_vector_missing_data(monkeypatch):
    monkeypatch.setattr(fs, "_winrate_month", lambda *args, **kwargs: None)
    monkeypatch.setattr(fs, "_winrate_surface", lambda *args, **kwargs: None)
    monkeypatch.setattr(fs, "_winrate_speed", lambda *args, **kwargs: None)
    with pytest.raises(ValueError):
        fs.get_matchup_hist_vector(1, 2, 3, "", 5)


def test_get_matchup_hist_vector_rpc_missing_data(monkeypatch):
    def fake_rpc(name, payload):
        assert name == "get_matchup_hist_vector"
        return {
            "surface": "hard",
            "speed_bucket": "Medium",
            "d_hist_month": None,
            "d_hist_surface": 0.1,
            "d_hist_speed": -0.2,
        }

    monkeypatch.setattr(fs, "_rpc", fake_rpc)
    with pytest.raises(ValueError):
        fs.get_matchup_hist_vector(1, 2, 3, "", 5)


def test_get_matchup_hist_vector_returns_deltas(monkeypatch):
    monkeypatch.setattr(fs, "_winrate_month", lambda pid, m, y: 0.6 if pid == 1 else 0.4)
    monkeypatch.setattr(fs, "_winrate_surface", lambda pid, s, y: 0.8 if pid == 1 else 0.2)
    monkeypatch.setattr(fs, "_winrate_speed", lambda pid, s, y: 0.5 if pid == 1 else 0.7)
    res = fs.get_matchup_hist_vector(1, 2, 3, "", 5)
    assert res["d_hist_month"] == pytest.approx(0.2)
    assert res["d_hist_surface"] == pytest.approx(0.6)
    assert res["d_hist_speed"] == pytest.approx(-0.2)
