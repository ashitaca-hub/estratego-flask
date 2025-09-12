import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import main
import services.supabase_fs as fs


def test_get_defense_prev_year_by_sr_returns_mapping():
    class DummyCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

        def execute(self, sql, params):
            # Ignore SQL; test focuses on return shape
            pass

        def fetchall(self):
            return [
                (123, 2000, "champ"),
                (456, 1200, "runner"),
            ]

    class DummyConn:
        def cursor(self):
            return DummyCursor()

    res = fs.get_defense_prev_year_by_sr("Roland Garros", [123, 456], conn=DummyConn())
    assert res == {
        123: {"points": 2000, "title_code": "champ"},
        456: {"points": 1200, "title_code": "runner"},
    }


def test_enrich_resp_with_extras_uses_int_sr_and_sets_extras(monkeypatch):
    def fake_get_player_meta(pid, conn=None):
        return {"ext_sportradar_id": f"sr:competitor:{225050 if pid == 1 else 407573}"}

    captured = {}

    def fake_get_defense_prev_year_by_sr(tname, ids, conn=None):
        captured["tname"] = tname
        captured["ids"] = ids
        return {
            225050: {"points": 720, "title_code": "champ"},
            407573: {"points": 600, "title_code": "runner"},
        }

    monkeypatch.setattr(main.FS, "get_player_meta", fake_get_player_meta)
    monkeypatch.setattr(main.FS, "get_defense_prev_year_by_sr", fake_get_defense_prev_year_by_sr)
    monkeypatch.setattr(main.FS, "get_tourney_country", lambda name: None)

    resp = {
        "inputs": {
            "player_id": 1,
            "opponent_id": 2,
            "tournament": {"name": "Roland Garros"},
        }
    }

    out = main.enrich_resp_with_extras(resp)

    assert captured["ids"] == [225050, 407573]
    extras = out["extras"]
    assert extras["def_points_p"] == 720
    assert extras["def_title_p"] == "champ"
    assert extras["def_points_o"] == 600
    assert extras["def_title_o"] == "runner"
