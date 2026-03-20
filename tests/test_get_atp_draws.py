from apps_script.get_atp_draws import parse_line


def test_parse_line_keeps_three_digit_positions():
    parsed = parse_line("100 FUCSOVICS, Marton HUN")

    assert parsed == [
        {
            "pos": 100,
            "player_name": "FUCSOVICS, Marton",
            "seed": None,
            "tag": None,
            "country": "HUN",
        }
    ]


def test_parse_line_keeps_bottom_half_seeded_player():
    parsed = parse_line("128 2 SINNER, Jannik ITA")

    assert parsed == [
        {
            "pos": 128,
            "player_name": "SINNER, Jannik",
            "seed": 2,
            "tag": None,
            "country": "ITA",
        }
    ]
