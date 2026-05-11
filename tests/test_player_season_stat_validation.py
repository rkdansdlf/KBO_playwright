from src.utils.player_season_stat_validation import (
    filter_valid_season_stat_payloads,
    validate_season_stat_payload,
)


def _batting_row(**overrides):
    row = {
        "player_id": 1001,
        "player_name": "홍길동",
        "season": 2025,
        "league": "REGULAR",
        "team_code": "LG",
        "games": 10,
        "hits": 5,
    }
    row.update(overrides)
    return row


def _pitching_row(**overrides):
    row = {
        "player_id": 2001,
        "player_name": "원태인",
        "season": 2025,
        "league": "REGULAR",
        "team_code": "SS",
        "games": 10,
        "innings_outs": 90,
    }
    row.update(overrides)
    return row


def _fielding_row(**overrides):
    row = {
        "player_id": 3001,
        "player_name": "오지환",
        "year": 2025,
        "team_id": "LG",
        "position_id": "SS",
        "games": 10,
        "errors": 1,
    }
    row.update(overrides)
    return row


def test_validate_season_stat_rejects_missing_identity_and_core_fields():
    cases = [
        (_batting_row(player_id=None), "invalid_player_id"),
        (_batting_row(player_name=""), "missing_player_name"),
        (_batting_row(player_name="Unknown Player"), "unknown_player_name"),
        (_batting_row(season=None), "missing_season"),
        (_batting_row(team_code=""), "missing_team_code"),
        (_batting_row(games=None, hits=None), "empty_core_stats"),
    ]

    for payload, reason in cases:
        assert validate_season_stat_payload(payload, stat_type="batting") == (False, reason)


def test_validate_season_stat_rejects_invalid_numeric_values():
    assert validate_season_stat_payload(
        _pitching_row(innings_outs="not-a-number"),
        stat_type="pitching",
    ) == (False, "invalid_numeric_stat")


def test_filter_valid_season_stat_payloads_normalizes_valid_rows_and_counts_failures():
    rows, reasons = filter_valid_season_stat_payloads(
        [
            _batting_row(player_id="1001", season="2025", player_name=" 홍길동 "),
            _batting_row(player_id="bad"),
            _batting_row(games=None, hits=None),
        ],
        stat_type="batting",
    )

    assert rows[0]["player_id"] == 1001
    assert rows[0]["season"] == 2025
    assert rows[0]["player_name"] == "홍길동"
    assert dict(reasons) == {"invalid_player_id": 1, "empty_core_stats": 1}


def test_validate_fielding_payload_requires_year_team_position_and_core_stats():
    cases = [
        (_fielding_row(player_id=None), "invalid_player_id"),
        (_fielding_row(year=None), "missing_year"),
        (_fielding_row(team_id=""), "missing_team_id"),
        (_fielding_row(position_id=""), "missing_position_id"),
        (_fielding_row(games=None, errors=None), "empty_core_stats"),
    ]

    for payload, reason in cases:
        assert validate_season_stat_payload(payload, stat_type="fielding") == (False, reason)


def test_filter_fielding_payload_allows_fallback_rows_without_player_name():
    rows, reasons = filter_valid_season_stat_payloads(
        [
            {
                "player_id": "3001",
                "year": "2025",
                "team_id": "LG",
                "position_id": "SS",
                "games": 10,
            }
        ],
        stat_type="fielding",
    )

    assert rows[0]["player_id"] == 3001
    assert rows[0]["year"] == 2025
    assert dict(reasons) == {}
