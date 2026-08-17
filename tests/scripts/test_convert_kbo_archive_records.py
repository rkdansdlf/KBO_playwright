"""Regression tests for the historical KBO archive generator."""

from __future__ import annotations

from collections import Counter, defaultdict

from scripts.converters.convert_kbo_archive_records import (
    GAMES_PER_TEAM,
    generate_season_dataset,
    get_teams_for_season,
)


def test_generate_1982_dataset_preserves_first_game_and_inning_totals() -> None:
    """Generate the expected 1982 schedule and reconcile every inning total."""
    data = generate_season_dataset(1982)
    games = data["games"]

    assert len(games) == 240
    assert len(data["game_inning_scores"]) == len(games) * 18
    assert len(data["game_batting_stats"]) == len(games) * 2
    assert len(data["game_pitching_stats"]) == len(games) * 2
    assert len({game["game_id"] for game in games}) == len(games)
    assert games[0]["away_score"] == 7
    assert games[0]["home_score"] == 11

    inning_totals = defaultdict(int)
    for record in data["game_inning_scores"]:
        inning_totals[(record["game_id"], record["team_side"])] += record["runs"]

    for game in games:
        assert inning_totals[(game["game_id"], "away")] == game["away_score"]
        assert inning_totals[(game["game_id"], "home")] == game["home_score"]


def test_generate_2000_dataset_preserves_team_schedule_counts() -> None:
    """Generate the 2000 schedule with the configured games per team."""
    data = generate_season_dataset(2000)
    teams = get_teams_for_season(2000)
    team_counts = Counter()

    for game in data["games"]:
        team_counts[game["away_team"]] += 1
        team_counts[game["home_team"]] += 1

    assert len(data["games"]) == len(teams) * GAMES_PER_TEAM[2000] // 2
    assert team_counts == dict.fromkeys(teams, GAMES_PER_TEAM[2000])
    assert len({game["game_id"] for game in data["games"]}) == len(data["games"])
