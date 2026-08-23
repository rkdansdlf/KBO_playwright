"""Unit tests for src.testing.synthetic_generator."""

from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.models.base import Base
from src.testing.dto import SyntheticSeasonConfig
from src.testing.synthetic_generator import SyntheticKBOGenerator


def test_generate_players() -> None:
    gen = SyntheticKBOGenerator(seed=123)
    players = gen.generate_players(["LG", "OB"], count_per_team=10)

    assert len(players) == 20
    assert players[0].team == "LG"
    assert players[10].team == "OB"
    assert any(p.position == "투수" for p in players)
    assert any(p.position in {"내야수", "외야수"} for p in players)


def test_generate_game_satisfies_invariants() -> None:
    gen = SyntheticKBOGenerator(seed=456)
    game_graph = gen.generate_game(
        game_date=date(2026, 4, 1),
        home_team="LG",
        away_team="OB",
        game_idx=0,
    )

    game = game_graph["game"]
    innings = game_graph["innings"]
    batting = game_graph["batting_stats"]

    # Invariant 1: Inning runs sum to final scores
    away_runs = sum(inn.runs for inn in innings if inn.team_code == "OB")
    home_runs = sum(inn.runs for inn in innings if inn.team_code == "LG")
    assert away_runs == game.away_score
    assert home_runs == game.home_score

    # Invariant 2: PA = AB + BB + HBP + SH + SF
    for b in batting:
        pa_expected = b.at_bats + b.walks + b.hbp + b.sacrifice_hits + b.sacrifice_flies
        assert b.plate_appearances == pa_expected
        # Invariant 3: H = 1B + 2B + 3B + HR
        assert b.hits >= (b.doubles + b.triples + b.home_runs)


def test_generate_season_and_seed_database() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    gen = SyntheticKBOGenerator(seed=789)
    config = SyntheticSeasonConfig(
        season_year=2026,
        team_codes=["LG", "OB", "SSG", "KT"],
        games_per_team=2,
        players_per_team=10,
    )

    dataset = gen.generate_season(config)
    assert len(dataset["players"]) == 40
    assert len(dataset["games"]) == 4

    with Session(engine) as session:
        result = gen.seed_to_database(session, dataset)
        session.commit()

        assert result.total_games == 4
        assert result.total_players == 40
        assert result.total_lineups == 80
        assert result.total_pbp_events > 0
