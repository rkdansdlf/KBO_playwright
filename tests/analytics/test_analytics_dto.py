"""Unit tests for src.analytics.dto."""

from __future__ import annotations

from src.analytics.dto import (
    BattingSabermetrics,
    LeagueConstants,
    MatchupMatrix,
    PitchingSabermetrics,
    SplitMetrics,
)


def test_league_constants_to_dict() -> None:
    consts = LeagueConstants(
        year=2025,
        level="KBO1",
        woba_scale=1.24,
        league_woba=0.335,
        league_era=4.15,
    )
    d = consts.to_dict()
    assert d["year"] == 2025
    assert d["level"] == "KBO1"
    assert d["league_woba"] == 0.335
    assert d["league_era"] == 4.15


def test_batting_sabermetrics_to_dict() -> None:
    bat = BattingSabermetrics(
        player_id=78224,
        season=2025,
        plate_appearances=500,
        at_bats=420,
        hits=130,
        woba=0.395,
        wraa=22.5,
        wrc_plus=138.4,
        war=4.85,
    )
    d = bat.to_dict()
    assert d["player_id"] == 78224
    assert d["season"] == 2025
    assert d["woba"] == 0.395
    assert d["wrc_plus"] == 138.4
    assert d["war"] == 4.85


def test_pitching_sabermetrics_to_dict() -> None:
    pit = PitchingSabermetrics(
        player_id=61234,
        season=2025,
        innings_pitched=165.1,
        earned_runs=55,
        era=3.0,
        fip=3.25,
        war=4.2,
    )
    d = pit.to_dict()
    assert d["player_id"] == 61234
    assert d["season"] == 2025
    assert d["era"] == 3.0
    assert d["fip"] == 3.25
    assert d["war"] == 4.2


def test_matchup_matrix_to_dict() -> None:
    m = MatchupMatrix(
        batter_id=78224,
        pitcher_id=61234,
        plate_appearances=12,
        at_bats=10,
        hits=4,
        doubles=1,
        home_runs=1,
        walks=2,
        strikeouts=2,
        avg=0.400,
        obp=0.500,
        slg=0.800,
        ops=1.300,
    )
    d = m.to_dict()
    assert d["batter_id"] == 78224
    assert d["pitcher_id"] == 61234
    assert d["avg"] == 0.400
    assert d["ops"] == 1.300


def test_split_metrics_to_dict() -> None:
    split = SplitMetrics(
        category="risp",
        entity_id=78224,
        season=2025,
        split_key="RISP",
        sample_size=110,
        stats={"ab": 90, "h": 32, "avg": 0.356, "rbi": 45},
    )
    d = split.to_dict()
    assert d["category"] == "risp"
    assert d["entity_id"] == 78224
    assert d["sample_size"] == 110
    assert d["stats"]["avg"] == 0.356
