"""Unit tests for recalc_player_stats pure calculation functions."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.cli.recalc_player_stats import (
    _build_batting_payloads,
    _build_pitching_payloads,
    _compute_batting_rates,
    _compute_pitching_rates,
)


def _batting_row(
    player_id: int = 1001,
    games: int | None = 10,
    plate_appearances: int | None = 458,
    at_bats: int = 400,
    runs: int | None = 70,
    hits: int = 120,
    doubles: int = 20,
    triples: int = 5,
    home_runs: int = 15,
    rbi: int | None = 80,
    walks: int = 50,
    intentional_walks: int | None = 2,
    hbp: int = 5,
    sacrifice_flies: int = 3,
    sacrifice_hits: int | None = 1,
    strikeouts: int = 100,
    stolen_bases: int | None = 10,
    caught_stealing: int | None = 3,
    gdp: int | None = 6,
) -> object:
    return SimpleNamespace(
        player_id=player_id,
        games=games,
        plate_appearances=plate_appearances,
        at_bats=at_bats,
        runs=runs,
        hits=hits,
        doubles=doubles,
        triples=triples,
        home_runs=home_runs,
        rbi=rbi,
        walks=walks,
        intentional_walks=intentional_walks,
        hbp=hbp,
        sacrifice_flies=sacrifice_flies,
        sacrifice_hits=sacrifice_hits,
        strikeouts=strikeouts,
        stolen_bases=stolen_bases,
        caught_stealing=caught_stealing,
        gdp=gdp,
    )


def _pitching_row(
    player_id: int = 2001,
    games: int | None = 20,
    games_started: int | None = 10,
    innings_outs: int | None = 180,
    hits_allowed: int | None = 50,
    runs_allowed: int | None = 25,
    earned_runs: int | None = 20,
    home_runs_allowed: int | None = 5,
    walks_allowed: int | None = 15,
    strikeouts: int | None = 70,
    hit_batters: int | None = 2,
    wild_pitches: int | None = 1,
    balks: int | None = 0,
    wins: int | None = 8,
    losses: int | None = 3,
    saves: int | None = 1,
    holds: int | None = 4,
    batters_faced: int | None = 250,
    pitches: int | None = 900,
) -> object:
    return SimpleNamespace(
        player_id=player_id,
        games=games,
        games_started=games_started,
        innings_outs=innings_outs,
        hits_allowed=hits_allowed,
        runs_allowed=runs_allowed,
        earned_runs=earned_runs,
        home_runs_allowed=home_runs_allowed,
        walks_allowed=walks_allowed,
        strikeouts=strikeouts,
        hit_batters=hit_batters,
        wild_pitches=wild_pitches,
        balks=balks,
        wins=wins,
        losses=losses,
        saves=saves,
        holds=holds,
        batters_faced=batters_faced,
        pitches=pitches,
    )


class TestComputeBattingRates:
    def test_normal_row(self) -> None:
        row = _batting_row()
        result = _compute_batting_rates(row)
        assert result["avg"] == pytest.approx(0.300, abs=0.001)
        assert result["obp"] == pytest.approx(0.382, abs=0.001)
        assert result["slg"] == pytest.approx(0.487, abs=0.001)
        assert result["ops"] == pytest.approx(0.869, abs=0.001)

    def test_zero_ab_avoids_division(self) -> None:
        row = _batting_row(at_bats=0)
        result = _compute_batting_rates(row)
        assert result["avg"] == 0.0
        assert result["slg"] == 0.0

    def test_zero_sf_adds_one(self) -> None:
        row = _batting_row(sacrifice_flies=0)
        result = _compute_batting_rates(row)
        assert result["slg"] > 0

    def test_high_values(self) -> None:
        row = _batting_row(at_bats=600, hits=200, walks=100, home_runs=40)
        result = _compute_batting_rates(row)
        assert result["avg"] == pytest.approx(0.333, abs=0.001)
        assert result["ops"] > 0.9

    def test_all_zero_row(self) -> None:
        row = _batting_row(at_bats=0, hits=0, walks=0, home_runs=0, doubles=0, triples=0, strikeouts=0)
        result = _compute_batting_rates(row)
        assert result["avg"] == 0
        assert result["slg"] == 0

    def test_missing_none_ab(self) -> None:
        row = SimpleNamespace(
            at_bats=None,
            hits=None,
            doubles=None,
            triples=None,
            home_runs=None,
            walks=None,
            hbp=None,
            sacrifice_flies=None,
            strikeouts=None,
        )
        result = _compute_batting_rates(row)
        assert result["avg"] == 0
        assert result["slg"] == 0

    def test_iso_diff(self) -> None:
        row = _batting_row()
        result = _compute_batting_rates(row)
        assert result["iso"] == pytest.approx(result["slg"] - result["avg"], abs=0.001)

    def test_babip(self) -> None:
        row = _batting_row(at_bats=400, hits=120, home_runs=15, strikeouts=100, sacrifice_flies=3)
        result = _compute_batting_rates(row)
        expected = (120 - 15) / (400 - 100 - 15 + 3)
        assert result["babip"] == pytest.approx(expected, abs=0.001)


class TestComputePitchingRates:
    def test_normal(self) -> None:
        result = _compute_pitching_rates(540, 150, 40, 45, 160)
        assert result["innings_pitched"] == 180.0
        assert result["era"] == pytest.approx(2.25, abs=0.01)
        assert result["whip"] == pytest.approx(1.06, abs=0.01)
        assert result["k_per_nine"] == pytest.approx(8.0, abs=0.01)

    def test_zero_outs(self) -> None:
        result = _compute_pitching_rates(0, 0, 0, 0, 0)
        assert result["era"] == 0
        assert result["whip"] == 0

    def test_high_so_low_bb(self) -> None:
        result = _compute_pitching_rates(540, 100, 5, 30, 200)
        assert result["kbb"] == pytest.approx(40.0, abs=0.01)
        assert result["k_per_nine"] == pytest.approx(10.0, abs=0.01)

    def test_no_walks_avoids_division(self) -> None:
        result = _compute_pitching_rates(540, 150, 0, 45, 160)
        assert result["kbb"] == 0
        assert result["bb_per_nine"] == 0

    def test_low_era_high_k(self) -> None:
        result = _compute_pitching_rates(720, 100, 20, 25, 250)
        assert result["era"] < 3.0
        assert result["k_per_nine"] > 9.0


class TestBuildBattingPayloads:
    def test_builds_payload_with_team_and_rates(self) -> None:
        payload = _build_batting_payloads([_batting_row()], 2025, "REGULAR", "KBO1", {1001: "LG"})[0]

        assert payload["player_id"] == 1001
        assert payload["season"] == 2025
        assert payload["league"] == "REGULAR"
        assert payload["level"] == "KBO1"
        assert payload["source"] == "AGGREGATED"
        assert payload["canonical_team_code"] == "LG"
        assert payload["plate_appearances"] == 458
        assert payload["avg"] == pytest.approx(0.300, abs=0.001)
        assert payload["ops"] == pytest.approx(0.869, abs=0.001)

    def test_missing_team_maps_to_none(self) -> None:
        payload = _build_batting_payloads([_batting_row(player_id=9999)], 2025, "REGULAR", "KBO1", {})[0]
        assert payload["canonical_team_code"] is None

    def test_none_aggregates_become_zero(self) -> None:
        row = _batting_row(
            games=None,
            plate_appearances=None,
            at_bats=None,
            runs=None,
            hits=None,
            doubles=None,
            triples=None,
            home_runs=None,
            rbi=None,
            walks=None,
            intentional_walks=None,
            hbp=None,
            sacrifice_flies=None,
            sacrifice_hits=None,
            strikeouts=None,
            stolen_bases=None,
            caught_stealing=None,
            gdp=None,
        )
        payload = _build_batting_payloads([row], 2025, "REGULAR", "KBO1", {1001: "LG"})[0]

        for key in (
            "games",
            "plate_appearances",
            "at_bats",
            "runs",
            "hits",
            "doubles",
            "triples",
            "home_runs",
            "rbi",
            "walks",
            "intentional_walks",
            "hbp",
            "strikeouts",
            "stolen_bases",
            "caught_stealing",
            "sacrifice_hits",
            "sacrifice_flies",
            "gdp",
        ):
            assert payload[key] == 0

    def test_empty_rows_returns_empty_list(self) -> None:
        assert _build_batting_payloads([], 2025, "REGULAR", "KBO1", {}) == []


class TestBuildPitchingPayloads:
    def test_builds_payload_with_team_and_rates(self) -> None:
        payload = _build_pitching_payloads([_pitching_row()], 2025, "REGULAR", "KBO1", {2001: "SSG"})[0]

        assert payload["player_id"] == 2001
        assert payload["canonical_team_code"] == "SSG"
        assert payload["innings_outs"] == 180
        assert payload["innings_pitched"] == 60.0
        assert payload["era"] == pytest.approx(3.0, abs=0.01)
        assert payload["tbf"] == 250
        assert payload["np"] == 900

    def test_zero_outs_keeps_rates_zero(self) -> None:
        payload = _build_pitching_payloads(
            [_pitching_row(innings_outs=0, hits_allowed=10, earned_runs=5, walks_allowed=2, strikeouts=8)],
            2025,
            "REGULAR",
            "KBO1",
            {2001: "SSG"},
        )[0]

        assert payload["innings_pitched"] == 0.0
        assert payload["era"] == 0.0
        assert payload["whip"] == 0.0

    def test_missing_team_maps_to_none(self) -> None:
        payload = _build_pitching_payloads([_pitching_row(player_id=9999)], 2025, "REGULAR", "KBO1", {})[0]
        assert payload["canonical_team_code"] is None

    def test_none_aggregates_become_zero(self) -> None:
        row = _pitching_row(
            games=None,
            games_started=None,
            innings_outs=None,
            hits_allowed=None,
            runs_allowed=None,
            earned_runs=None,
            home_runs_allowed=None,
            walks_allowed=None,
            strikeouts=None,
            hit_batters=None,
            wild_pitches=None,
            balks=None,
            wins=None,
            losses=None,
            saves=None,
            holds=None,
            batters_faced=None,
            pitches=None,
        )
        payload = _build_pitching_payloads([row], 2025, "REGULAR", "KBO1", {2001: "SSG"})[0]

        for key in (
            "games",
            "games_started",
            "innings_outs",
            "hits_allowed",
            "runs_allowed",
            "earned_runs",
            "home_runs_allowed",
            "walks_allowed",
            "strikeouts",
            "hit_batters",
            "wild_pitches",
            "balks",
            "wins",
            "losses",
            "saves",
            "holds",
            "tbf",
            "np",
        ):
            assert payload[key] == 0

    def test_empty_rows_returns_empty_list(self) -> None:
        assert _build_pitching_payloads([], 2025, "REGULAR", "KBO1", {}) == []
