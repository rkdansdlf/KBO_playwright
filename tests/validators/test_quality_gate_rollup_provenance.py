"""Tests that the quality gate reports when a team total was never cross-checkable.

A team season aggregate has two possible provenances, and the gate's value depends
on which one it is comparing:

* ``kbo_team_page`` -- parsed from the official team page. The player sums are an
  independent read of the same season, so a difference means one side is wrong.
* ``player_rollup`` -- summed from the player rows because the page was blocked by
  robots.txt. Comparing that number against the very rows it was summed from is a
  tautology: it can only ever agree, and when it disagrees the cause is the
  aggregation rather than either source.

Reporting "ok" for the second case without saying so reads as a passed source
cross-check. It passed, but nothing was compared. These tests pin that the result
distinguishes the two instead of collapsing them into one green.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.validators.quality_gate import (
    PITCHING_TEAM_STAT_FIELDS,
    QualityGate,
)


def _mock_result(rows: list[object]) -> MagicMock:
    """Return an execute() result carrying the given rows.

    ``scalars`` is populated too because the season-id lookup reads through it, and
    a bare MagicMock there yields an empty list that makes the gate return early.
    """
    result = MagicMock()
    result.all.return_value = rows
    result.scalars.return_value.all.return_value = (
        [int(row) for row in rows] if rows and isinstance(rows[0], int) else rows
    )
    return result


def _team_row(team_id: str, *, values: dict[str, object], extra_stats: dict[str, object] | None) -> MagicMock:
    """Build a team season row whose fields all carry the given values."""
    row = MagicMock()
    row.team_id = team_id
    row.innings_pitched = values.get("innings_pitched", 0.0)
    row.innings_outs = values.get("innings_outs")
    for field in PITCHING_TEAM_STAT_FIELDS:
        setattr(row, field, values.get(field, 100))
    row.extra_stats = extra_stats
    return row


def _player_row(team_id: str, *, values: dict[str, object]) -> MagicMock:
    """Build a per-team player-sum row."""
    row = MagicMock()
    row.team_code = team_id
    row.innings_outs = values.get("innings_outs")
    for field in PITCHING_TEAM_STAT_FIELDS:
        setattr(row, field, values.get(field, 100))
    return row


def _run_gate(team_row: MagicMock, player_row: MagicMock) -> dict[str, object]:
    """Run the team pitching gate against one team row and one player-sum row."""
    session = MagicMock()
    session.execute.side_effect = [
        _mock_result([1]),
        _mock_result([team_row]),
        _mock_result([player_row]),
    ]
    return QualityGate(session).validate_season_team_pitching(2026)


class TestTheTeamProvenanceIsReported:
    """The result must say which provenance produced the team total."""

    def test_a_page_scraped_total_is_reported_as_cross_checked(self) -> None:
        team = _team_row(
            "DB",
            values={"innings_pitched": 300.0, "innings_outs": 900},
            extra_stats={"source": "kbo_team_page", "source_url": "https://example.invalid/record"},
        )
        player = _player_row("DB", values={"innings_outs": 900})

        result = _run_gate(team, player)

        assert result["ok"] is True
        assert result["team_source"] == "KBO_TEAM_PAGE"
        assert result["cross_checkable"] is True

    def test_an_aggregated_total_is_reported_as_not_cross_checkable(self) -> None:
        team = _team_row(
            "DB",
            values={"innings_pitched": 300.0, "innings_outs": 900},
            extra_stats={"source": "player_rollup"},
        )
        player = _player_row("DB", values={"innings_outs": 900})

        result = _run_gate(team, player)

        assert result["team_source"] == "PLAYER_ROLLUP"
        assert result["cross_checkable"] is False

    def test_a_missing_provenance_marker_is_not_treated_as_cross_checkable(self) -> None:
        """An unmarked row has unknown provenance, so it must not claim a cross-check."""
        team = _team_row(
            "DB",
            values={"innings_pitched": 300.0, "innings_outs": 900},
            extra_stats=None,
        )
        player = _player_row("DB", values={"innings_outs": 900})

        result = _run_gate(team, player)

        assert result["cross_checkable"] is False


class TestAnAggregatedTotalCannotMasqueradeAsACrossCheck:
    """A real difference must stay visible even when the total was aggregated."""

    def test_a_difference_against_an_aggregated_total_is_still_reported(self) -> None:
        team = _team_row(
            "DB",
            values={"innings_pitched": 300.0, "innings_outs": 900, "hits_allowed": 400},
            extra_stats={"source": "player_rollup"},
        )
        player = _player_row("DB", values={"innings_outs": 900, "hits_allowed": 100})

        result = _run_gate(team, player)

        assert result["ok"] is False
        assert any("hits_allowed" in diff for m in result["mismatches"] for diff in m["diffs"])
        # The failure is reported, and so is the reason a passing run could not mean much.
        assert result["cross_checkable"] is False


class TestEverySourceTheCrawlersWriteIsRanked:
    """A source the crawlers write but the gate does not rank is a silent exclusion.

    ``FALLBACK_AUTO`` is what both player crawlers stamp on a season line built from
    game rows when the source page was blocked. Leaving it out of the priority tuple
    makes the gate's source ranking fall through to an ``else_`` branch, so the
    excluded row is neither preferred nor rejected -- the gate simply reads whatever
    row sorts next, which for a player whose team code drifted is a different team's
    total. ``audit_fallback_stats`` already treats these four as official values, so
    the ranking has to agree.
    """

    @pytest.mark.parametrize("marker", ["FALLBACK_AUTO", "FALLBACK", "AUDIT_FIX"])
    def test_the_crawler_written_marker_is_ranked(self, marker: str) -> None:
        from src.validators.quality_gate import PLAYER_SEASON_SOURCE_PRIORITY

        assert marker in PLAYER_SEASON_SOURCE_PRIORITY

    def test_a_ranked_marker_beats_an_unranked_one(self) -> None:
        from src.validators.quality_gate import PLAYER_SEASON_SOURCE_PRIORITY

        assert PLAYER_SEASON_SOURCE_PRIORITY.index("FALLBACK_AUTO") < PLAYER_SEASON_SOURCE_PRIORITY.index("ROLLUP")


class TestTheProvenanceHelperIsReusable:
    """The provenance read is a pure function so the batting gate can share it."""

    def test_it_reads_the_marker_from_extra_stats(self) -> None:
        from src.validators.quality_gate import _team_row_source

        assert _team_row_source(SimpleNamespace(extra_stats={"source": "player_rollup"})) == "PLAYER_ROLLUP"
        assert _team_row_source(SimpleNamespace(extra_stats=None)) is None

    def test_it_falls_back_to_a_column_when_extra_stats_is_absent(self) -> None:
        from src.validators.quality_gate import _team_row_source

        assert _team_row_source(SimpleNamespace(source="kbo_team_page")) == "KBO_TEAM_PAGE"
