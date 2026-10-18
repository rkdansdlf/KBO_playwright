"""The source-limited status report must report the sweep signature, not average it.

1982~2000 have scores and no bodies. A date-ordered sweep on 2026-09-23 rewrote
2,848 of those rows, and the boundary it left -- ``CANCELLED`` up to 1995-06-23,
``UNRESOLVED_MISSING`` from 1995-06-24 -- is invisible to any per-row check. Each
row is individually well-formed.

So the report's job is to say what the era looks like and to refuse to propose a
repair, because the snapshot the rows are compared against disagrees with the
live rows about what they used to say.
"""

from __future__ import annotations

import json
from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.cli.reports import report_source_limited_status as report
from src.models.game import (
    Game,
    GameBattingStat,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
)


@pytest.fixture
def session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game.__table__,
        GameMetadata.__table__,
        GameInningScore.__table__,
        GameLineup.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
    ):
        table.create(engine)
    return sessionmaker(bind=engine)


def _add(session, game_id: str, game_date: date, status: str) -> None:
    """Add a scored game with metadata and no bodies -- the source-limited shape."""
    session.add(
        Game(
            game_id=game_id,
            game_date=game_date,
            away_team="OB",
            home_team="LG",
            away_score=4,
            home_score=1,
            game_status=status,
        ),
    )
    session.add(GameMetadata(game_id=game_id, stadium_code="JAMSIL"))


class TestItReportsTheEraAsItIs:
    def test_the_sweep_boundary_is_visible(self, session_factory) -> None:
        with session_factory() as session:
            _add(session, "19950623OBLG0", date(1995, 6, 23), "CANCELLED")
            _add(session, "19950624OBLG0", date(1995, 6, 24), "UNRESOLVED_MISSING")
            session.commit()
            payload = report.build_report(session)

            assert payload["status_counts"] == {"UNRESOLVED_MISSING": 1, "CANCELLED": 1}
            assert payload["asymmetry"]["observed"] is True

    def test_a_within_season_date_cut_is_caught(self, session_factory) -> None:
        """Both rows sit in one season, so a per-season comparison alone would miss it.

        This is the shape the 2026-09-23 sweep left: 1995-06-23 reads
        ``CANCELLED`` and 1995-06-24 reads ``UNRESOLVED_MISSING``, both in 1995.
        """
        with session_factory() as session:
            _add(session, "19950623OBLG0", date(1995, 6, 23), "CANCELLED")
            _add(session, "19950624OBLG0", date(1995, 6, 24), "UNRESOLVED_MISSING")
            session.commit()
            asymmetry = report.build_report(session)["asymmetry"]

            assert asymmetry["across_seasons"] is False
            assert len(asymmetry["within_season_boundaries"]) == 1
            boundary = asymmetry["within_season_boundaries"][0]
            assert boundary["season"] == 1995
            assert boundary["statuses_in_date_order"] == ["CANCELLED", "UNRESOLVED_MISSING"]

    def test_several_dates_in_the_same_status_are_one_run(self, session_factory) -> None:
        with session_factory() as session:
            for day in (20, 21, 22, 23):
                _add(session, f"199506{day}OBLG0", date(1995, 6, day), "CANCELLED")
            _add(session, "19950624OBLG0", date(1995, 6, 24), "UNRESOLVED_MISSING")
            session.commit()
            boundary = report.build_report(session)["asymmetry"]["within_season_boundaries"][0]

            assert boundary["segments"] == [
                {"status": "CANCELLED", "rows": 4, "from": "1995-06-20"},
                {"status": "UNRESOLVED_MISSING", "rows": 1, "from": "1995-06-24"},
            ]

    def test_both_ends_of_the_boundary_are_in_the_era(self, session_factory) -> None:
        with session_factory() as session:
            _add(session, "19950623OBLG0", date(1995, 6, 23), "CANCELLED")
            _add(session, "19950624OBLG0", date(1995, 6, 24), "UNRESOLVED_MISSING")
            session.commit()
            seasons = report.build_report(session)["by_season"]

            assert [row["season"] for row in seasons] == [1995]

    def test_a_season_with_published_bodies_is_out_of_scope(self, session_factory) -> None:
        """2001+ bodies were collected, so its rows are not this report's business."""
        with session_factory() as session:
            _add(session, "20010410OBLG0", date(2001, 4, 10), "COMPLETED")
            session.commit()
            payload = report.build_report(session)

            assert payload["status_counts"] == {}
            assert payload["body_rows"]["games"] == 0


class TestItRefusesToProposeARepair:
    def test_no_automatic_repair_is_offered(self, session_factory) -> None:
        with session_factory() as session:
            _add(session, "19950624OBLG0", date(1995, 6, 24), "UNRESOLVED_MISSING")
            session.commit()
            payload = report.build_report(session)

            assert payload["automatic_repair"] is False
            assert payload["manual_reason"] == report.MANUAL_REASON

    def test_the_unconfirmed_baseline_is_stated(self, session_factory) -> None:
        """Restoring to an unconfirmed 'before' is a guess dressed as a repair."""
        with session_factory() as session:
            _add(session, "19950624OBLG0", date(1995, 6, 24), "UNRESOLVED_MISSING")
            session.commit()
            payload = report.build_report(session)

            assert payload["baseline_status"] == report.BASELINE_UNCONFIRMED
            assert "dump" in payload["baseline_status"]


class TestItShowsTheEvidenceForSourceLimitation:
    def test_body_counts_are_reported_not_asserted(self, session_factory) -> None:
        with session_factory() as session:
            _add(session, "19950624OBLG0", date(1995, 6, 24), "UNRESOLVED_MISSING")
            session.commit()
            bodies = report.build_report(session)["body_rows"]

            assert bodies["games"] == 1
            assert bodies["game_metadata"] == 1
            assert bodies["game_inning_scores"] == 0
            assert bodies["game_lineups"] == 0
            assert bodies["game_batting_stats"] == 0
            assert bodies["game_pitching_stats"] == 0


class TestTheReportWritesNothing:
    def test_a_full_report_leaves_the_database_unchanged(self, session_factory) -> None:
        with session_factory() as session:
            _add(session, "19950624OBLG0", date(1995, 6, 24), "UNRESOLVED_MISSING")
            session.commit()
            report.build_report(session)
            session.rollback()

            row = session.query(Game).one()
            assert row.game_status == "UNRESOLVED_MISSING"


class TestRendering:
    def test_the_asymmetry_is_visible_in_the_text(self, session_factory) -> None:
        with session_factory() as session:
            _add(session, "19950623OBLG0", date(1995, 6, 23), "CANCELLED")
            _add(session, "19950624OBLG0", date(1995, 6, 24), "UNRESOLVED_MISSING")
            session.commit()
            text = report.render(report.build_report(session))

            assert "ASYMMETRY" in text
            assert "sweep signature" in text
            assert "Automatic repair: disabled" in text

    def test_a_uniform_era_does_not_claim_a_sweep(self, session_factory) -> None:
        with session_factory() as session:
            _add(session, "19950623OBLG0", date(1995, 6, 23), "CANCELLED")
            _add(session, "19950624OBLG0", date(1995, 6, 24), "CANCELLED")
            session.commit()
            text = report.render(report.build_report(session))

            assert "ASYMMETRY" not in text
            assert "No asymmetry detected" in text


class TestJsonOutput:
    def test_the_payload_serializes(self, session_factory) -> None:
        with session_factory() as session:
            _add(session, "19950624OBLG0", date(1995, 6, 24), "UNRESOLVED_MISSING")
            session.commit()
            payload = json.loads(json.dumps(report.build_report(session)))

            assert payload["era"] == "1982-2000"
            assert payload["read_only"] is True
            assert payload["automatic_repair"] is False

    def test_the_cli_renders_without_a_database_flag(self, session_factory, monkeypatch, capsys) -> None:
        """The CLI must be runnable against the configured database."""
        monkeypatch.setattr(report, "SessionLocal", session_factory, raising=False)
        import src.db.engine as engine_module

        monkeypatch.setattr(engine_module, "SessionLocal", session_factory)
        with session_factory() as session:
            _add(session, "19950624OBLG0", date(1995, 6, 24), "UNRESOLVED_MISSING")
            session.commit()
            assert report.main(["--json"]) == 0

            payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
            assert payload["era"] == "1982-2000"
