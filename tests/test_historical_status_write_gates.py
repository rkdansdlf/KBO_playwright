"""Gates that stop a date-ordered sweep from rewriting historical game_status.

The 2026-10-12 incident had three independent causes, and each is pinned here by
a test that fails if the corresponding guard is removed:

1. ``_apply_detail_failure_fallback`` protected only ``CANCELLED`` and
   ``POSTPONED``. A sweep over 1995~2000 therefore turned 2,848 rows the
   answer-set apply had written as ``COMPLETED`` into ``UNRESOLVED_MISSING``,
   because nothing about a failed detail fetch says a finished game was
   un-finished.
2. Nothing restricted *which* dates the refresh path may write. A crawl that
   cannot find a boxscore used to answer by rewriting the status that recorded
   the fact that it could not.
3. The backfill gap query treated "no published body" as "needs backfill", so
   the loop never converged: each pass removed the row from the query rather
   than filling it, and reported progress while recovering nothing.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config.historical_backfill_policy import (
    REASON_RUN_CAP_EXCEEDED,
    REASON_SEASON_NOT_APPROVED,
    HistoricalBackfillPolicy,
    load_historical_backfill_policy,
)
from src.models.audit_trail import CorrectionAuditTrail
from src.models.game import (
    Game,
    GameBattingStat,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
)
from src.repositories.game_status import (
    StatusRefreshOptions,
    refresh_game_status_for_date,
)
from src.utils.game_status import (
    GAME_STATUS_CANCELLED,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_SCHEDULED,
    GAME_STATUS_UNRESOLVED,
)

#: The date the incident's first sweep ran, in the 1995~2000 window.
HISTORICAL_DATE = "19950624"


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
        CorrectionAuditTrail.__table__,
    ):
        table.create(engine)
    return sessionmaker(bind=engine)


def _historical_game(session, *, game_status: str = GAME_STATUS_COMPLETED) -> Game:
    """Add a 1995 game with a score, metadata, and no bodies -- the incident shape."""
    game = Game(
        game_id="19950624OBLG0",
        game_date=date(1995, 6, 24),
        away_team="OB",
        home_team="LG",
        away_score=4,
        home_score=1,
        game_status=game_status,
    )
    session.add(game)
    session.add(GameMetadata(game_id="19950624OBLG0", stadium_code="JAMSIL"))
    session.commit()
    return game


class TestPolicyRefusesOldSeasonsByDefault:
    def test_a_season_outside_the_grace_window_is_refused(self) -> None:
        policy = HistoricalBackfillPolicy()
        allowed, reason = policy.status_write_allowed(date(1995, 6, 24), date(2026, 10, 14))

        assert allowed is False
        assert reason is not None
        assert reason.startswith(REASON_SEASON_NOT_APPROVED)

    def test_the_previous_season_is_still_writable(self) -> None:
        """The 03:00 KST finalize finalizes yesterday, which can be last season's."""
        policy = HistoricalBackfillPolicy(grace_seasons=1)
        allowed, _ = policy.status_write_allowed(date(2025, 12, 31), date(2026, 1, 1))

        assert allowed is True

    def test_today_and_future_are_always_writable(self) -> None:
        policy = HistoricalBackfillPolicy()
        assert policy.status_write_allowed(date(2026, 10, 14), date(2026, 10, 14))[0] is True
        assert policy.status_write_allowed(date(2026, 10, 15), date(2026, 10, 14))[0] is True

    def test_an_approved_date_is_writable(self) -> None:
        policy = HistoricalBackfillPolicy(approved_dates=frozenset({"19950624"}))

        assert policy.status_write_allowed(date(1995, 6, 24), date(2026, 10, 14))[0] is True

    def test_approval_is_per_date_not_per_season(self) -> None:
        """Approving one date must not open the whole 1995 season."""
        policy = HistoricalBackfillPolicy(approved_dates=frozenset({"19950624"}))

        assert policy.status_write_allowed(date(1995, 6, 25), date(2026, 10, 14))[0] is False


class TestRunCap:
    def test_more_approved_dates_than_the_cap_are_cut(self) -> None:
        policy = HistoricalBackfillPolicy(
            approved_dates=frozenset(f"199506{day:02d}" for day in range(1, 31)), max_dates_per_run=10
        )

        allowed, reason = policy.approve_for_run(sorted(policy.approved_dates))

        assert len(allowed) == 10
        assert reason is not None
        assert reason.startswith(REASON_RUN_CAP_EXCEEDED)

    def test_within_the_cap_nothing_is_dropped(self) -> None:
        policy = HistoricalBackfillPolicy(approved_dates=frozenset({"19950624"}), max_dates_per_run=10)

        allowed, reason = policy.approve_for_run(sorted(policy.approved_dates))

        assert allowed == ["19950624"]
        assert reason is None


class TestPolicyLoading:
    def test_a_missing_file_falls_back_to_protective_defaults(self, tmp_path) -> None:
        policy = load_historical_backfill_policy(tmp_path / "absent.yaml")

        assert policy.grace_seasons == 1
        assert policy.approved_dates == frozenset()
        assert policy.max_dates_per_run == 10

    def test_a_malformed_file_does_not_silently_widen_the_scope(self, tmp_path) -> None:
        path = tmp_path / "broken.yaml"
        path.write_text("approved_dates: {oops: not-a-list}\n", encoding="utf-8")

        policy = load_historical_backfill_policy(path)

        assert policy.approved_dates == frozenset()

    def test_values_are_read_from_the_file(self, tmp_path) -> None:
        path = tmp_path / "policy.yaml"
        path.write_text(
            "current_season_grace_seasons: 2\napproved_dates: ['1995-06-24']\nmax_dates_per_run: 3\n",
            encoding="utf-8",
        )

        policy = load_historical_backfill_policy(path)

        assert policy.grace_seasons == 2
        assert policy.approved_dates == frozenset({"19950624"})
        assert policy.max_dates_per_run == 3


class TestRefreshRefusesToRewriteHistory:
    def test_the_stored_status_survives_and_the_transition_is_only_reported(
        self,
        session_factory: sessionmaker,
    ) -> None:
        with session_factory() as session:
            game = _historical_game(session)
            result = refresh_game_status_for_date(HISTORICAL_DATE, today=date(2026, 10, 14), session=session)

            assert result["skipped"] is True
            assert result["updated"] == 0
            assert result["updated_game_ids"] == []
            assert result["transition_counts"], "a refused write must still report what it would have done"
            session.refresh(game)
            assert game.game_status == GAME_STATUS_COMPLETED

    def test_a_refused_refresh_writes_no_audit_row(self, session_factory: sessionmaker) -> None:
        with session_factory() as session:
            _historical_game(session)
            result = refresh_game_status_for_date(HISTORICAL_DATE, today=date(2026, 10, 14), session=session)

            assert result["audited"] == 0
            assert session.query(CorrectionAuditTrail).count() == 0

    def test_an_approved_date_writes_and_records(self, session_factory: sessionmaker) -> None:
        policy = HistoricalBackfillPolicy(approved_dates=frozenset({"19950624"}))
        with session_factory() as session:
            game = _historical_game(session)
            result = refresh_game_status_for_date(
                HISTORICAL_DATE,
                today=date(2026, 10, 14),
                session=session,
                options=StatusRefreshOptions(caller="test", policy=policy),
            )

            assert result["skipped"] is False
            assert result["audited"] == len(result["transitions"])
            session.refresh(game)
            assert game.game_status == GAME_STATUS_CANCELLED

    def test_a_refusal_does_not_raise(self, session_factory: sessionmaker) -> None:
        """A past-date run must not take the review and story batches down with it."""
        with session_factory() as session:
            _historical_game(session)
            result = refresh_game_status_for_date(HISTORICAL_DATE, today=date(2026, 10, 14), session=session)

        assert result["skip_reason"] is not None


class TestAuditCarriesTheReason:
    def test_each_written_change_leaves_one_row_naming_the_caller(self, session_factory: sessionmaker) -> None:
        policy = HistoricalBackfillPolicy(approved_dates=frozenset({"19950624"}))
        with session_factory() as session:
            _historical_game(session)
            refresh_game_status_for_date(
                HISTORICAL_DATE,
                today=date(2026, 10, 14),
                session=session,
                options=StatusRefreshOptions(caller="daily_review_batch", policy=policy),
            )
            rows = session.query(CorrectionAuditTrail).all()

            assert len(rows) == 1
            row = rows[0]
            assert row.field_name == "game_status"
            assert row.raw_value == GAME_STATUS_COMPLETED
            assert row.corrected_value == GAME_STATUS_CANCELLED
            assert row.corrected_source == "refresh_game_status"
            assert row.correction_reason == f"refresh:{HISTORICAL_DATE}"
            # The evidence is what lets a reader disagree with the change.
            assert row.extra_metadata["has_metadata"] is True
            assert row.extra_metadata["caller"] == "daily_review_batch"

    def test_dry_run_reports_without_writing_or_auditing(self, session_factory: sessionmaker) -> None:
        with session_factory() as session:
            game = _historical_game(session)
            result = refresh_game_status_for_date(
                HISTORICAL_DATE,
                today=date(2026, 10, 14),
                session=session,
                options=StatusRefreshOptions(dry_run=True),
            )

            assert result["dry_run"] is True
            assert result["updated"] == 0
            assert result["audited"] == 0
            assert result["transitions"], "a dry run exists to preview the transitions"
            assert session.query(CorrectionAuditTrail).count() == 0
            session.refresh(game)
            assert game.game_status == GAME_STATUS_COMPLETED


class TestTheGapQueryCannotChaseUnpublishedBodies:
    def test_the_query_floors_the_season(self) -> None:
        from src.scheduler.jobs.daily import SOURCE_LIMITED_SEASON_FLOOR, _find_detail_gaps

        session = MagicMock()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        session.execute.return_value = result

        _find_detail_gaps(session, date(1990, 1, 1))

        statement = str(session.execute.call_args[0][0])
        assert "g.game_date >= :season_floor" in statement
        assert session.execute.call_args[0][1]["season_floor"] == date(SOURCE_LIMITED_SEASON_FLOOR, 1, 1)

    def test_the_floor_is_the_first_season_with_published_bodies(self) -> None:
        from src.scheduler.jobs.daily import SOURCE_LIMITED_SEASON_FLOOR

        assert SOURCE_LIMITED_SEASON_FLOOR == 2001


class TestBackfillScope:
    def test_the_window_starts_at_the_season_floor(self, monkeypatch) -> None:
        """A lookback wide enough to reach 1995 must still stop at the grace window."""
        from src.scheduler.jobs import daily as daily_jobs

        captured: dict[str, object] = {}

        def fake_phase_detail(start, backfilled, *, extra_dates=None):
            captured["start"] = start
            captured["extra"] = list(extra_dates or [])
            return []

        monkeypatch.setattr(daily_jobs, "_backfill_phase_detail", fake_phase_detail)
        monkeypatch.setattr(daily_jobs, "_backfill_phase_pbp", lambda *a, **k: None)
        monkeypatch.setattr(daily_jobs, "_backfill_phase_preview", lambda *a, **k: None)
        monkeypatch.setattr(daily_jobs, "_backfill_phase_profiles", lambda *a, **k: None)
        monkeypatch.setattr(daily_jobs, "_can_run_job", lambda _name: (True, ""))
        monkeypatch.setattr(daily_jobs, "load_historical_backfill_policy", HistoricalBackfillPolicy)

        daily_jobs.backfill_missed_daily_crawls(lookback_days=16_000)

        assert captured["start"] >= date(daily_jobs.datetime.now(daily_jobs.KST).year - 1, 1, 1)
        assert captured["extra"] == []

    def test_approved_dates_reach_the_detail_phase(self, monkeypatch) -> None:
        from src.scheduler.jobs import daily as daily_jobs

        captured: dict[str, object] = {}

        def fake_phase_detail(start, backfilled, *, extra_dates=None):
            captured["extra"] = list(extra_dates or [])
            return []

        monkeypatch.setattr(daily_jobs, "_backfill_phase_detail", fake_phase_detail)
        monkeypatch.setattr(daily_jobs, "_backfill_phase_pbp", lambda *a, **k: None)
        monkeypatch.setattr(daily_jobs, "_backfill_phase_preview", lambda *a, **k: None)
        monkeypatch.setattr(daily_jobs, "_backfill_phase_profiles", lambda *a, **k: None)
        monkeypatch.setattr(daily_jobs, "_can_run_job", lambda _name: (True, ""))
        policy = HistoricalBackfillPolicy(
            approved_dates=frozenset({"19950624", "19950625"}),
            max_dates_per_run=10,
        )
        monkeypatch.setattr(daily_jobs, "load_historical_backfill_policy", lambda: policy)

        daily_jobs.backfill_missed_daily_crawls(lookback_days=14)

        assert captured["extra"] == [date(1995, 6, 24), date(1995, 6, 25)]

    def test_the_per_run_cap_holds_over_the_phase(self, monkeypatch) -> None:
        from src.scheduler.jobs import daily as daily_jobs

        captured: dict[str, object] = {}

        monkeypatch.setattr(
            daily_jobs,
            "_backfill_phase_detail",
            lambda start, backfilled, *, extra_dates=None: captured.setdefault("extra", list(extra_dates or [])) or [],
        )
        monkeypatch.setattr(daily_jobs, "_backfill_phase_pbp", lambda *a, **k: None)
        monkeypatch.setattr(daily_jobs, "_backfill_phase_preview", lambda *a, **k: None)
        monkeypatch.setattr(daily_jobs, "_backfill_phase_profiles", lambda *a, **k: None)
        monkeypatch.setattr(daily_jobs, "_can_run_job", lambda _name: (True, ""))
        policy = HistoricalBackfillPolicy(
            approved_dates=frozenset(f"199506{day:02d}" for day in range(1, 21)),
            max_dates_per_run=5,
        )
        monkeypatch.setattr(daily_jobs, "load_historical_backfill_policy", lambda: policy)

        daily_jobs.backfill_missed_daily_crawls(lookback_days=14)

        assert len(captured["extra"]) == 5


class TestManifestRecordsWhatChanged:
    def test_the_aggregate_is_written_into_the_manifest(self, tmp_path) -> None:
        from src.utils.refresh_manifest import RefreshManifestSpec, write_refresh_manifest

        path = write_refresh_manifest(
            RefreshManifestSpec(
                phase="postgame_finalize",
                target_date=HISTORICAL_DATE,
                game_ids=["19950624OBLG0"],
                datasets=["game"],
                output_dir=tmp_path,
                status_transitions={
                    "transition_counts": {f"{GAME_STATUS_COMPLETED}->{GAME_STATUS_CANCELLED}": 1},
                    "transition_total": 1,
                    "written": 0,
                    "audited": 0,
                    "write_allowed": False,
                    "skip_reason": "historical_season_not_approved:19950624",
                    "dry_run": False,
                },
            ),
        )

        import json

        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["status_transitions"]["transition_total"] == 1
        assert payload["status_transitions"]["write_allowed"] is False

    def test_a_manifest_without_transitions_omits_the_key(self, tmp_path) -> None:
        from src.utils.refresh_manifest import RefreshManifestSpec, write_refresh_manifest

        path = write_refresh_manifest(
            RefreshManifestSpec(
                phase="postgame_finalize",
                target_date=HISTORICAL_DATE,
                game_ids=[],
                datasets=["game"],
                output_dir=tmp_path,
            ),
        )

        import json

        assert "status_transitions" not in json.loads(path.read_text(encoding="utf-8"))


class TestAFailedDetailFetchNoLongerUnFinishesAGame:
    @pytest.fixture
    def ctx(self):
        from src.cli.pipelines.run_daily_update import _RunContext

        return _RunContext(
            target_date=HISTORICAL_DATE,
            year=1995,
            month=6,
            today_kst=date(2026, 10, 14),
            runner=MagicMock(),
            write_contract=MagicMock(),
        )

    @pytest.mark.parametrize(
        "stored",
        [GAME_STATUS_COMPLETED, GAME_STATUS_CANCELLED, "DRAW"],
    )
    def test_a_terminal_status_is_preserved(self, ctx, monkeypatch, stored: str) -> None:
        from src.cli.pipelines import run_daily_update as rdu

        session = MagicMock()
        session.query.return_value.filter.return_value.one_or_none.return_value = MagicMock(game_status=stored)
        monkeypatch.setattr(rdu, "SessionLocal", lambda: _session_ctx(session))
        writes: list[tuple[str, str]] = []
        monkeypatch.setattr(
            rdu,
            "update_game_status",
            lambda gid, status: writes.append((gid, status)),
        )

        rdu._apply_detail_failure_fallback(ctx, "19950624OBLG0", "not_found")

        assert writes == [], "a failed fetch must not rewrite a status that already resolved"

    def test_a_drawn_game_is_preserved_too(self, ctx, monkeypatch) -> None:
        from src.cli.pipelines import run_daily_update as rdu

        session = MagicMock()
        session.query.return_value.filter.return_value.one_or_none.return_value = MagicMock(game_status="DRAW")
        monkeypatch.setattr(rdu, "SessionLocal", lambda: _session_ctx(session))
        writes: list[tuple[str, str]] = []
        monkeypatch.setattr(rdu, "update_game_status", lambda gid, status: writes.append((gid, status)))

        rdu._apply_detail_failure_fallback(ctx, "19950624OBLG0", "not_found")

        assert writes == []

    def test_an_unresolved_still_game_is_still_marked(self, ctx, monkeypatch) -> None:
        from src.cli.pipelines import run_daily_update as rdu

        session = MagicMock()
        session.query.return_value.filter.return_value.one_or_none.return_value = MagicMock(
            game_status=GAME_STATUS_SCHEDULED,
        )
        monkeypatch.setattr(rdu, "SessionLocal", lambda: _session_ctx(session))
        writes: list[tuple[str, str]] = []
        monkeypatch.setattr(rdu, "update_game_status", lambda gid, status: writes.append((gid, status)))

        rdu._apply_detail_failure_fallback(ctx, "19950624OBLG0", "not_found")

        assert writes == [("19950624OBLG0", GAME_STATUS_UNRESOLVED)]


def _session_ctx(session):
    from contextlib import contextmanager

    @contextmanager
    def _ctx():
        yield session

    return _ctx()
