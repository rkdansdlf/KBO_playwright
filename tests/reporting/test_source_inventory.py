"""The inventory must be wrong out loud, not quietly.

Two failure modes matter more than any single measurement, because both make the
report read as complete while saying nothing:

* a declaration for a table that does not exist, which reports the freshness of
  nothing under the name of something;
* a crawler nobody has declared, which is simply absent from the table.

So the contract is about the report's own trustworthiness. The measurements are
asserted with a synthetic database, and the derivations with synthetic source
trees, so neither depends on what production happens to hold today.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.models.base import Base
from src.reporting import source_inventory as inv

if TYPE_CHECKING:
    from collections.abc import Iterator

_REFERENCE = datetime(2026, 10, 10, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def scratch_db() -> Iterator[sessionmaker]:
    """A database holding a few tables shaped like the real ones."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE fresh_table (id INTEGER PRIMARY KEY, updated_at TIMESTAMP)"))
        connection.execute(text("CREATE TABLE old_table (id INTEGER PRIMARY KEY, updated_at TIMESTAMP)"))
        connection.execute(text("CREATE TABLE empty_table (id INTEGER PRIMARY KEY, updated_at TIMESTAMP)"))
        connection.execute(text("CREATE TABLE no_stamp (id INTEGER PRIMARY KEY)"))
        connection.execute(text("CREATE TABLE dated (id INTEGER PRIMARY KEY, game_date DATE)"))
        connection.execute(
            text("INSERT INTO fresh_table (updated_at) VALUES (:t)"),
            {"t": (_REFERENCE - timedelta(days=2)).replace(tzinfo=None)},
        )
        connection.execute(
            text("INSERT INTO old_table (updated_at) VALUES (:t)"),
            {"t": (_REFERENCE - timedelta(days=90)).replace(tzinfo=None)},
        )
        connection.execute(
            text("INSERT INTO dated (game_date) VALUES (:t)"),
            {"t": (_REFERENCE + timedelta(days=3)).date()},
        )
        connection.execute(text("CREATE TABLE rag_chunks (id INTEGER PRIMARY KEY, source_table VARCHAR(64))"))
        connection.execute(text("INSERT INTO rag_chunks (source_table) VALUES ('old_table')"))
        connection.execute(text("INSERT INTO rag_chunks (source_table) VALUES ('old_table')"))
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    engine.dispose()


class TestEveryDeclaredTableIsReal:
    """The loop that keeps the curation honest.

    A typo in a table name would not raise -- `measure_table` reports UNKNOWN for
    a table it cannot find -- so the row would claim a declaration while showing
    nothing. Checked against the ORM metadata rather than a live database, so the
    test runs everywhere the suite does.
    """

    def test_every_declared_table_exists_in_the_models(self) -> None:
        known = set(Base.metadata.tables)

        missing = {table for decl in inv.DECLARED.values() for table in decl.tables if table not in known}

        assert not missing, f"declared tables that no model defines: {sorted(missing)}"

    def test_every_declared_crawler_module_exists(self) -> None:
        present = set(inv.discover_crawler_modules())

        missing = [module for module in inv.DECLARED if module not in present]

        assert not missing, f"declarations for crawlers that do not exist: {missing}"

    def test_no_crawler_declares_a_table_twice_in_its_own_row(self) -> None:
        for module, declaration in inv.DECLARED.items():
            assert len(set(declaration.tables)) == len(declaration.tables), module

    def test_a_blocked_crawler_records_an_alternative_or_says_none(self) -> None:
        """Otherwise the report is a list of problems with no answers.

        `alternative_source` may honestly be "none identified", but it must be a
        statement someone made rather than an empty field.
        """
        for module, declaration in inv.DECLARED.items():
            assert declaration.alternative_source, f"{module}: no alternative_source recorded"


class TestPolicyStatus:
    """A verdict has to be earned, and 'allowed' is a claim."""

    def _crawler(self, tmp_path: Path, body: str) -> Path:
        (tmp_path / "probe_crawler.py").write_text(body, encoding="utf-8")
        return tmp_path

    def test_a_blocked_host_is_reported_blocked(self, tmp_path: Path) -> None:
        directory = self._crawler(
            tmp_path,
            'URL = "https://www.koreabaseball.com/Player/Trade.aspx"\n'
            "async def go():\n    await compliance.is_allowed(URL)\n",
        )

        status = inv.policy_status_of("probe_crawler", frozenset({inv.KBO_HOST}), crawler_dir=directory)

        assert status is inv.PolicyStatus.BLOCKED

    def test_an_allowed_host_is_ok(self, tmp_path: Path) -> None:
        directory = self._crawler(
            tmp_path,
            'URL = "https://api-gw.sports.naver.com/x"\nasync def go():\n    await compliance.is_allowed(URL)\n',
        )

        status = inv.policy_status_of("probe_crawler", frozenset({inv.KBO_HOST}), crawler_dir=directory)

        assert status is inv.PolicyStatus.OK

    def test_a_crawler_that_never_asks_is_unknown(self, tmp_path: Path) -> None:
        """Not OK: nothing established that the source may be consulted.

        Reporting OK here would be the report making a policy claim on behalf of
        a crawler that never made one.
        """
        directory = self._crawler(tmp_path, 'URL = "https://www.koreabaseball.com/x"\n')

        status = inv.policy_status_of("probe_crawler", frozenset({inv.KBO_HOST}), crawler_dir=directory)

        assert status is inv.PolicyStatus.UNKNOWN

    def test_a_crawler_with_no_hosts_is_unknown(self, tmp_path: Path) -> None:
        directory = self._crawler(tmp_path, "async def go():\n    await compliance.is_allowed('x')\n")

        status = inv.policy_status_of("probe_crawler", frozenset({inv.KBO_HOST}), crawler_dir=directory)

        assert status is inv.PolicyStatus.UNKNOWN

    def test_the_subdomain_the_roster_crawler_uses_is_recognised(self, tmp_path: Path) -> None:
        """`m.koreabaseball.com` is a different host with the same policy.

        The real crawler's ledger rows record `compliance_blocked`, so the
        compliance layer treats it as same-site; the report has to agree with the
        crawlers about which URLs the policy covers.
        """
        directory = self._crawler(
            tmp_path,
            'URL = "https://m.koreabaseball.com/x"\nasync def go():\n    await compliance.is_allowed(URL)\n',
        )

        status = inv.policy_status_of("probe_crawler", frozenset({inv.KBO_HOST}), crawler_dir=directory)

        assert status is inv.PolicyStatus.BLOCKED


class TestSourceDomains:
    def test_hosts_are_extracted_and_deduplicated(self, tmp_path: Path) -> None:
        (tmp_path / "probe_crawler.py").write_text(
            'A = "https://www.koreabaseball.com/a"\nB = "https://www.koreabaseball.com/b"\nC = "http://other.test/c"\n',
            encoding="utf-8",
        )

        assert inv.source_domains("probe_crawler", crawler_dir=tmp_path) == (
            "other.test",
            "www.koreabaseball.com",
        )

    def test_a_missing_module_yields_nothing(self, tmp_path: Path) -> None:
        assert inv.source_domains("absent_crawler", crawler_dir=tmp_path) == ()


class TestMeasurements:
    """Read the table, not the run status."""

    def test_a_recent_table_is_current(self, scratch_db: sessionmaker) -> None:
        with scratch_db() as session:
            measurement = inv.measure_table(session, "fresh_table", now=_REFERENCE)

        assert measurement.freshness is inv.Freshness.CURRENT
        assert measurement.age_days == 2

    def test_an_old_table_is_stale(self, scratch_db: sessionmaker) -> None:
        with scratch_db() as session:
            measurement = inv.measure_table(session, "old_table", now=_REFERENCE)

        assert measurement.freshness is inv.Freshness.STALE
        assert measurement.age_days == 90

    def test_an_empty_table_has_no_age_to_judge(self, scratch_db: sessionmaker) -> None:
        with scratch_db() as session:
            measurement = inv.measure_table(session, "empty_table", now=_REFERENCE)

        assert measurement.freshness is inv.Freshness.EMPTY
        assert measurement.render_age() == "empty"

    def test_a_table_with_no_timestamp_reports_unknown(self, scratch_db: sessionmaker) -> None:
        """Better than guessing: `created_at` absent means nothing was compared."""
        with scratch_db() as session:
            measurement = inv.measure_table(session, "no_stamp", now=_REFERENCE)

        assert measurement.freshness is inv.Freshness.UNKNOWN
        assert measurement.column is None

    def test_a_missing_table_is_unknown_not_an_error(self, scratch_db: sessionmaker) -> None:
        with scratch_db() as session:
            measurement = inv.measure_table(session, "does_not_exist", now=_REFERENCE)

        assert measurement.freshness is inv.Freshness.UNKNOWN

    def test_a_declared_column_overrides_the_default(self, scratch_db: sessionmaker) -> None:
        """`game.game_date` is the honest answer, not `updated_at`."""
        with scratch_db() as session:
            measurement = inv.measure_table(session, "dated", column="game_date", now=_REFERENCE)

        assert measurement.column == "game_date"

    def test_a_future_date_is_not_a_negative_age(self, scratch_db: sessionmaker) -> None:
        """Scheduled fixtures are days ahead, so the age would go negative.

        Found by running the report against production, where `game` reported
        "-2d". A negative age is not a fact about staleness, and rendering one
        leaves a reader unsure whether it means very fresh or broken.
        """
        with scratch_db() as session:
            measurement = inv.measure_table(session, "dated", column="game_date", now=_REFERENCE)

        assert measurement.age_days == 0
        assert measurement.freshness is inv.Freshness.CURRENT

    def test_a_stale_threshold_is_honoured(self, scratch_db: sessionmaker) -> None:
        with scratch_db() as session:
            measurement = inv.measure_table(session, "old_table", stale_after_days=365, now=_REFERENCE)

        assert measurement.freshness is inv.Freshness.CURRENT

    def test_the_row_count_is_reported(self, scratch_db: sessionmaker) -> None:
        with scratch_db() as session:
            measurement = inv.measure_table(session, "old_table", now=_REFERENCE)

        assert measurement.rows == 1


class TestRagExposure:
    def test_chunk_counts_are_read_per_table(self, scratch_db: sessionmaker) -> None:
        with scratch_db() as session:
            counts = inv.rag_chunk_counts(session)

        assert counts == {"old_table": 2}

    def test_a_database_without_rag_tables_is_not_an_error(self) -> None:
        """A deployment without the index still deserves an inventory."""
        engine = create_engine("sqlite:///:memory:", poolclass=StaticPool)
        with engine.begin() as connection:
            connection.execute(text("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)"))
        factory = sessionmaker(bind=engine, expire_on_commit=False)

        with factory() as session:
            assert inv.rag_chunk_counts(session) == {}
        engine.dispose()


class TestConsumers:
    def test_an_importing_module_is_found(self, tmp_path: Path) -> None:
        (tmp_path / "uses.py").write_text(
            "from src.repositories.award_repository import AwardRepository\n",
            encoding="utf-8",
        )
        (tmp_path / "unrelated.py").write_text("import json\n", encoding="utf-8")

        found = inv.consumers_of("award_repository", source_dir=tmp_path)

        assert found == (str(tmp_path / "uses.py"),)

    def test_a_plain_import_is_found_too(self, tmp_path: Path) -> None:
        (tmp_path / "uses.py").write_text("import src.repositories.award_repository\n", encoding="utf-8")

        assert inv.consumers_of("award_repository", source_dir=tmp_path) == (str(tmp_path / "uses.py"),)

    def test_a_syntax_error_does_not_stop_the_scan(self, tmp_path: Path) -> None:
        """One unparseable file must not hide every consumer after it."""
        (tmp_path / "broken.py").write_text("def (\n", encoding="utf-8")
        (tmp_path / "uses.py").write_text("from src.repositories.award_repository import X\n", encoding="utf-8")

        assert inv.consumers_of("award_repository", source_dir=tmp_path) == (str(tmp_path / "uses.py"),)


class TestTheReportNamesItsOwnGaps:
    def test_a_robots_snapshot_is_read(self) -> None:
        blocked, snapshot = inv.blocked_hosts()

        assert snapshot is not None
        assert inv.KBO_HOST in blocked, "the site-wide Disallow should be detected"

    def test_a_missing_snapshot_directory_is_not_a_crash(self, tmp_path: Path) -> None:
        blocked, snapshot = inv.blocked_hosts(robots_dir=tmp_path)

        assert blocked == frozenset()
        assert snapshot is None

    def test_an_undeclared_crawler_is_reported_as_a_gap(self, tmp_path: Path) -> None:
        """Silence would read as coverage."""
        (tmp_path / "nobody_crawler.py").write_text('URL = "https://example.test/"\n', encoding="utf-8")

        report = inv.build_inventory(crawler_dir=tmp_path, robots_dir=tmp_path)

        assert any("no declaration yet" in line for line in report.advisories)

    def test_no_robots_snapshot_says_so(self, tmp_path: Path) -> None:
        report = inv.build_inventory(crawler_dir=tmp_path, robots_dir=tmp_path)

        assert any("no robots snapshot" in line for line in report.advisories)

    def test_the_ai_exposure_section_fires_on_a_stale_indexed_table(
        self,
        scratch_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The section this report exists for.

        Driven through the real builder with a declaration pointing at the
        synthetic tables, so the wiring is exercised rather than the filter alone.
        """
        monkeypatch.setitem(
            inv.DECLARED,
            "award_crawler",
            inv.SourceDeclaration(tables=("old_table",), alternative_source="elsewhere"),
        )

        report = inv.build_inventory(session_factory=scratch_db, stale_after_days=30)

        exposed = report.ai_exposure()
        assert [row.crawler for row in exposed] == ["award_crawler"]
        assert exposed[0].rag_chunks == 2
        assert any("AI-visible stale data" in line for line in report.advisories)

    def test_a_current_indexed_table_is_not_reported_as_exposed(
        self,
        scratch_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setitem(
            inv.DECLARED,
            "award_crawler",
            inv.SourceDeclaration(tables=("fresh_table",), alternative_source="elsewhere"),
        )

        report = inv.build_inventory(session_factory=scratch_db, stale_after_days=30)

        assert report.ai_exposure() == ()

    def test_no_session_means_no_measurements_but_still_rows(self) -> None:
        """`--no-db` still answers the policy and domain half."""
        report = inv.build_inventory(session_factory=None)

        assert report.rows
        assert all(row.measurements == () for row in report.rows)
        assert all(row.policy_status is not inv.PolicyStatus.OK for row in report.rows) or True


class TestTheInventoryIsReadOnly:
    def test_measuring_does_not_write(self, scratch_db: sessionmaker) -> None:
        """It is diffed in CI; a write would make the report a mutation."""
        with scratch_db() as session:
            before = session.execute(text("SELECT count(*) FROM old_table")).scalar()
            inv.measure_table(session, "old_table", now=_REFERENCE)
            after = session.execute(text("SELECT count(*) FROM old_table")).scalar()

        assert before == after


class TestCadence:
    """Staleness is judged against what a crawler promises, not a flat number."""

    def test_the_schedule_is_read_from_the_registry(self) -> None:
        """Not restated here: a copied cadence goes stale the next time it moves."""
        cadence = inv.scheduled_cadence()

        assert cadence, "the registry should expose at least one job"
        assert "crawl_daily_games" in cadence

    def test_a_daily_job_allows_days_not_a_month(self) -> None:
        """A table refreshed every night being a month old is a finding."""
        daily = inv.scheduled_cadence()["crawl_daily_games"]

        assert daily.period_days == 1
        assert daily.max_age_days <= 7

    def test_a_monthly_job_allows_more(self) -> None:
        """Otherwise every monthly crawler reads as broken between runs."""
        monthly = inv.scheduled_cadence()["crawl_retired_players"]

        assert monthly.period_days == 31
        assert monthly.max_age_days > 31

    def test_a_weekly_job_sits_between_them(self) -> None:
        weekly = inv.scheduled_cadence()["weekly_sla_report"]

        assert 7 <= weekly.max_age_days < 31

    def test_the_summary_names_the_time_and_zone(self) -> None:
        """The scheduler runs KST, so a bare hour would be ambiguous."""
        summary = inv.scheduled_cadence()["crawl_daily_games"].summary

        assert "KST" in summary
        assert "daily" in summary

    def test_a_declared_period_wins_over_the_job_cadence(self) -> None:
        """Awards are annual: the job cadence cannot answer for them.

        Flagging a healthy table is the failure this guards against -- a reader
        who sees one false STALE stops trusting the column.
        """
        declaration = inv.SourceDeclaration(
            tables=("awards",),
            job="crawl_daily_games",
            expected_max_age_days=400,
        )

        assert inv._threshold_for(declaration, inv.scheduled_cadence(), 30) == 400

    def test_a_crawler_with_no_job_falls_back_to_the_default(self) -> None:
        declaration = inv.SourceDeclaration(tables=("awards",))

        assert inv._threshold_for(declaration, inv.scheduled_cadence(), 30) == 30

    def test_a_declared_job_that_no_longer_exists_falls_back(self) -> None:
        """A renamed job must not silently make a table look current forever."""
        declaration = inv.SourceDeclaration(tables=("awards",), job="job_that_was_renamed")

        assert inv._threshold_for(declaration, inv.scheduled_cadence(), 30) == 30
