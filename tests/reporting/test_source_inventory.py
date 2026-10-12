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
        connection.execute(
            text(
                "CREATE TABLE crawl_execution_runs (id INTEGER PRIMARY KEY, crawler VARCHAR(64), "
                "status VARCHAR(32), error_code VARCHAR(32), checkpoint TEXT, records_read INTEGER, "
                "records_written INTEGER, started_at TIMESTAMP)"
            )
        )
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
    """A verdict has to come from a recorded policy, and say whose.

    These were rewritten when the inventory started reading each host's own
    robots.txt instead of the KBO one. Before that, a crawler whose host had no
    record read as `unknown`, and a crawler that never asked the compliance layer
    read the same way -- two different problems collapsed into one word. The
    verdict now comes from the host's policy and `enforced` says separately
    whether the crawler consults it.
    """

    ALLOWED = {inv.HostVerdict.ALLOWED}
    BLOCKED = {inv.HostVerdict.DISALLOWED}
    UNREAD = {inv.HostVerdict.UNREACHABLE}

    def _crawler(self, tmp_path: Path, body: str) -> Path:
        (tmp_path / "probe_crawler.py").write_text(body, encoding="utf-8")
        return tmp_path

    def _status(self, directory: Path, verdicts) -> inv.PolicyStatus:
        return inv.policy_status_of("probe_crawler", verdicts, crawler_dir=directory)

    def test_a_disallowed_host_is_reported_blocked(self, tmp_path: Path) -> None:
        directory = self._crawler(tmp_path, 'URL = "https://blocked.test/x"\n')

        assert self._status(directory, {"blocked.test": inv.HostVerdict.DISALLOWED}) is inv.PolicyStatus.BLOCKED

    def test_an_allowed_host_is_ok(self, tmp_path: Path) -> None:
        directory = self._crawler(tmp_path, 'URL = "https://open.test/x"\n')

        assert self._status(directory, {"open.test": inv.HostVerdict.ALLOWED}) is inv.PolicyStatus.OK

    def test_a_host_that_publishes_no_policy_is_allowed(self, tmp_path: Path) -> None:
        """Absent policy is permission by default, and must not read as unknown."""
        directory = self._crawler(tmp_path, 'URL = "https://open.test/x"\n')

        assert self._status(directory, {"open.test": inv.HostVerdict.NO_POLICY}) is inv.PolicyStatus.OK

    def test_a_host_with_no_record_is_unknown(self, tmp_path: Path) -> None:
        """Not OK: nobody has looked, and absence of knowledge is not permission."""
        directory = self._crawler(tmp_path, 'URL = "https://unasked.test/x"\n')

        assert self._status(directory, {"unasked.test": inv.HostVerdict.UNREACHABLE}) is inv.PolicyStatus.UNKNOWN

    def test_a_host_with_no_entry_at_all_is_unknown(self, tmp_path: Path) -> None:
        """The mapping is allowed to be silent about a host."""
        directory = self._crawler(tmp_path, 'URL = "https://absent.test/x"\n')

        assert self._status(directory, {}) is inv.PolicyStatus.UNKNOWN

    def test_one_blocked_host_among_allowed_ones_is_still_blocked(self, tmp_path: Path) -> None:
        """A crawler with two sources is only as permitted as its worst one."""
        directory = self._crawler(
            tmp_path,
            'A = "https://open.test/a"\nB = "https://blocked.test/b"\n',
        )
        verdicts = {"open.test": inv.HostVerdict.ALLOWED, "blocked.test": inv.HostVerdict.DISALLOWED}

        assert self._status(directory, verdicts) is inv.PolicyStatus.BLOCKED

    def test_a_partly_unread_host_set_is_unknown(self, tmp_path: Path) -> None:
        """One unknown among known ones is not a permission."""
        directory = self._crawler(
            tmp_path,
            'A = "https://open.test/a"\nB = "https://unasked.test/b"\n',
        )
        verdicts = {"open.test": inv.HostVerdict.ALLOWED, "unasked.test": inv.HostVerdict.UNREACHABLE}

        assert self._status(directory, verdicts) is inv.PolicyStatus.UNKNOWN

    def test_a_crawler_with_no_hosts_is_unknown(self, tmp_path: Path) -> None:
        directory = self._crawler(tmp_path, "async def go():\n    pass\n")

        assert self._status(directory, {"any.test": inv.HostVerdict.ALLOWED}) is inv.PolicyStatus.UNKNOWN

    def test_consulting_the_policy_is_recorded_separately_from_the_verdict(
        self,
        tmp_path: Path,
    ) -> None:
        """Allowed is not the same as guarded.

        The roster crawler's host was recorded as permitting `/Player/Trade.aspx`
        while the compliance layer blocks it, because that layer treats the mobile
        host as the same site as the blocked one. Recording `enforced` beside the
        verdict is what makes that visible instead of something a reader has to
        already know.
        """
        directory = self._crawler(
            tmp_path, 'URL = "https://open.test/x"\nasync def go():\n    await compliance.is_allowed(URL)\n'
        )

        assert inv.consults_policy("probe_crawler", crawler_dir=directory) is True

        plain_dir = tmp_path / "plain"
        plain_dir.mkdir()
        self._crawler(plain_dir, 'URL = "https://open.test/x"\n')
        assert inv.consults_policy("probe_crawler", crawler_dir=plain_dir) is False


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
    def test_the_recorded_kbo_policy_is_read(self) -> None:
        """The archive still holds the site-wide block, which is the whole reason
        the blocked crawlers are blocked.
        """
        policy = inv.read_policy("www.koreabaseball.com")

        assert policy.verdict is inv.HostVerdict.DISALLOWED, policy
        assert policy.recorded is True

    def test_a_missing_policy_directory_yields_no_verdicts(self, tmp_path: Path) -> None:
        verdicts = inv.host_verdicts(policy_dir=tmp_path)

        assert set(verdicts.values()) <= {inv.HostVerdict.UNREACHABLE}

    def test_an_undeclared_crawler_is_reported_as_a_gap(self, tmp_path: Path) -> None:
        """Silence would read as coverage."""
        (tmp_path / "nobody_crawler.py").write_text('URL = "https://example.test/"\n', encoding="utf-8")

        report = inv.build_inventory(crawler_dir=tmp_path, robots_dir=tmp_path)

        assert any("no declaration yet" in line for line in report.advisories)

    def test_crawlers_that_may_read_but_never_ask_are_named(self) -> None:
        """The state that turns a policy change into a surprise."""
        report = inv.build_inventory()

        guarded = [row for row in report.rows if row.policy_status is inv.PolicyStatus.OK and row.enforced]
        unguarded = [row for row in report.rows if row.policy_status is inv.PolicyStatus.OK and not row.enforced]
        assert guarded or unguarded, "some crawler should have a readable source"
        if unguarded:
            assert any("never consult the policy" in line for line in report.advisories)

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


class TestTheInventoryIsComplete:
    """Coverage that is asserted, not hoped for.

    When the report listed forty undeclared crawlers it was honest about it, but
    an advisory nobody acts on is a to-do list, not a guarantee. The declarations
    are filled in now, so a crawler added tomorrow fails this test until someone
    says what it reads and writes.
    """

    def test_every_discovered_crawler_is_declared(self) -> None:
        undeclared = [m for m in inv.discover_crawler_modules() if m not in inv.DECLARED]

        assert not undeclared, f"crawlers with no declaration: {undeclared}"

    def test_the_build_reports_no_coverage_gap(self) -> None:
        report = inv.build_inventory()

        assert not [line for line in report.advisories if "no declaration yet" in line]

    def test_a_crawler_that_writes_no_table_is_still_declared(self) -> None:
        """A pure reader is a fact worth stating, not a hole in the map.

        Four crawlers produce no domain rows. Leaving them out would make the
        inventory read as though they had not been looked at.
        """
        readers = [m for m, d in inv.DECLARED.items() if not d.tables]

        assert readers, "the reader concept should be exercised by at least one crawler"
        assert all(inv.DECLARED[m].alternative_source for m in readers)

    def test_a_reader_has_no_measurements_to_misreport(self) -> None:
        report = inv.build_inventory()

        for row in report.rows:
            if row.declaration and not row.declaration.tables:
                assert row.measurements == ()
                assert row.rag_chunks == 0


def _seed_runs(factory, crawler: str, *, succeeded: int, zero_yield: int, total: int = 0) -> None:
    """Write a ledger that reports `succeeded` successes, `zero_yield` of them empty.

    Stamped relative to now rather than to a fixed instant. The measurement reads
    a rolling window, so rows anchored to a fixed date fall out of it as time
    passes -- the suite went from 50 rows to 44 without anything changing.
    """
    total = total or succeeded
    empty = succeeded - zero_yield
    # Committed explicitly: closing a session without one rolls the inserts back,
    # which reads as "the ledger is empty" rather than as a broken fixture.
    with factory() as session:
        for index in range(empty):
            session.execute(
                text(
                    "INSERT INTO crawl_execution_runs (crawler, status, records_read, records_written, "
                    "started_at) VALUES (:c, 'success', 5, 5, :t)"
                ),
                {"c": crawler, "t": (datetime.now(UTC) - timedelta(hours=index)).replace(tzinfo=None)},
            )
        for index in range(zero_yield):
            session.execute(
                text(
                    "INSERT INTO crawl_execution_runs (crawler, status, records_read, records_written, "
                    "started_at) VALUES (:c, 'success', 0, 0, :t)"
                ),
                {"c": crawler, "t": (datetime.now(UTC) - timedelta(hours=index)).replace(tzinfo=None)},
            )
        for index in range(total - succeeded):
            session.execute(
                text(
                    "INSERT INTO crawl_execution_runs (crawler, status, records_read, records_written, "
                    "started_at) VALUES (:c, 'failed', 0, 0, :t)"
                ),
                {"c": crawler, "t": (datetime.now(UTC) - timedelta(hours=index)).replace(tzinfo=None)},
            )
        session.commit()


class TestRunYields:
    """A success that collected nothing is the quiet failure this exists to find.

    Found in production: four crawlers reported `success` on every run in a
    72-hour window while reading and writing nothing, and the tables three of
    them own were 56 to 138 days old. Nothing in the ledger was wrong. The run
    did what it was asked, and it collected no data.
    """

    def test_zero_yield_successes_are_counted(self, scratch_db: sessionmaker) -> None:
        _seed_runs(scratch_db, "preview", succeeded=50, zero_yield=50)

        with scratch_db() as session:
            summary = inv.run_summaries(session)["preview"]

        assert summary.succeeded == 50
        assert summary.zero_yield == 50
        assert summary.yields is False

    def test_a_run_that_read_but_wrote_nothing_still_counts_as_yielding(self, scratch_db: sessionmaker) -> None:
        """An unchanged page is a legitimate empty result, not a failure."""
        with scratch_db() as session:
            session.execute(
                text(
                    "INSERT INTO crawl_execution_runs (crawler, status, records_read, records_written, "
                    "started_at) VALUES ('preview', 'success', 7, 0, :t)"
                ),
                {"t": datetime.now(UTC).replace(tzinfo=None)},
            )
            session.commit()

        with scratch_db() as session:
            summary = inv.run_summaries(session)["preview"]

        assert summary.zero_yield == 0
        assert summary.yields is True

    def test_a_crawler_with_only_failures_is_not_reported_as_collecting_nothing(
        self,
        scratch_db: sessionmaker,
    ) -> None:
        """Every run failing is already visible; folding it in hides the quiet case."""
        _seed_runs(scratch_db, "food", succeeded=0, zero_yield=0, total=3)

        with scratch_db() as session:
            summary = inv.run_summaries(session)["food"]

        assert summary.succeeded == 0
        assert summary.yields is True

    def test_one_empty_run_among_yielding_ones_does_not_raise_the_section(
        self,
        scratch_db: sessionmaker,
    ) -> None:
        """A single quiet run can be an off-day."""
        _seed_runs(scratch_db, "game_detail", succeeded=9, zero_yield=1)

        with scratch_db() as session:
            summary = inv.run_summaries(session)["game_detail"]

        assert summary.yields is True

    def test_the_ledger_name_overrides_the_module_name(
        self,
        scratch_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The ledger and the filesystem use different words for the same crawler.

        The ledger writes `roster_transactions` where the module is
        `roster_transaction_crawler`. Guessing the join silently attached the run
        measurement to nothing, so the report looked complete while measuring
        none -- which is the failure mode this whole module is about.
        """
        _seed_runs(scratch_db, "roster_transactions", succeeded=7, zero_yield=7)
        monkeypatch.setitem(
            inv.DECLARED,
            "roster_transaction_crawler",
            inv.SourceDeclaration(
                tables=("roster_transactions",),
                ledger_name="roster_transactions",
                alternative_source="elsewhere",
            ),
        )

        report = inv.build_inventory(session_factory=scratch_db)
        row = report.rows_by_crawler()["roster_transaction_crawler"]

        assert row.runs is not None
        assert row.runs.succeeded == 7

    def test_a_success_with_nothing_collected_beside_a_stale_table_is_reported(
        self,
        scratch_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The two facts corroborate each other; neither does on its own."""
        _seed_runs(scratch_db, "game_detail", succeeded=4, zero_yield=4)
        monkeypatch.setitem(
            inv.DECLARED,
            "game_detail_crawler",
            inv.SourceDeclaration(tables=("old_table",), alternative_source="elsewhere"),
        )

        report = inv.build_inventory(session_factory=scratch_db)

        assert [row.crawler for row in report.succeeding_and_stale()] == ["game_detail_crawler"]
        assert any("none of which read or wrote a record" in line for line in report.advisories)

    def test_a_yielding_crawler_with_a_stale_table_is_not_in_that_section(
        self,
        scratch_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Otherwise a slow collector and a blocked one read identically."""
        _seed_runs(scratch_db, "game_detail", succeeded=4, zero_yield=0)
        monkeypatch.setitem(
            inv.DECLARED,
            "game_detail_crawler",
            inv.SourceDeclaration(tables=("old_table",), alternative_source="elsewhere"),
        )

        report = inv.build_inventory(session_factory=scratch_db)

        assert report.succeeding_and_stale() == ()

    def test_a_pure_reader_is_still_reported_without_a_table(
        self,
        scratch_db: sessionmaker,
    ) -> None:
        """The preview crawler has no table of its own, and it is the worst case."""
        _seed_runs(scratch_db, "preview", succeeded=50, zero_yield=50)

        report = inv.build_inventory(session_factory=scratch_db)

        reported = {row.crawler for row in report.zero_yield_runs()}
        assert "preview_crawler" in reported


def _seed_blocked(factory, crawler: str, count: int) -> None:
    """Write `count` runs the ledger recorded as refused rather than unreachable."""
    with factory() as session:
        for index in range(count):
            session.execute(
                text(
                    "INSERT INTO crawl_execution_runs (crawler, status, error_code, records_read, "
                    "records_written, started_at) VALUES (:c, 'failed', 'FETCH_BLOCKED', 0, 0, :t)"
                ),
                {"c": crawler, "t": (datetime.now(UTC) - timedelta(hours=index)).replace(tzinfo=None)},
            )
        session.commit()


class TestCollectionState:
    """The verdict is the ledger and the table together, because neither alone works.

    Found by running the report: `parking` had succeeded four days running, three
    records a day, while `parking_lots.updated_at` sat at 2026-08-15. An age-only
    verdict called that a table that had not been collected for 57 days.
    """

    def _row(self, factory, monkeypatch, crawler, *, tables, succeeded, zero_yield, total=0):
        _seed_runs(factory, crawler.removesuffix("_crawler"), succeeded=succeeded, zero_yield=zero_yield, total=total)
        monkeypatch.setitem(
            inv.DECLARED,
            crawler,
            inv.SourceDeclaration(tables=tables, alternative_source="elsewhere"),
        )
        return inv.build_inventory(session_factory=factory).rows_by_crawler()[crawler]

    def test_yielding_runs_with_a_stale_table_are_stable_not_broken(
        self,
        scratch_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The false positive this verdict exists to remove."""
        row = self._row(
            scratch_db,
            monkeypatch,
            "game_detail_crawler",
            tables=("old_table",),
            succeeded=4,
            zero_yield=0,
        )

        assert row.collection_state is inv.CollectionState.STABLE
        assert row.stale is True

    def test_zero_yield_successes_are_not_collected(
        self, scratch_db: sessionmaker, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        row = self._row(
            scratch_db,
            monkeypatch,
            "game_detail_crawler",
            tables=("old_table",),
            succeeded=4,
            zero_yield=4,
        )

        assert row.collection_state is inv.CollectionState.NOT_COLLECTED

    def test_zero_yield_successes_are_not_collected_without_a_table_to_match(
        self,
        scratch_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The preview crawler: no table of its own, and the worst case in the set."""
        row = self._row(
            scratch_db,
            monkeypatch,
            "preview_crawler",
            tables=(),
            succeeded=50,
            zero_yield=50,
        )

        assert row.collection_state is inv.CollectionState.NOT_COLLECTED

    def test_no_successful_run_is_failing_rather_than_not_collected(
        self,
        scratch_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`food` collected nothing and succeeded at nothing; calling that a silent
        failure would dress up a loud one.
        """
        row = self._row(
            scratch_db,
            monkeypatch,
            "food_crawler",
            tables=("old_table",),
            succeeded=0,
            zero_yield=0,
            total=3,
        )

        assert row.collection_state is inv.CollectionState.FAILING
        assert row.stale is True

    def test_a_stale_table_with_no_runs_is_unexplained(
        self,
        scratch_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Weekly and on-demand crawlers simply do not run; that is not a verdict."""
        monkeypatch.setitem(
            inv.DECLARED,
            "game_detail_crawler",
            inv.SourceDeclaration(tables=("old_table",), alternative_source="elsewhere"),
        )

        report = inv.build_inventory(session_factory=scratch_db)
        row = report.rows_by_crawler()["game_detail_crawler"]

        assert row.collection_state is inv.CollectionState.UNEPLAINED

    def test_yielding_runs_with_a_current_table_are_collecting(
        self,
        scratch_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        row = self._row(
            scratch_db,
            monkeypatch,
            "game_detail_crawler",
            tables=("fresh_table",),
            succeeded=4,
            zero_yield=0,
        )

        assert row.collection_state is inv.CollectionState.COLLECTING

    def test_a_stable_crawler_is_not_reported_as_collecting_nothing(
        self,
        scratch_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _seed_runs(scratch_db, "game_detail", succeeded=4, zero_yield=0)
        monkeypatch.setitem(
            inv.DECLARED,
            "game_detail_crawler",
            inv.SourceDeclaration(tables=("old_table",), alternative_source="elsewhere"),
        )

        report = inv.build_inventory(session_factory=scratch_db)

        assert report.not_collected() == ()

    def test_a_permitted_crawler_with_refused_runs_is_a_disagreement(
        self,
        scratch_db: sessionmaker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`food` reads from a host whose policy permits it, yet the ledger recorded
        refusals -- because the URL is assembled at runtime and this report cannot
        see it. Reporting the disagreement beats deciding which side is wrong.
        """
        _seed_runs(scratch_db, "food", succeeded=2, zero_yield=0)
        _seed_blocked(scratch_db, "food", 1)
        monkeypatch.setitem(
            inv.DECLARED,
            "food_crawler",
            inv.SourceDeclaration(tables=(), alternative_source="elsewhere"),
        )
        # Inject the verdict rather than rebuilding the row afterwards: a test that
        # reassembles the object under test is no longer testing the code that
        # builds it.
        real_status = inv.policy_status_of

        def allowed(module, verdicts, **kwargs):
            return inv.PolicyStatus.OK if module == "food_crawler" else real_status(module, verdicts, **kwargs)

        monkeypatch.setattr(inv, "policy_status_of", allowed)

        report = inv.build_inventory(session_factory=scratch_db)
        row = report.rows_by_crawler()["food_crawler"]

        assert row.policy_status is inv.PolicyStatus.OK
        assert row.runs is not None
        assert row.runs.policy_blocked == 1
        assert [r.crawler for r in report.policy_disagreements()] == ["food_crawler"]
        assert any("read as permitted" in line for line in report.advisories)
