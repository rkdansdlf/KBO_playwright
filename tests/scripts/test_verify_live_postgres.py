"""Tests for the read-only PostgreSQL live acceptance script.

The evaluators and config handling are pure and run everywhere. The integration
test builds a scratch database on the server named by ``KBO_TEST_PG_URL``, runs the
whole script against it, and proves the script wrote nothing.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import UTC, date, datetime
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool

from scripts.verification import verify_live_postgres as vlp
from src.constants import KST

SECRET = "s3cret-pass"
PG_URL = f"postgresql+psycopg://kbo_ci_ro:{SECRET}@100.81.73.13:5432/bega_prod"
IN_SEASON_NOW = datetime(2026, 7, 15, 12, 0, tzinfo=KST)


# --- configuration -------------------------------------------------------------


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+psycopg://u:p@host:5432/db",
        "postgresql://u:p@host:5432/db",
    ],
)
def test_postgres_urls_are_accepted(url):
    assert vlp.resolve_url(url, {}) == url


def test_cli_url_takes_precedence_over_environment():
    assert vlp.resolve_url(PG_URL, {vlp.ENV_VAR: "postgresql://other/db"}) == PG_URL


def test_missing_url_is_a_config_error_not_a_default():
    with pytest.raises(vlp.ConfigError, match="never falls back to a default database"):
        vlp.resolve_url(None, {})


def test_blank_url_is_treated_as_missing():
    with pytest.raises(vlp.ConfigError):
        vlp.resolve_url("   ", {vlp.ENV_VAR: "  "})


@pytest.mark.parametrize(
    ("url", "backend"),
    [
        ("sqlite:///./data/kbo_dev.db", "sqlite"),
        ("oracle+oracledb://user:pw@db/service", "oracle"),
        ("postgres://u:p@host/db", "postgres"),
    ],
)
def test_non_postgres_urls_are_config_errors(url, backend):
    with pytest.raises(vlp.ConfigError, match=f"got backend '{backend}'"):
        vlp.resolve_url(url, {})


def test_unparseable_url_error_does_not_echo_the_url():
    broken = f"postgresql+psycopg://kbo_ci_ro:{SECRET}@host:notaport/db"
    with pytest.raises(vlp.ConfigError) as info:
        vlp.resolve_url(broken, {})
    assert SECRET not in str(info.value)
    assert info.value.__cause__ is None


def test_main_exits_2_for_missing_url_without_touching_a_database(monkeypatch, capsys):
    monkeypatch.delenv(vlp.ENV_VAR, raising=False)

    def fail_if_called(url):
        msg = f"no engine may be built on a config error, got {url}"
        raise AssertionError(msg)

    monkeypatch.setattr(vlp, "build_engine", fail_if_called)

    assert vlp.main([]) == vlp.EXIT_CONFIG
    assert "config error" in capsys.readouterr().err


def test_main_exits_2_for_sqlite_url_without_creating_a_file(monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(vlp.ENV_VAR, "sqlite:///./stray.db")

    assert vlp.main([]) == vlp.EXIT_CONFIG
    assert not (tmp_path / "stray.db").exists()
    assert "postgresql" in capsys.readouterr().err


def test_main_rejects_non_positive_freshness_limits(monkeypatch):
    monkeypatch.setenv(vlp.ENV_VAR, PG_URL)
    with pytest.raises(SystemExit) as info:
        vlp.main(["--max-sync-age-hours", "0"])
    assert info.value.code == 2


# --- redaction -----------------------------------------------------------------


def test_redaction_removes_the_password():
    redacted = vlp.redact(PG_URL)

    assert SECRET not in redacted
    assert "kbo_ci_ro" in redacted
    assert "100.81.73.13" in redacted


def test_report_json_never_contains_the_password():
    checks = [vlp.CheckResult(name="connection", ok=True, message="ok")]
    report = vlp.build_report(PG_URL, checks, now=datetime(2026, 7, 15, tzinfo=UTC))

    assert SECRET not in json.dumps(asdict(report), default=str)


# --- report shape --------------------------------------------------------------


def test_report_status_fails_when_any_check_fails():
    checks = [
        vlp.CheckResult(name="connection", ok=True, message="ok"),
        vlp.CheckResult(name="schema", ok=False, message="missing"),
    ]

    assert vlp.build_report(PG_URL, checks, now=datetime(2026, 7, 15, tzinfo=UTC)).status == "FAILED"


def test_report_status_passes_with_skips_and_warnings():
    checks = [
        vlp.CheckResult(name="connection", ok=True, message="ok"),
        vlp.CheckResult(name="freshness_games", ok=True, message="skipped", skipped=True),
        vlp.CheckResult(name="role_privileges", ok=True, message="writable", warning="use a ro role"),
    ]

    assert vlp.build_report(PG_URL, checks, now=datetime(2026, 7, 15, tzinfo=UTC)).status == "PASSED"


def test_report_json_has_the_documented_shape():
    checks = [
        vlp.CheckResult(name="connection", ok=True, message="ok", details={"server_version": "17"}),
        vlp.CheckResult(name="freshness_games", ok=True, message="skipped", skipped=True),
    ]
    payload = json.loads(json.dumps(asdict(vlp.build_report(PG_URL, checks, now=datetime(2026, 7, 15, tzinfo=UTC)))))

    assert set(payload) == {"database", "timestamp", "status", "checks"}
    assert payload["status"] == "PASSED"
    assert payload["timestamp"].startswith("2026-07-15")
    assert set(payload["checks"][0]) == {"name", "ok", "message", "details", "warning", "skipped"}
    assert payload["checks"][0]["details"] == {"server_version": "17"}
    assert payload["checks"][1]["skipped"] is True


# --- role privileges -----------------------------------------------------------


def test_read_only_role_passes_even_with_the_flag():
    result = vlp.evaluate_role(
        is_superuser=False,
        can_create_in_public=False,
        writable_tables=[],
        require_readonly_role=True,
    )

    assert result.ok is True
    assert result.warning is None


def test_writable_role_is_a_warning_without_the_flag():
    result = vlp.evaluate_role(
        is_superuser=False,
        can_create_in_public=False,
        writable_tables=["game"],
        require_readonly_role=False,
    )

    assert result.ok is True
    assert result.warning is not None
    assert "write privilege on 1 public table" in result.message


def test_writable_role_fails_with_the_flag_and_names_the_fix():
    result = vlp.evaluate_role(
        is_superuser=True,
        can_create_in_public=True,
        writable_tables=[],
        require_readonly_role=True,
    )

    assert result.ok is False
    assert "pg_read_all_data" in result.message
    assert "superuser" in result.message


def test_role_details_sample_at_most_five_writable_tables():
    tables = [f"t{i}" for i in range(9)]
    result = vlp.evaluate_role(
        is_superuser=False,
        can_create_in_public=False,
        writable_tables=tables,
        require_readonly_role=False,
    )

    assert result.details["writable_tables_count"] == 9
    assert result.details["writable_tables_sample"] == tables[:5]


# --- schema --------------------------------------------------------------------


def test_missing_orm_column_fails_the_schema_check():
    result = vlp.evaluate_schema(
        missing_tables=[],
        missing_columns=["game.home_franchise_id"],
        extra_tables=[],
        extra_columns=[],
        missing_index_count=0,
    )

    assert result.ok is False
    assert result.details["missing_columns"] == ["game.home_franchise_id"]


def test_extra_live_objects_are_information_not_failure():
    result = vlp.evaluate_schema(
        missing_tables=[],
        missing_columns=[],
        extra_tables=["legacy_table"],
        extra_columns=["game.legacy_flag"],
        missing_index_count=3,
    )

    assert result.ok is True
    assert result.details["extra_tables"] == ["legacy_table"]
    assert result.details["extra_columns"] == ["game.legacy_flag"]
    assert result.details["missing_index_count"] == 3


# --- freshness with an injected clock -----------------------------------------


def test_fresh_sync_passes():
    now = datetime(2026, 10, 27, 12, 0, tzinfo=KST)
    latest = datetime(2026, 10, 27, 1, 0)  # naive UTC, as the ledger stores it

    result = vlp.evaluate_sync_freshness(latest, now=now, max_age_hours=72)

    assert result.ok is True


def test_stale_sync_fails():
    now = datetime(2026, 10, 27, 12, 0, tzinfo=KST)
    latest = datetime(2026, 10, 20, 0, 0)

    result = vlp.evaluate_sync_freshness(latest, now=now, max_age_hours=72)

    assert result.ok is False
    assert "limit 72h" in result.message


def test_missing_sync_history_fails():
    result = vlp.evaluate_sync_freshness(None, now=IN_SEASON_NOW, max_age_hours=72)

    assert result.ok is False


NOV_15 = datetime(2026, 11, 15, 12, 0, tzinfo=KST)


def test_no_recent_past_game_is_a_skip_not_a_failure():
    # Off-season or a break: nothing was scheduled in the last 10 days.
    result = vlp.evaluate_game_freshness(None, None, now=NOV_15, max_age_days=10)

    assert result.ok is True
    assert result.skipped is True
    assert "no games scheduled in the last 10 days" in result.message


def test_last_game_older_than_the_window_is_a_skip():
    result = vlp.evaluate_game_freshness(date(2026, 11, 4), date(2026, 11, 4), now=NOV_15, max_age_days=10)

    assert result.skipped is True


def test_game_exactly_at_the_window_edge_is_checked():
    result = vlp.evaluate_game_freshness(date(2026, 11, 5), None, now=NOV_15, max_age_days=10)

    assert result.skipped is False
    assert result.ok is False


def test_completed_game_within_the_grace_passes():
    result = vlp.evaluate_game_freshness(date(2026, 11, 14), date(2026, 11, 12), now=NOV_15, max_age_days=10)

    assert result.ok is True
    assert result.skipped is False
    assert result.details["latest_completed_game_date"] == "2026-11-12"


def test_scheduled_past_games_without_results_fail_and_name_both_dates():
    result = vlp.evaluate_game_freshness(date(2026, 11, 14), date(2026, 11, 10), now=NOV_15, max_age_days=10)

    assert result.ok is False
    assert "latest scheduled past game 2026-11-14" in result.message
    assert "latest completed 2026-11-10" in result.message
    assert result.details == {
        "today_kst": "2026-11-15",
        "latest_past_game_date": "2026-11-14",
        "latest_completed_game_date": "2026-11-10",
    }


def test_past_games_with_no_completed_result_at_all_fail():
    result = vlp.evaluate_game_freshness(date(2026, 11, 14), None, now=NOV_15, max_age_days=10)

    assert result.ok is False
    assert "latest completed none" in result.message


def test_today_is_judged_on_the_kst_calendar():
    # 2026-11-14 16:00 UTC is already 2026-11-15 01:00 KST, so the 14th is yesterday.
    now = datetime(2026, 11, 14, 16, 0, tzinfo=UTC)

    result = vlp.evaluate_game_freshness(date(2026, 11, 14), date(2026, 11, 14), now=now, max_age_days=10)

    assert result.details["today_kst"] == "2026-11-15"
    assert result.ok is True


# --- core tables -----------------------------------------------------------------


def test_core_tables_pass_only_when_every_table_is_populated():
    states = dict.fromkeys(vlp.CORE_TABLES, "populated")
    assert vlp.evaluate_core_tables(states).ok is True

    states["game"] = "empty"
    result = vlp.evaluate_core_tables(states)
    assert result.ok is False
    assert "game (empty)" in result.message


# --- integration against a real PostgreSQL --------------------------------------

pg_required = pytest.mark.skipif(
    not os.environ.get("KBO_TEST_PG_URL"),
    reason="set KBO_TEST_PG_URL to a writable PostgreSQL URL to run the live read-only integration test",
)


@pytest.fixture
def scratch_database():
    """Create and drop a throwaway database; the writable URL is used for setup only."""
    admin_url = make_url(os.environ["KBO_TEST_PG_URL"])
    name = f"kbo_live_verify_{uuid4().hex[:10]}"
    admin = create_engine(
        admin_url.set(database="postgres"),
        isolation_level="AUTOCOMMIT",
        poolclass=NullPool,
    )
    with admin.connect() as connection:
        connection.execute(text(f'CREATE DATABASE "{name}"'))
    try:
        yield admin_url.set(database=name).render_as_string(hide_password=False)
    finally:
        with admin.connect() as connection:
            connection.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


def _build_baseline(url: str) -> None:
    """Build the schema the way production does: ORM tables, then migrations."""
    from src.cli.sync.apply_postgres_migrations import apply_migrations
    from src.models.base import Base
    import src.models

    engine = create_engine(url, poolclass=NullPool)
    try:
        with engine.begin() as connection:
            connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        Base.metadata.create_all(bind=engine)
        apply_migrations(engine)
    finally:
        engine.dispose()


def _public_write_counters(url: str) -> dict[str, int]:
    engine = create_engine(url, poolclass=NullPool)
    try:
        with engine.connect() as connection:
            rows = connection.execute(
                text(
                    "SELECT relname, n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_user_tables ORDER BY relname",
                ),
            ).all()
    finally:
        engine.dispose()
    return {row[0]: row[1] + row[2] + row[3] for row in rows}


def _insert_games(url: str, rows: list[tuple[str, date, str | None]]) -> None:
    engine = create_engine(url, poolclass=NullPool)
    try:
        with engine.begin() as connection:
            for game_id, game_date, status in rows:
                connection.execute(
                    text("INSERT INTO game (game_id, game_date, game_status) VALUES (:id, :day, :status)"),
                    {"id": game_id, "day": game_date, "status": status},
                )
    finally:
        engine.dispose()


@pg_required
@pytest.mark.integration
def test_game_freshness_uses_the_schedule_and_ignores_cancelled_games(scratch_database):
    _build_baseline(scratch_database)
    _insert_games(
        scratch_database,
        [
            ("20261020OBLG0", date(2026, 10, 20), "COMPLETED"),
            ("20261024LGOB0", date(2026, 10, 24), "SCHEDULED"),
            ("20261023NULLS0", date(2026, 10, 23), None),
            ("20261025CANCL0", date(2026, 10, 25), "CANCELLED"),
            ("20261026POSTP0", date(2026, 10, 26), "POSTPONED"),
        ],
    )
    engine = vlp.build_engine(scratch_database)
    try:
        now = datetime(2026, 10, 27, 12, 0, tzinfo=KST)
        stale = vlp.check_game_freshness(engine, now=now, max_age_days=10)
        # The 24th has no result and the last completed game is the 20th: a gap of 4 days.
        assert stale.ok is False
        assert stale.details["latest_past_game_date"] == "2026-10-24"
        assert stale.details["latest_completed_game_date"] == "2026-10-20"

        _insert_games(scratch_database, [("20261023OBLG0", date(2026, 10, 23), "COMPLETED")])
        fresh = vlp.check_game_freshness(engine, now=now, max_age_days=10)
        assert fresh.ok is True
        assert fresh.skipped is False
    finally:
        engine.dispose()


@pg_required
@pytest.mark.integration
def test_script_runs_read_only_against_a_real_postgres(scratch_database):
    _build_baseline(scratch_database)
    before = _public_write_counters(scratch_database)

    engine = vlp.build_engine(scratch_database)
    try:
        checks = vlp.run_checks(
            engine,
            now=IN_SEASON_NOW,
            max_sync_age_hours=72,
            max_game_age_days=10,
            require_readonly_role=False,
        )
    finally:
        engine.dispose()

    by_name = {check.name: check for check in checks}
    assert by_name["connection"].ok is True
    assert by_name["connection"].details["transaction_read_only"] == "on"
    assert by_name["migrations"].ok is True, by_name["migrations"].message
    assert by_name["schema"].ok is True, by_name["schema"].message
    assert by_name["freshness_games"].skipped is True  # empty scratch game table: no recent games
    assert by_name["core_tables"].ok is False  # empty scratch tables

    # The script wrote nothing: no tuple inserted, updated, or deleted.
    assert _public_write_counters(scratch_database) == before


@pg_required
@pytest.mark.integration
def test_a_write_inside_the_script_session_is_refused(scratch_database):
    _build_baseline(scratch_database)

    engine = vlp.build_engine(scratch_database)
    try:
        with vlp.read_only_transaction(engine) as connection, pytest.raises(SQLAlchemyError) as info:
            connection.execute(text("CREATE TABLE should_never_exist (id integer)"))
        assert "read-only" in str(info.value).lower()
    finally:
        engine.dispose()


@pg_required
@pytest.mark.integration
def test_default_session_is_read_only_even_without_a_transaction_guard(scratch_database):
    _build_baseline(scratch_database)

    engine = vlp.build_engine(scratch_database)
    try:
        with engine.connect() as connection, pytest.raises(SQLAlchemyError) as info:
            connection.execute(text("CREATE TABLE should_never_exist_either (id integer)"))
        assert "read-only" in str(info.value).lower()
    finally:
        engine.dispose()


@pg_required
@pytest.mark.integration
def test_writable_superuser_role_is_flagged_by_the_flag(scratch_database):
    _build_baseline(scratch_database)

    engine = vlp.build_engine(scratch_database)
    try:
        result = vlp.check_role(engine, require_readonly_role=True)
    finally:
        engine.dispose()

    # The integration URL is a writable role by design; the flag must fail it.
    assert result.details["writable_tables_count"] > 0 or result.details["is_superuser"]
    assert result.ok is False
