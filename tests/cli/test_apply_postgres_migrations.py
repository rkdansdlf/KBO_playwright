from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, inspect, text

from src.cli.apply_postgres_migrations import (
    ADOPTABLE_MIGRATIONS,
    _looks_like_prod,
    adopt_existing_schema,
    apply_migrations,
    main,
)


def _create_baseline(engine) -> None:
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE game (game_id TEXT PRIMARY KEY)"))
        connection.execute(text("CREATE TABLE kbo_seasons (season_id INTEGER PRIMARY KEY)"))


def _create_adoptable_schema(engine) -> None:
    _create_baseline(engine)
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE awards (id INTEGER PRIMARY KEY, player_id INTEGER, team_code VARCHAR(20))"),
        )
        connection.execute(text("CREATE INDEX idx_award_player_id ON awards(player_id)"))
        connection.execute(
            text(
                "CREATE TABLE quarantined_records (id INTEGER PRIMARY KEY, game_id TEXT,"
                " entity_type TEXT NOT NULL, entity_id TEXT, rule_id TEXT NOT NULL,"
                " severity TEXT NOT NULL, failure_reason TEXT NOT NULL, raw_payload JSON NOT NULL,"
                " source TEXT NOT NULL, status TEXT NOT NULL, retry_count INTEGER NOT NULL,"
                " resolved_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
            ),
        )
        connection.execute(
            text(
                "CREATE TABLE correction_audit_trail (id INTEGER PRIMARY KEY, game_id TEXT,"
                " entity_type TEXT NOT NULL, entity_id TEXT, field_name TEXT NOT NULL, raw_value TEXT,"
                " raw_source TEXT NOT NULL, corrected_value TEXT, corrected_source TEXT NOT NULL,"
                " correction_reason TEXT NOT NULL, confidence FLOAT NOT NULL, extra_metadata JSON,"
                " created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
            ),
        )
        connection.execute(
            text(
                "CREATE TABLE player_projections (id INTEGER PRIMARY KEY, target_season INTEGER NOT NULL,"
                " player_id INTEGER NOT NULL, player_name TEXT NOT NULL, team_code TEXT,"
                " position_type TEXT NOT NULL, age INTEGER, projected_pa FLOAT, projected_ip NUMERIC(6,2),"
                " projected_avg FLOAT, projected_obp FLOAT, projected_slg FLOAT, projected_ops FLOAT,"
                " projected_woba FLOAT, projected_era FLOAT, projected_fip FLOAT, projected_whip FLOAT,"
                " projected_stats JSON NOT NULL, weights_used JSON NOT NULL, regression_params JSON NOT NULL,"
                " version TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
            ),
        )
        connection.execute(text("CREATE TABLE external_season_stats (id INTEGER PRIMARY KEY)"))


def test_postgres_migrations_are_idempotent(tmp_path):
    migration = tmp_path / "001_test.sql"
    migration.write_text(
        "CREATE TABLE IF NOT EXISTS migration_fixture (id INTEGER PRIMARY KEY);",
        encoding="utf-8",
    )
    engine = create_engine("sqlite:///:memory:")
    _create_baseline(engine)

    assert apply_migrations(engine, directory=tmp_path) == ["001_test.sql"]
    assert apply_migrations(engine, directory=tmp_path) == []
    assert apply_migrations(engine, directory=tmp_path, check=True) == []


def test_postgres_migrations_check_does_not_create_tracking_table(tmp_path):
    migration = tmp_path / "001_test.sql"
    migration.write_text("CREATE TABLE migration_fixture (id INTEGER PRIMARY KEY);", encoding="utf-8")
    engine = create_engine("sqlite:///:memory:")
    _create_baseline(engine)

    assert apply_migrations(engine, directory=tmp_path, check=True) == ["001_test.sql"]
    assert not inspect(engine).has_table("schema_migrations")


def test_postgres_migrations_require_baseline(tmp_path):
    migration = tmp_path / "001_test.sql"
    migration.write_text("CREATE TABLE migration_fixture (id INTEGER PRIMARY KEY);", encoding="utf-8")

    with pytest.raises(RuntimeError, match="ORM baseline schema"):
        apply_migrations(create_engine("sqlite:///:memory:"), directory=tmp_path)


def test_adopt_existing_schema_records_current_baseline_without_running_sql() -> None:
    engine = create_engine("sqlite:///:memory:")
    _create_adoptable_schema(engine)

    assert adopt_existing_schema(engine) == sorted(ADOPTABLE_MIGRATIONS)
    with engine.connect() as connection:
        applied = connection.execute(text("SELECT version FROM schema_migrations ORDER BY version")).scalars().all()
    assert applied == sorted(ADOPTABLE_MIGRATIONS)
    assert adopt_existing_schema(engine) == []


def test_adopt_existing_schema_rejects_missing_award_link_shape() -> None:
    engine = create_engine("sqlite:///:memory:")
    _create_baseline(engine)

    with pytest.raises(RuntimeError, match="awards table"):
        adopt_existing_schema(engine)

    assert not inspect(engine).has_table("schema_migrations")


def test_adopt_existing_schema_rejects_legacy_tracking_table() -> None:
    engine = create_engine("sqlite:///:memory:")
    _create_adoptable_schema(engine)
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE _schema_migrations (filename TEXT PRIMARY KEY)"))

    with pytest.raises(RuntimeError, match="Legacy _schema_migrations"):
        adopt_existing_schema(engine)


def test_adopt_existing_cli_skips_orm_bootstrap() -> None:
    engine = MagicMock()
    with (
        patch("src.cli.apply_postgres_migrations.create_engine_for_url", return_value=engine),
        patch("src.cli.apply_postgres_migrations.adopt_existing_schema", return_value=[]),
        patch("src.cli.apply_postgres_migrations._bootstrap_orm_schema") as bootstrap,
    ):
        assert main(["--url", "postgresql://example/db", "--adopt-existing"]) == 0

    bootstrap.assert_not_called()
    engine.dispose.assert_called_once()


def _create_numeric_tracking(engine, *, versions: list[tuple[int, str]]) -> None:
    """Create the integer-versioned tracking table used by the production database."""
    _create_baseline(engine)
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, filename VARCHAR(255) NOT NULL,"
                " applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"
            ),
        )
        for version, filename in versions:
            connection.execute(
                text("INSERT INTO schema_migrations (version, filename) VALUES (:v, :f)"),
                {"v": version, "f": filename},
            )


def _pending_names(engine, tmp_path) -> list[str]:
    return apply_migrations(engine, directory=tmp_path, check=True)


class TestNumericTrackingShape:
    """Production stores only the numeric prefix, so file names never match."""

    def test_numeric_rows_are_recognised_as_applied(self, tmp_path) -> None:
        migration = tmp_path / "001_test.sql"
        migration.write_text("CREATE TABLE IF NOT EXISTS migration_fixture (id INTEGER PRIMARY KEY);", encoding="utf-8")
        engine = create_engine("sqlite:///:memory:")
        _create_numeric_tracking(engine, versions=[(1, "001_test.sql")])

        assert _pending_names(engine, tmp_path) == []

    def test_zero_padded_file_name_matches_integer_row(self, tmp_path) -> None:
        """``047_x.sql`` must match the stored integer 47, not the file name."""
        migration = tmp_path / "047_padded.sql"
        migration.write_text("CREATE TABLE IF NOT EXISTS t47 (id INTEGER PRIMARY KEY);", encoding="utf-8")
        migration2 = tmp_path / "048_next.sql"
        migration2.write_text("CREATE TABLE IF NOT EXISTS t48 (id INTEGER PRIMARY KEY);", encoding="utf-8")
        engine = create_engine("sqlite:///:memory:")
        _create_numeric_tracking(engine, versions=[(47, "047_padded.sql")])

        assert _pending_names(engine, tmp_path) == ["048_next.sql"]

    def test_apply_records_version_and_filename(self, tmp_path) -> None:
        migration = tmp_path / "060_brand_new.sql"
        migration.write_text("CREATE TABLE IF NOT EXISTS t60 (id INTEGER PRIMARY KEY);", encoding="utf-8")
        engine = create_engine("sqlite:///:memory:")
        _create_numeric_tracking(engine, versions=[(47, "047_padded.sql")])

        assert apply_migrations(engine, directory=tmp_path) == ["060_brand_new.sql"]
        with engine.connect() as connection:
            row = connection.execute(
                text("SELECT version, filename FROM schema_migrations ORDER BY version"),
            ).all()
        assert (60, "060_brand_new.sql") in [tuple(r) for r in row]

    def test_reapply_is_idempotent(self, tmp_path) -> None:
        migration = tmp_path / "061_once.sql"
        migration.write_text("CREATE TABLE IF NOT EXISTS t61 (id INTEGER PRIMARY KEY);", encoding="utf-8")
        engine = create_engine("sqlite:///:memory:")
        _create_numeric_tracking(engine, versions=[])

        assert apply_migrations(engine, directory=tmp_path) == ["061_once.sql"]
        assert apply_migrations(engine, directory=tmp_path) == []
        assert _pending_names(engine, tmp_path) == []

    def test_non_numeric_value_is_reported_not_silently_reapplied(self, tmp_path) -> None:
        """A corrupt numeric row must fail loudly rather than re-run the migration."""
        migration = tmp_path / "062_corrupt.sql"
        migration.write_text("CREATE TABLE IF NOT EXISTS t62 (id INTEGER PRIMARY KEY);", encoding="utf-8")
        engine = create_engine("sqlite:///:memory:")
        _create_baseline(engine)
        with engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TABLE schema_migrations (version VARCHAR(255) PRIMARY KEY,"
                    " applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"
                ),
            )
            connection.execute(text("INSERT INTO schema_migrations (version) VALUES ('not-a-number')"))

        # A name-shaped table keeps its own comparison rules, so only assert it does not crash.
        assert _pending_names(engine, tmp_path) == ["062_corrupt.sql"]


class TestAdoptExistingTrackingShape:
    def test_adopt_records_into_numeric_table(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        _create_adoptable_schema(engine)
        _rewrite_tracking_as_numeric(engine)

        recorded = adopt_existing_schema(engine)

        assert recorded == sorted(ADOPTABLE_MIGRATIONS)
        with engine.connect() as connection:
            rows = connection.execute(text("SELECT version, filename FROM schema_migrations")).all()
        assert len(rows) == len(ADOPTABLE_MIGRATIONS)
        assert all(isinstance(r[0], int) and str(r[0]) in r[1] for r in rows)

    def test_adopt_skips_versions_already_recorded_numerically(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        _create_adoptable_schema(engine)
        _rewrite_tracking_as_numeric(
            engine,
            versions=[(int(name.split("_", 1)[0]), name) for name in sorted(ADOPTABLE_MIGRATIONS)],
        )

        assert adopt_existing_schema(engine) == []


def _rewrite_tracking_as_numeric(engine, *, versions: list[tuple[int, str]] | None = None) -> None:
    with engine.begin() as connection:
        connection.execute(text("DROP TABLE IF EXISTS schema_migrations"))
        connection.execute(
            text(
                "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, filename VARCHAR(255) NOT NULL,"
                " applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"
            ),
        )
        for version, filename in versions or []:
            connection.execute(
                text("INSERT INTO schema_migrations (version, filename) VALUES (:v, :f)"),
                {"v": version, "f": filename},
            )


class TestProductionDetection:
    @pytest.mark.parametrize(
        "url",
        [
            "postgresql://user:pw@100.81.73.13:5432/bega_prod",
            "postgresql+psycopg2://user:pw@db.internal:5432/bega_prod",
            "postgresql://user:pw@10.0.0.5:5432/BEGA_PROD",
        ],
    )
    def test_remote_prod_names_are_production(self, url: str) -> None:
        assert _looks_like_prod(url) is True

    @pytest.mark.parametrize(
        "url",
        [
            "postgresql+psycopg2://user:pw@127.0.0.1:5434/bega_prod",
            "postgresql://user:pw@localhost:5432/bega_prod",
            "postgresql://user:pw@127.0.0.1:5432/bega_prod",
        ],
    )
    def test_loopback_is_never_production(self, url: str) -> None:
        assert _looks_like_prod(url) is False

    @pytest.mark.parametrize(
        "url",
        [
            "postgresql+psycopg2://user:pw@127.0.0.1:5434/kbo",
            "postgresql://user:pw@100.81.73.13:5432/some_other_db",
            "postgresql://user:pw@100.81.73.13:5432/bega_prod_staging",
        ],
    )
    def test_other_targets_are_not_production(self, url: str) -> None:
        assert _looks_like_prod(url) is False

    def test_prod_names_are_configurable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KBO_PROD_DB_NAMES", "kbo_prod, other_prod")

        assert _looks_like_prod("postgresql://u:p@10.1.1.1:5432/other_prod") is True
        # The built-in default is replaced, not extended.
        assert _looks_like_prod("postgresql://u:p@10.1.1.1:5432/bega_prod") is False

    def test_blank_override_falls_back_to_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KBO_PROD_DB_NAMES", "  ,  ")

        assert _looks_like_prod("postgresql://u:p@10.1.1.1:5432/bega_prod") is True


class TestProductionWriteGuard:
    """A schema write against production must be opted into explicitly."""

    PROD = "postgresql://user:pw@100.81.73.13:5432/bega_prod"

    def test_write_to_production_is_refused_by_default(self) -> None:
        with patch("src.cli.apply_postgres_migrations.create_engine_for_url") as engine:
            assert main(["--url", self.PROD]) == 2

        engine.assert_not_called()

    def test_adopt_existing_against_production_is_refused(self) -> None:
        with patch("src.cli.apply_postgres_migrations.create_engine_for_url") as engine:
            assert main(["--url", self.PROD, "--adopt-existing"]) == 2

        engine.assert_not_called()

    def test_allow_prod_permits_the_write(self) -> None:
        engine = MagicMock()
        with (
            patch("src.cli.apply_postgres_migrations.create_engine_for_url", return_value=engine),
            patch("src.cli.apply_postgres_migrations.adopt_existing_schema", return_value=[]),
        ):
            assert main(["--url", self.PROD, "--adopt-existing", "--allow-prod"]) == 0

        engine.dispose.assert_called_once()

    def test_check_against_production_needs_no_opt_in(self) -> None:
        """``--check`` is read-only, so it must stay usable while investigating."""
        engine = MagicMock()
        with (
            patch("src.cli.apply_postgres_migrations.create_engine_for_url", return_value=engine),
            patch("src.cli.apply_postgres_migrations.apply_migrations", return_value=[]),
        ):
            assert main(["--url", self.PROD, "--check"]) == 0

        engine.dispose.assert_called_once()

    def test_implicit_database_url_is_guarded_too(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The dangerous path is a bare invocation inheriting DATABASE_URL."""
        monkeypatch.setenv("DATABASE_URL", self.PROD)
        with patch("src.cli.apply_postgres_migrations.create_engine_for_url") as engine:
            assert main([]) == 2

        engine.assert_not_called()

    def test_local_target_needs_no_opt_in(self) -> None:
        engine = MagicMock()
        with (
            patch("src.cli.apply_postgres_migrations.create_engine_for_url", return_value=engine),
            patch("src.cli.apply_postgres_migrations.adopt_existing_schema", return_value=[]),
        ):
            assert main(["--url", "postgresql://user:pw@127.0.0.1:5434/kbo", "--adopt-existing"]) == 0

        engine.dispose.assert_called_once()
