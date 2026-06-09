from unittest.mock import MagicMock, patch

from scripts.legacy.maintenance.full_audit import (
    GATE_METRIC_KEYS,
    _count_orphans,
    _dialect_name,
    collect_audit_metrics,
    evaluate_strict_zero,
    flatten_gate_metrics,
    parse_args,
    run_audit,
    table_exists,
)


class TestDialectName:
    def test_sqlite(self):
        conn = MagicMock()
        conn.dialect.name = "sqlite"
        assert _dialect_name(conn) == "sqlite"


class TestTableExists:
    def test_sqlite_true(self):
        conn = MagicMock()
        conn.dialect.name = "sqlite"
        conn.execute.return_value.first.return_value = (1,)
        assert table_exists(conn, "game") is True

    def test_sqlite_false(self):
        conn = MagicMock()
        conn.dialect.name = "sqlite"
        conn.execute.return_value.first.return_value = None
        assert table_exists(conn, "nonexistent") is False


class TestCountOrphans:
    def test_missing_columns(self):
        conn = MagicMock()
        conn.dialect.name = "sqlite"
        conn.execute.return_value.fetchall.return_value = []
        conn.execute.return_value.first.return_value = None
        result = _count_orphans(conn, table_name="t", column="missing", parent_table="p", parent_column="id")
        assert result == 0


class TestCollectAuditMetrics:
    @patch("scripts.legacy.maintenance.full_audit.table_columns")
    def test_returns_structure(self, mock_columns):
        conn = MagicMock()
        conn.dialect.name = "sqlite"
        conn.execute.return_value.scalar.return_value = 0
        conn.execute.return_value.fetchall.return_value = []
        conn.execute.return_value.first.return_value = None
        mock_columns.return_value = {"id", "name", "game_id", "player_id", "team_code", "team_id",
                                      "hits", "at_bats", "plate_appearances", "earned_runs", "runs_allowed",
                                      "game_date", "game_status", "home_score", "away_score", "team_side",
                                      "season", "league", "level", "source", "team", "code", "alternate_code",
                                      "player_name", "other"}

        report = collect_audit_metrics(conn)
        assert "orphans" in report
        assert "duplicates" in report
        assert "logical_errors" in report


class TestFlattenGateMetrics:
    def test_empty_report(self):
        flat = flatten_gate_metrics({})
        assert all(k in flat for k in GATE_METRIC_KEYS)
        assert all(v == 0 for v in flat.values())


class TestEvaluateStrictZero:
    def test_all_zero(self):
        report = {
            "orphans": {},
            "duplicates": {},
            "team_collisions": {},
            "logical_errors": {},
            "nulls": {},
            "pseudo_profiles": {},
        }
        failures = evaluate_strict_zero(report)
        assert failures == []


class TestRunAudit:
    @patch("scripts.legacy.maintenance.full_audit.create_engine")
    def test_basic(self, mock_create_engine):
        mock_conn = MagicMock()
        mock_conn.dialect.name = "sqlite"
        mock_conn.execute.return_value.scalar.return_value = 0
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_conn.execute.return_value.first.return_value = None
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine

        result = run_audit(db_url="sqlite:///")
        assert isinstance(result, dict)
        assert "ok" in result


class TestParseArgs:
    def test_defaults(self):
        with patch("sys.argv", ["prog"]):
            args = parse_args([])
            assert args.strict_zero is False
            assert args.write_artifacts is False
