"""Tests for the read-only `kbo dlq` inspection commands."""

from __future__ import annotations

import contextlib
import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.cli.dlq import main as dlq_main
from src.cli.kbo import main as kbo_main
from src.services.crawl_dead_letter_stats import DlqStats


def _stats() -> DlqStats:
    return DlqStats(
        pending=3,
        due=1,
        retrying=2,
        stale_retrying=1,
        resolved=10,
        exhausted=4,
        ignored=1,
        oldest_pending_at=datetime(2026, 9, 26, 6, 0, 0),
        oldest_pending_age_seconds=3720.0,
        by_status_crawler={("pending", "awards"): 3, ("retrying", "schedule"): 2},
    )


def _letter() -> SimpleNamespace:
    return SimpleNamespace(
        dlq_id="dlq-0001",
        status="pending",
        crawler="awards",
        target_type="award_history",
        target_id="kbo_awards_yagoonara",
        failure_stage="fetch",
        error_code="FETCH_TIMEOUT",
        error_message="timeout",
        retry_count=2,
        max_retries=5,
        next_retry_at=datetime(2026, 9, 26, 6, 20, 0),
        original_run_id="run-a",
        replay_run_id="run-b",
        created_at=datetime(2026, 9, 26, 5, 0, 0),
        updated_at=datetime(2026, 9, 26, 6, 0, 0),
        resolved_at=None,
    )


@contextlib.contextmanager
def _fake_session():
    yield MagicMock()


def test_status_human_output(capsys) -> None:
    with patch("src.cli.dlq.collect_dlq_stats", return_value=_stats()):
        assert dlq_main(["status"]) == 0
    out = capsys.readouterr().out
    assert "pending" in out
    assert "due: 1" in out
    assert "1h 2m 0s" in out


def test_stats_json_output(capsys) -> None:
    with patch("src.cli.dlq.collect_dlq_stats", return_value=_stats()):
        assert dlq_main(["stats", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["pending"] == 3
    assert payload["by_status_crawler"]["pending:awards"] == 3


def test_list_json_output(capsys) -> None:
    repo = MagicMock()
    repo.list_recent.return_value = [_letter()]
    with (
        patch("src.cli.dlq.get_db_session", _fake_session),
        patch("src.cli.dlq.CrawlDeadLetterRepository", return_value=repo),
    ):
        assert dlq_main(["list", "--status", "pending", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload[0]["dlq_id"] == "dlq-0001"
    assert payload[0]["error_code"] == "FETCH_TIMEOUT"


def test_show_renders_lineage(capsys) -> None:
    letter_repo = MagicMock()
    letter_repo.get_by_dlq_id.return_value = _letter()
    run_repo = MagicMock()
    run_repo.get_by_run_ids.return_value = {
        "run-a": SimpleNamespace(
            run_id="run-a",
            crawler="awards",
            status="partial",
            started_at=datetime(2026, 9, 26, 5, 0, 0),
            finished_at=datetime(2026, 9, 26, 5, 1, 0),
            error_code="SOURCE_PARTIAL",
            error_message="partial",
            replay_of_run_id=None,
        ),
        "run-b": SimpleNamespace(
            run_id="run-b",
            crawler="awards",
            status="failed",
            started_at=datetime(2026, 9, 26, 6, 0, 0),
            finished_at=datetime(2026, 9, 26, 6, 1, 0),
            error_code="PERSIST_CONNECTION",
            error_message="down",
            replay_of_run_id="run-a",
        ),
    }
    with (
        patch("src.cli.dlq.get_db_session", _fake_session),
        patch("src.cli.dlq.CrawlDeadLetterRepository", return_value=letter_repo),
        patch("src.cli.dlq.CrawlExecutionRepository", return_value=run_repo),
    ):
        assert dlq_main(["show", "dlq-0001"]) == 0
    out = capsys.readouterr().out
    assert "original run" in out
    assert "run-a" in out
    assert "latest replay" in out
    assert "PERSIST_CONNECTION" in out


def test_show_unknown_returns_not_found(capsys) -> None:
    letter_repo = MagicMock()
    letter_repo.get_by_dlq_id.return_value = None
    with (
        patch("src.cli.dlq.get_db_session", _fake_session),
        patch("src.cli.dlq.CrawlDeadLetterRepository", return_value=letter_repo),
    ):
        assert dlq_main(["show", "missing"]) == 1
    assert "not found" in capsys.readouterr().out


def test_master_cli_dispatches_dlq(capsys) -> None:
    with patch("src.cli.dlq.collect_dlq_stats", return_value=_stats()):
        assert kbo_main(["dlq", "status"]) == 0
    assert "DLQ status" in capsys.readouterr().out
