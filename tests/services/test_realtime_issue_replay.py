from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.crawlers.realtime_issue_crawler import (
    MLBPARK_BULLPEN_TARGET_ID,
    NAVER_NEWS_TARGET_ID,
)
from src.services import crawl_replay_dispatcher as dispatcher_module
from src.services.crawl_replay_dispatcher import ReplayOutcome, build_default_dispatcher

ORIGINAL_RUN_ID = "original-run"
REPLAY_RUN_ID = "replay-run"


def _letter(target_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        crawler="realtime_issue",
        target_type="realtime_issue_source",
        target_id=target_id,
        season=None,
        game_id=None,
        source_url="https://source.example.test/page",
        original_run_id=ORIGINAL_RUN_ID,
    )


@pytest.mark.parametrize("target_id", [NAVER_NEWS_TARGET_ID, MLBPARK_BULLPEN_TARGET_ID])
def test_replay_retries_exact_source_and_uses_original_run_lineage(target_id: str) -> None:
    stored_outcome = ReplayOutcome(success=True, replay_run_id=REPLAY_RUN_ID, status="success")
    with (
        patch.object(dispatcher_module, "RealtimeIssueCrawler") as crawler_class,
        patch.object(dispatcher_module, "_outcome_from_persisted_run", return_value=stored_outcome),
    ):
        crawler_class.return_value.run = AsyncMock(return_value=[])

        outcome = dispatcher_module._replay_realtime_issue(_letter(target_id), REPLAY_RUN_ID)

    assert outcome is stored_outcome
    kwargs = crawler_class.return_value.run.await_args.kwargs
    assert kwargs["target_id"] == target_id
    assert kwargs["save"] is True
    assert kwargs["record_dead_letters"] is False
    assert kwargs["raise_on_persist_error"] is True
    spec = kwargs["run_spec"]
    assert spec.run_id == REPLAY_RUN_ID
    assert spec.parent_run_id == ORIGINAL_RUN_ID
    assert spec.replay_of_run_id == ORIGINAL_RUN_ID


def test_unknown_replay_target_fails_closed_without_crawling() -> None:
    with patch.object(dispatcher_module, "RealtimeIssueCrawler") as crawler_class:
        outcome = dispatcher_module._replay_realtime_issue(_letter("unknown"), REPLAY_RUN_ID)

    assert not outcome.success
    assert outcome.status == "unaddressable"
    crawler_class.assert_not_called()


def test_default_dispatcher_registers_realtime_issue_handler() -> None:
    dispatcher = build_default_dispatcher()

    assert "realtime_issue" in dispatcher.registered_crawlers()
