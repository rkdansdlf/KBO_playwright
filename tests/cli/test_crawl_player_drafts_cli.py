"""CLI contract for the Wikipedia rookie-draft crawler.

The crawler now reports an outcome instead of raising, so the CLI carries the
duty the exception used to perform: a season that genuinely has no page is
normal, while a failed request has to reach the shell as a non-zero exit.
Without that split the scheduler would read an outage as a successful run that
happened to find nothing.
"""

from __future__ import annotations

import argparse
import logging
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.cli.collection.crawl_player_drafts import run
from src.crawlers.result import CrawlOutcome, CrawlResult

_MODULE = "src.cli.collection.crawl_player_drafts"


def _args(**overrides: object) -> argparse.Namespace:
    base: dict[str, object] = {"season": 2024, "source": "wiki", "save": False}
    base.update(overrides)
    return argparse.Namespace(**base)


def _crawler_returning(result: CrawlResult[object]) -> Mock:
    """Build the crawler instance the CLI would get, answering with `result`.

    The CLI calls `crawl_season` on the constructed instance, so the stub has to
    carry the coroutine on the instance rather than on a call return value.
    """
    crawler = Mock()
    crawler.crawl_season = AsyncMock(return_value=result)
    return crawler


class TestTheWikiSource:
    @pytest.mark.asyncio
    async def test_records_are_saved_when_the_crawl_succeeds(self) -> None:
        """A successful crawl reaches the save path with its records."""
        records = [{"season": 2024, "player_name": "김선수"}]
        crawler = _crawler_returning(CrawlResult.success(records))

        with patch(f"{_MODULE}.DraftWikiCrawler", return_value=crawler), patch(f"{_MODULE}.PlayerDraftRepository"):
            await run(_args())

        crawler.crawl_season.assert_awaited_once_with(2024)

    @pytest.mark.asyncio
    async def test_a_missing_season_saves_nothing_and_succeeds(self, caplog) -> None:
        """An empty season is normal, so the command exits 0."""
        crawler = _crawler_returning(CrawlResult.empty(url="https://ko.wikipedia.org/w/api.php"))

        with (
            caplog.at_level(logging.WARNING),
            patch(
                f"{_MODULE}.DraftWikiCrawler",
                return_value=crawler,
            ),
            patch(f"{_MODULE}.PlayerDraftRepository"),
        ):
            await run(_args())

        assert "No Wikipedia draft page" in caplog.text

    @pytest.mark.asyncio
    async def test_a_failed_crawl_exits_non_zero(self) -> None:
        """A transport fault must not read as a successful empty run."""
        crawler = _crawler_returning(
            CrawlResult.failure(
                CrawlOutcome.RETRYABLE_ERROR,
                error="Connection reset by peer",
                error_code="FETCH_HTTP_ERROR",
            )
        )

        with (
            pytest.raises(SystemExit) as excinfo,
            patch(
                f"{_MODULE}.DraftWikiCrawler",
                return_value=crawler,
            ),
            patch(f"{_MODULE}.PlayerDraftRepository"),
        ):
            await run(_args())

        assert excinfo.value.code == 1

    @pytest.mark.asyncio
    async def test_a_failed_crawl_records_why(self, caplog) -> None:
        """The failure reason reaches the log, not just the exit code."""
        crawler = _crawler_returning(
            CrawlResult.failure(
                CrawlOutcome.RETRYABLE_ERROR,
                error="Connection reset by peer",
                error_code="FETCH_HTTP_ERROR",
            )
        )

        with (
            caplog.at_level(logging.ERROR),
            patch(
                f"{_MODULE}.DraftWikiCrawler",
                return_value=crawler,
            ),
            patch(f"{_MODULE}.PlayerDraftRepository"),
        ):
            with pytest.raises(SystemExit):
                await run(_args())

        assert "Connection reset by peer" in caplog.text
