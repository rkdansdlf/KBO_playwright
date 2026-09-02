from __future__ import annotations

import os
import subprocess
import sys
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path

import pytest

from src.cli.crawl_external_stats import (
    DEFAULT_PROVIDERS,
    Options,
    _crawl,
    _persist_result,
    main,
)
from src.crawlers.external_stats_crawler import ExternalCrawlResult, FetchedExternalPage
from src.repositories.external_season_stats_repository import (
    ExternalStatsProjectionReport,
    ExternalStatsSaveReport,
)
from src.sources.stats.base import ExternalStatRecord


ROOT = Path(__file__).resolve().parents[2]


def _record() -> ExternalStatRecord:
    return ExternalStatRecord(
        provider="fangraphs",
        source_key="fangraphs_kbo_batting",
        stat_type="batting",
        season=2025,
        player_name="김타자",
        team_name="두산",
        team_code="DB",
        external_player_id="123",
        metrics={"war": 3.2},
        source_url="https://example.test/stats",
        metric_metadata={"parser_version": "test-v1"},
    )


@pytest.mark.asyncio
async def test_crawl_uses_default_providers_and_closes_crawler() -> None:
    result = ExternalCrawlResult(records=[], pages=[], failures=[])
    crawler = MagicMock()
    crawler.crawl = AsyncMock(return_value=result)
    crawler.close = AsyncMock()

    with patch("src.cli.collection.crawl_external_stats.ExternalStatsCrawler", return_value=crawler):
        assert await _crawl(2025, []) is result

    crawler.crawl.assert_awaited_once_with(2025, providers=DEFAULT_PROVIDERS)
    crawler.close.assert_awaited_once_with()


def test_persist_saves_records_snapshots_projections_and_rankings() -> None:
    result = ExternalCrawlResult(
        records=[_record()],
        pages=[
            FetchedExternalPage(
                source_key="fangraphs_kbo_batting",
                url="https://example.test/stats",
                body="{}",
                status_code=200,
                content_hash="a" * 64,
                content_type="application/json",
            ),
        ],
        failures=[],
    )
    session = MagicMock()
    session.__enter__.return_value = session
    session.__exit__.return_value = False
    repository = MagicMock()
    repository.save_records.return_value = ExternalStatsSaveReport(attempted=1, saved=1, resolved=1)
    repository.project.return_value = ExternalStatsProjectionReport(considered=1, projected=1)

    with (
        patch("src.db.engine.SessionLocal", return_value=session),
        patch(
            "src.repositories.external_season_stats_repository.ExternalSeasonStatsRepository",
            return_value=repository,
        ),
        patch("src.repositories.source_registry_repository.save_raw_snapshots", return_value=1) as save_raw,
        patch("src.cli.calculate_rankings.rebuild_rankings", return_value=3) as rebuild,
    ):
        _persist_result(
            result,
            Options(
                ["fangraphs"],
                season=2025,
                save=True,
                project=True,
                rebuild_rankings=True,
                capture_raw=True,
            ),
        )

    repository.save_records.assert_called_once_with(
        result.records,
        content_hashes={"fangraphs_kbo_batting": "a" * 64},
    )
    repository.project.assert_called_once_with(provider="fangraphs", season=2025)
    save_raw.assert_called_once()
    assert save_raw.call_args.args[1][0]["parse_status"] == "done"
    rebuild.assert_called_once_with(2025, external_provider="fangraphs")
    session.commit.assert_called_once_with()


def test_dry_run_does_not_persist() -> None:
    result = ExternalCrawlResult(records=[], pages=[], failures=[])
    with (
        patch("src.cli.crawl_external_stats._crawl", new=AsyncMock(return_value=result)),
        patch("src.cli.crawl_external_stats._persist_result") as persist,
    ):
        assert main(["--season", "2025", "--provider", "fangraphs", "--dry-run"]) == 0

    options = persist.call_args.args[1]
    assert options.save is False
    assert options.providers == ["fangraphs"]


def test_project_requires_save() -> None:
    with pytest.raises(SystemExit, match="--project requires --save"):
        main(["--season", "2025", "--provider", "fangraphs", "--project"])


def test_capture_raw_requires_save() -> None:
    with pytest.raises(SystemExit, match="--capture-raw requires --save"):
        main(["--season", "2025", "--provider", "fangraphs", "--capture-raw"])


def test_rebuild_rankings_requires_one_provider() -> None:
    with pytest.raises(SystemExit, match="exactly one"):
        main(["--season", "2025", "--rebuild-rankings"])


def test_crawl_failure_returns_nonzero() -> None:
    result = ExternalCrawlResult(records=[], pages=[], failures=["fangraphs/batting: blocked"])
    with (
        patch("src.cli.crawl_external_stats._crawl", new=AsyncMock(return_value=result)),
        patch("src.cli.crawl_external_stats._persist_result"),
    ):
        assert main(["--season", "2025", "--provider", "fangraphs", "--dry-run"]) == 1


def test_dry_run_module_import_does_not_require_database_url() -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = "not-a-sqlalchemy-url"
    env.pop("RAG_TEST_DB_URL", None)
    result = subprocess.run(
        [sys.executable, "-c", "import src.cli.crawl_external_stats; print('ok')"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
