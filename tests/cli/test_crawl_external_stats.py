from __future__ import annotations

import os
import subprocess
import sys
from unittest.mock import AsyncMock, patch
from pathlib import Path

import pytest

from src.cli.crawl_external_stats import main
from src.crawlers.external_stats_crawler import ExternalCrawlResult


ROOT = Path(__file__).resolve().parents[2]


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


def test_rebuild_rankings_requires_one_provider() -> None:
    with pytest.raises(SystemExit, match="exactly one"):
        main(["--season", "2025", "--rebuild-rankings"])


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
