from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

REQUIRED_TEST_ASSETS = (
    Path("tests/fixtures/game_details/20251001NCLG0.html"),
    Path("tests/fixtures/html/hh_events_notice.html"),
    Path("tests/fixtures/html/lg_events_notice.html"),
    Path("tests/fixtures/html/lg_ticket_prices.html"),
    Path("tests/fixtures/html/ob_events_notice.html"),
    Path("tests/fixtures/html/ssg_ticket_prices.html"),
    Path("tests/fixtures/html/team_batting_2023.html"),
    Path("tests/fixtures/html/team_pitching_2023.html"),
    Path("tests/fixtures/kbo_live_text/20260412_SKLG.html"),
    Path("tests/fixtures/naver_live/relay_inning_1.json"),
    Path("tests/fixtures/naver_live/schedule_today.json"),
    Path("tests/fixtures/naver_result/relay_inning_9.json"),
    Path("tests/fixtures/naver_result/schedule_result.json"),
    Path("data/seed/team_rivalries.csv"),
)


def test_required_test_assets_exist():
    missing = [str(path) for path in REQUIRED_TEST_ASSETS if not (ROOT / path).exists()]
    assert missing == []


def test_required_test_assets_are_not_gitignored():
    result = subprocess.run(
        ["git", "check-ignore", "--no-index", *map(str, REQUIRED_TEST_ASSETS)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1, result.stdout or result.stderr
