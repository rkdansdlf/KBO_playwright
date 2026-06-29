"""선수의 시즌별 주루 기록을 크롤링하고 DB에 저장합니다."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from typing import Any, Protocol

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Page, sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from src.constants import KST
from src.utils.playwright_blocking import install_sync_resource_blocking
from src.utils.playwright_retry import LONG_TIMEOUT
from src.utils.request_policy import RequestPolicy
from src.utils.team_codes import resolve_team_code
from src.utils.type_helpers import safe_float, safe_int

logger = logging.getLogger(__name__)

BASERUNNING_CRAWL_EXCEPTIONS = (
    PlaywrightError,
    PlaywrightTimeoutError,
    TimeoutError,
    RuntimeError,
    ValueError,
    OSError,
)
BASERUNNING_SAVE_EXCEPTIONS = (sqlite3.Error, ValueError, TypeError, OSError)


class _BaserunningCell(Protocol):
    def query_selector(self, selector: str) -> _BaserunningCell | None: ...

    def inner_text(self) -> str: ...

    def get_attribute(self, name: str) -> str | None: ...


class _BaserunningRow(Protocol):
    def query_selector_all(self, selector: str) -> list[_BaserunningCell]: ...


def crawl_baserunning_stats(
    year: int | None = None,
    max_retries: int = 3,
    timeout: int = LONG_TIMEOUT,
) -> list[dict[str, Any]]:
    """
    전체 선수의 주루 기록을 크롤링합니다.

    Args:
        year: Season year.
        max_retries: Max Retries.
        timeout: Timeout.
        year: 시즌 연도 (None이면 현재 연도)
        max_retries: 최대 재시도 횟수
        timeout: 페이지 로드 타임아웃 (밀리초)

    Returns:
        list: 주루 기록 리스트

    """
    if year is None:
        year = datetime.now(KST).year
    baserunning_data = []
    policy = RequestPolicy()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(**policy.build_context_kwargs())
        page = context.new_page()
        page.set_default_timeout(timeout)
        install_sync_resource_blocking(page)

        if not _load_baserunning_page(page, policy, max_retries, timeout):
            browser.close()
            return baserunning_data

        try:
            baserunning_data.extend(_parse_baserunning_page(page, year))

        except BASERUNNING_CRAWL_EXCEPTIONS:
            logger.exception("⚠️ 주루 기록 크롤링 중 오류")

        browser.close()

    return baserunning_data


def _load_baserunning_page(page: Page, policy: RequestPolicy, max_retries: int, timeout: int) -> bool:
    url = "https://www.koreabaseball.com/Record/Player/Runner/Basic.aspx"
    for attempt in range(max_retries):
        try:
            page.goto(url, wait_until="load", timeout=timeout)
            page.wait_for_load_state("networkidle", timeout=timeout)
        except BASERUNNING_CRAWL_EXCEPTIONS:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                logger.exception("   ⚠️  재시도 %s/%s (%s초 후 재시도)", attempt + 1, max_retries, wait_time)
                policy.delay()
            else:
                logger.exception("   ❌ 최대 재시도 횟수 초과")
        else:
            policy.delay()
            return True
    return False


def _parse_baserunning_page(page: Page, year: int) -> list[dict[str, Any]]:
    tables = page.query_selector_all("table")
    if not tables:
        return []
    tbody = tables[0].query_selector("tbody")
    rows = tbody.query_selector_all("tr") if tbody else []
    logger.info("   ✓ %s명의 주루 기록 발견", len(rows))
    return [stats for row in rows if (stats := _parse_baserunning_row(row, year))]


def _extract_baserunning_player(cells: list[_BaserunningCell]) -> tuple[str | None, str]:
    player_link = cells[1].query_selector("a")
    if not player_link:
        return None, cells[1].inner_text().strip()
    player_name = player_link.inner_text().strip()
    href = player_link.get_attribute("href")
    player_id = href.split("playerId=")[1].split("&")[0] if href and "playerId=" in href else None
    return player_id, player_name


def _parse_baserunning_row(row: _BaserunningRow, year: int) -> dict[str, Any] | None:
    cells = row.query_selector_all("td")
    if len(cells) < 10:
        return None
    player_name = "알 수 없음"
    try:
        player_id, player_name = _extract_baserunning_player(cells)
        team_name = cells[2].inner_text().strip()
        return {
            "player_id": player_id,
            "player_name": player_name,
            "team_id": resolve_team_code(team_name, year) or team_name,
            "year": year,
            "games": safe_int(cells[3].inner_text()),
            "stolen_base_attempts": safe_int(cells[4].inner_text()),
            "stolen_bases": safe_int(cells[5].inner_text()),
            "caught_stealing": safe_int(cells[6].inner_text()),
            "stolen_base_percentage": safe_float(cells[7].inner_text()),
            "out_on_base": safe_int(cells[8].inner_text()),
            "picked_off": safe_int(cells[9].inner_text()),
        }
    except (ValueError, AttributeError, IndexError) as e:
        logger.warning("   ⚠️  선수 데이터 파싱 오류 (%s): %s", player_name, e)
        return None


def save_baserunning_stats(
    player_list: list[dict[str, Any]],
    year: int | None = None,
    db_path: str | None = None,
) -> None:
    """
    Save baserunning stats.

    Args:
        player_list: Player List.
        year: Season year.
        db_path: Db file path.
        player_list: Player List.
        year: Season year.
        db_path: Db file path.

    """
    if year is None:
        year = datetime.now(KST).year
    if db_path is None:
        db_path = f"data/kbo_{year}.db"
    logger.info("\n%s", "=" * 60)
    logger.info("🏃 %s년 주루 기록 수집 시작", year)
    logger.info("%s\n", "=" * 60)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    baserunning_data = crawl_baserunning_stats(year)
    if not baserunning_data:
        logger.error("❌ 주루 기록을 가져올 수 없습니다.")
        conn.close()
        return

    player_map = {p["player_name"]: p["player_id"] for p in player_list}
    success_count = 0
    fail_count = 0

    for idx, stats in enumerate(baserunning_data, 1):
        player_id = _resolve_baserunning_player_id(
            stats,
            player_map,
            cursor,
            year,
        )
        if player_id:
            if _insert_baserunning_record(cursor, stats, player_id):
                conn.commit()
                success_count += 1
                if idx % 10 == 0:
                    logger.info("[%s/%s] %s 저장 완료", idx, len(baserunning_data), stats["player_name"])
            else:
                fail_count += 1
        else:
            fail_count += 1
            logger.warning("   ⚠️  %s: player_id를 찾을 수 없음", stats["player_name"])

    conn.close()
    _log_baserunning_summary(success_count, fail_count)


def _resolve_baserunning_player_id(
    stats: dict[str, Any],
    player_map: dict[str, object],
    cursor: sqlite3.Cursor,
    year: int,
) -> object:
    player_id = stats.get("player_id")
    if not player_id:
        player_id = player_map.get(stats["player_name"])
    if not player_id:
        cursor.execute(
            "SELECT player_id FROM player_season_participation WHERE player_name = ? AND year = ? AND team_id = ?",
            (stats["player_name"], year, stats["team_id"]),
        )
        row = cursor.fetchone()
        player_id = row[0] if row else None
    if not player_id:
        cursor.execute(
            "SELECT player_id FROM player_season_participation WHERE player_name = ? AND year = ? LIMIT 1",
            (stats["player_name"], year),
        )
        row = cursor.fetchone()
        player_id = row[0] if row else None
    return player_id


def _insert_baserunning_record(
    cursor: sqlite3.Cursor,
    stats: dict[str, Any],
    player_id: object,
) -> bool:
    try:
        cursor.execute(
            """INSERT OR REPLACE INTO kbo_season_baserunning_stats
(player_id, team_id, year, player_name, games, stolen_base_attempts, stolen_bases, caught_stealing, stolen_base_percentage, out_on_base, picked_off, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                player_id,
                stats["team_id"],
                stats["year"],
                stats["player_name"],
                stats["games"],
                stats["stolen_base_attempts"],
                stats["stolen_bases"],
                stats["caught_stealing"],
                stats["stolen_base_percentage"],
                stats["out_on_base"],
                stats["picked_off"],
                datetime.now(KST),
            ),
        )
    except BASERUNNING_SAVE_EXCEPTIONS:
        logger.exception("   ❌ %s 저장 실패", stats["player_name"])
        return False
    return True


def _log_baserunning_summary(success_count: int, fail_count: int) -> None:
    logger.info("\n%s", "=" * 60)
    logger.info("✅ 주루 기록 저장 완료!")
    logger.info("%s", "=" * 60)
    logger.info("  - 성공: %s명", success_count)
    logger.info("  - 실패: %s명", fail_count)
    logger.info("%s\n", "=" * 60)


if __name__ == "__main__":
    # 테스트용
    from player_list_crawler import crawl_player_list

    YEAR = datetime.now(KST).year
    players = crawl_player_list(YEAR)
    save_baserunning_stats(players, YEAR)
