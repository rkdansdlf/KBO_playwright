"""
KBO 전체 시리즈 타자 기록 크롤러
- 정규시즌, 시범경기, 와일드카드, 준플레이오프, 플레이오프, 한국시리즈.

Usage:
    # 2025년 모든 시리즈 크롤링
    python -m src.crawlers.player_batting_all_series_crawler --year 2025 --save

    # 특정 시리즈만
    python -m src.crawlers.player_batting_all_series_crawler --year 2025 --series regular --save
    python -m src.crawlers.player_batting_all_series_crawler --year 2025 --series exhibition --save

"""

from __future__ import annotations

import argparse
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from playwright.sync_api import ElementHandle, Page, sync_playwright
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from sqlalchemy.exc import SQLAlchemyError

from src.aggregators.season_stat_aggregator import SeasonStatAggregator
from src.constants import KST
from src.db.engine import SessionLocal
from src.models.game import Game, GameBattingStat
from src.models.player import PlayerBasic
from src.models.season import KboSeason
from src.repositories.safe_batting_repository import save_batting_stats_safe
from src.urls import HITTER_BASIC1
from src.utils.compliance import compliance
from src.utils.fallback_monitor import FallbackMonitor
from src.utils.player_season_stat_validation import filter_valid_season_stat_payloads
from src.utils.player_stats_helpers import extract_rows_fast
from src.utils.playwright_blocking import install_sync_resource_blocking
from src.utils.playwright_retry import NAV_TIMEOUT, SEL_TIMEOUT, retry_navigation, retry_wait_for_selector
from src.utils.request_policy import RequestPolicy
from src.utils.team_codes import resolve_team_code
from src.utils.team_mapping import get_team_code, get_team_mapping_for_year

logger = logging.getLogger(__name__)


@dataclass
class BattingCrawlContext:
    """BattingCrawlContext class."""

    page: Page
    year: int
    series_key: str
    iteration_targets: list[dict]
    by_team: bool
    limit: int | None
    policy: RequestPolicy
    unique_players: set[int]
    all_players_data: list[dict]


@dataclass
class BattingRowData:
    """BattingRowData class."""

    cells: list[str]
    player_id: int
    player_name: str
    team_code: str
    series_key: str
    is_basic2: bool
    year: int | None = None


@dataclass
class LegacyRowContext:
    """LegacyRowContext class."""

    row_idx: int
    row: ElementHandle
    current_header: str
    description: str
    year: int
    team_mapping: dict[str, str]


CRAWLER_EXCEPTIONS = (
    PlaywrightError,
    PlaywrightTimeoutError,
    RuntimeError,
    ValueError,
    TypeError,
    AttributeError,
    OSError,
)
DB_SAVE_EXCEPTIONS = (*CRAWLER_EXCEPTIONS, SQLAlchemyError)


def get_series_mapping() -> dict[str, dict[str, str]]:
    """시리즈 이름과 선택 값 매핑 (실제 페이지에서 확인된 값)."""
    return {
        "regular": {"name": "KBO 정규시즌", "value": "0", "league": "REGULAR"},
        "exhibition": {"name": "KBO 시범경기", "value": "1", "league": "EXHIBITION"},
        "wildcard": {"name": "KBO 와일드카드", "value": "4", "league": "WILDCARD"},
        "semi_playoff": {"name": "KBO 준플레이오프", "value": "3", "league": "SEMI_PLAYOFF"},
        "playoff": {"name": "KBO 플레이오프", "value": "5", "league": "PLAYOFF"},
        "korean_series": {"name": "KBO 한국시리즈", "value": "7", "league": "KOREAN_SERIES"},
    }


def safe_parse_number(value_str: str, data_type: type, *, _allow_zero: bool = True) -> int | float | None:
    """
    안전하게 숫자를 파싱하는 함수.

    Args:
        value_str: Value Str.
        data_type: Data Type.
        _allow_zero: Allow Zero.
        value_str: 파싱할 문자열
        data_type: 변환할 데이터 타입 (int 또는 float)
        allow_zero: 0 값을 허용할지 여부

    Returns:
        파싱된 숫자 또는 None

    """
    if not value_str:
        return None

    value_str = value_str.strip()

    # 빈 문자열, "-", "N/A" 등은 None으로 처리
    if not value_str or value_str in ["-", "N/A", ""]:
        return None

    try:
        return data_type(value_str)
    except (ValueError, TypeError):
        return None


def _extract_player_id_from_href(href: str | None) -> int | None:
    if not href:
        return None
    match = re.search(r"playerId=(\d+)", href)
    return int(match.group(1)) if match else None


def _is_basic2_headers(headers: list[str]) -> bool:
    basic2_indicators = ["BB", "볼넷", "IBB", "HBP", "SLG", "OBP", "OPS"]
    combined = "".join(headers)
    return any(indicator in combined for indicator in basic2_indicators)


def _build_batting_data(ctx: BattingRowData) -> dict[str, Any]:
    year = ctx.year or datetime.now(KST).year
    series_map = get_series_mapping()
    league_name = series_map.get(ctx.series_key, {}).get("league", "REGULAR")

    def cell(idx: int) -> str | None:
        """
        Handle the cell operation.

        Args:
            idx: Idx.
            idx: Idx.

        Returns:
            The result of the operation.

        """
        return ctx.cells[idx] if len(ctx.cells) > idx else None

    if ctx.series_key == "regular":
        if ctx.is_basic2:
            return {
                "player_id": ctx.player_id,
                "player_name": ctx.player_name,
                "team_code": ctx.team_code,
                "season": year,
                "league": league_name,
                "avg": safe_parse_number(cell(3), float),
                "walks": safe_parse_number(cell(4), int),
                "intentional_walks": safe_parse_number(cell(5), int),
                "hbp": safe_parse_number(cell(6), int),
                "strikeouts": safe_parse_number(cell(7), int),
                "gdp": safe_parse_number(cell(8), int),
                "slg": safe_parse_number(cell(9), float),
                "obp": safe_parse_number(cell(10), float),
                "ops": safe_parse_number(cell(11), float),
                "extra_stats": {
                    "multi_hits": safe_parse_number(cell(12), int),
                    "risp_avg": safe_parse_number(cell(13), float),
                    "pinch_hit_avg": safe_parse_number(cell(14), float),
                },
            }
        return {
            "player_id": ctx.player_id,
            "player_name": ctx.player_name,
            "team_code": ctx.team_code,
            "season": year,
            "league": league_name,
            "avg": safe_parse_number(cell(3), float),
            "games": safe_parse_number(cell(4), int),
            "plate_appearances": safe_parse_number(cell(5), int),
            "at_bats": safe_parse_number(cell(6), int),
            "runs": safe_parse_number(cell(7), int),
            "hits": safe_parse_number(cell(8), int),
            "doubles": safe_parse_number(cell(9), int),
            "triples": safe_parse_number(cell(10), int),
            "home_runs": safe_parse_number(cell(11), int),
            "total_bases": safe_parse_number(cell(12), int),
            "rbi": safe_parse_number(cell(13), int),
            "sacrifice_hits": safe_parse_number(cell(14), int),
            "sacrifice_flies": safe_parse_number(cell(15), int),
        }

    return {
        "player_id": ctx.player_id,
        "player_name": ctx.player_name,
        "team_code": ctx.team_code,
        "season": year,
        "league": league_name,
        "avg": safe_parse_number(cell(3), float),
        "games": safe_parse_number(cell(4), int),
        "plate_appearances": safe_parse_number(cell(5), int),
        "at_bats": safe_parse_number(cell(6), int),
        "hits": safe_parse_number(cell(7), int),
        "doubles": safe_parse_number(cell(8), int),
        "triples": safe_parse_number(cell(9), int),
        "home_runs": safe_parse_number(cell(10), int),
        "rbi": safe_parse_number(cell(11), int),
        "stolen_bases": safe_parse_number(cell(12), int),
        "caught_stealing": safe_parse_number(cell(13), int),
        "walks": safe_parse_number(cell(14), int),
        "hbp": safe_parse_number(cell(15), int),
        "strikeouts": safe_parse_number(cell(16), int),
        "gdp": safe_parse_number(cell(17), int),
        "extra_stats": {"errors": safe_parse_number(cell(18), int)},
    }


def _parse_batting_stats_table_fast(page: Page, series_key: str, year: int | None = None) -> list[dict]:
    """
    Parse batting table using JS extraction for reduced RPC.

    Args:
        page: Page.
        series_key: Series Key.
        year: Season year.

    """
    year = year or datetime.now(KST).year

    get_team_mapping_for_year(year)

    extraction_script = r"""
    () => {
        const table = document.querySelector('table.tData01.tt');
        if (!table) return null;
        const rows = table.querySelectorAll('tbody tr');
        if (rows.length === 0) return null;

        const headers = Array.from(document.querySelectorAll('table thead th')).map(th => th.textContent.trim());
        const basic2_indicators = ['BB', '볼넷', 'IBB', 'HBP', 'SLG', 'OBP', 'OPS'];
        const is_basic2 = basic2_indicators.some(ind => headers.join('').includes(ind));

        const results = [];
        rows.forEach(row => {
            const cells = Array.from(row.querySelectorAll('td'));
            if (cells.length < 10) return;

            const nameLink = cells[1].querySelector('a');
            if (!nameLink) return;

            const playerName = nameLink.textContent.trim();
            const href = nameLink.getAttribute('href');
            const idMatch = href ? href.match(/playerId=(\d+)/) : null;
            if (!idMatch) return;
            const playerId = parseInt(idMatch[1], 10);

            const teamName = cells[2].textContent.trim();
            results.push({
                player_id: playerId,
                player_name: playerName,
                team_name: teamName,
                raw_cells: cells.map(c => c.textContent.trim()),
            });
        });
        return { is_basic2, results };
    }
    """

    try:
        js_result = page.evaluate(extraction_script)
        if not js_result or not isinstance(js_result, dict) or not js_result.get("results"):
            return []

        extracted_rows = js_result["results"]
        is_basic2 = js_result.get("is_basic2", False)

        players_data = []
        for row in extracted_rows:
            player_id = row["player_id"]
            player_name = row["player_name"]
            team_name = row["team_name"]
            cells = row["raw_cells"]

            team_code = resolve_team_code(team_name, year) or team_name

            batting_data = _build_batting_data(
                BattingRowData(
                    cells=cells,
                    player_id=player_id,
                    player_name=player_name,
                    team_code=team_code,
                    series_key=series_key,
                    is_basic2=is_basic2,
                    year=year,
                ),
            )
            players_data.append(batting_data)

    except CRAWLER_EXCEPTIONS:
        logger.exception("❌ 테이블 파싱 오류 (JS)")
        return []
    else:
        return players_data


def _parse_batting_stats_table_legacy(page: Page, series_key: str, year: int | None = None) -> list[dict]:
    year = year or datetime.now(KST).year
    get_team_mapping_for_year(year)
    try:
        table = page.query_selector("table")
        if not table:
            return []

        thead = table.query_selector("thead")
        headers = []
        if thead:
            header_cells = thead.query_selector_all("th")
            headers = [cell.text_content().strip() for cell in header_cells]
        is_basic2 = _is_basic2_headers(headers) if headers else False

        tbody = table.query_selector("tbody")
        rows = tbody.query_selector_all("tr") if tbody else table.query_selector_all("tr")
        if not rows:
            return []

        players_data = []
        for row in rows:
            cell_nodes = row.query_selector_all("td")
            if len(cell_nodes) < 10:
                continue

            cells = [cell.text_content().strip() for cell in cell_nodes]
            name_link = cell_nodes[1].query_selector("a")
            href = name_link.get_attribute("href") if name_link else None
            player_id = _extract_player_id_from_href(href)
            if not player_id:
                continue

            player_name = name_link.text_content().strip() if name_link else (cells[1] if len(cells) > 1 else "")
            team_name = cells[2] if len(cells) > 2 else ""
            team_code = resolve_team_code(team_name, year) or team_name

            batting_data = _build_batting_data(
                BattingRowData(
                    cells=cells,
                    player_id=player_id,
                    player_name=player_name,
                    team_code=team_code,
                    series_key=series_key,
                    is_basic2=is_basic2,
                    year=year,
                ),
            )
            players_data.append(batting_data)

    except CRAWLER_EXCEPTIONS:
        logger.exception("❌ 테이블 파싱 오류 (Legacy)")
        return []
    else:
        return players_data


def parse_batting_stats_table(
    page: Page,
    series_key: str,
    year: int | None = None,
    *,
    use_fast: bool | None = None,
) -> list[dict]:
    """
    Parse batting stats table.

    Args:
        page: Page.
        series_key: Series Key.
        year: Season year.
        use_fast: Use Fast.
        page: Page.
        series_key: Series Key.
        year: Season year.

    Returns:
        List of results.

    """
    year = year or datetime.now(KST).year

    if use_fast is None:
        use_fast = os.getenv("KBO_FAST_PARSE", "1") != "0"
    if use_fast:
        return _parse_batting_stats_table_fast(page, series_key, year)
    return _parse_batting_stats_table_legacy(page, series_key, year)


def build_batting_crawl_summary(rows: list[dict]) -> tuple[dict[str, object], list[dict]]:
    """
    Build batting summary.

    Args:
        rows: Rows.
        rows: Rows.

    Returns:
        Tuple result.

    """
    valid_rows, failure_counts = filter_valid_season_stat_payloads(rows, stat_type="batting")

    summary = {
        "processed_rows": len(rows),
        "valid_rows": len(valid_rows),
        "filtered_rows": len(rows) - len(valid_rows),
        "failure_counts": dict(failure_counts),
    }
    return summary, valid_rows


def go_to_next_page(page: Page, current_page_num: int, policy: RequestPolicy | None = None) -> bool:
    """
    다음 페이지로 이동 (1→2,3,4,5→다음→6,7,8,9,10→다음 반복).

    Args:
        page: Page.
        current_page_num: Current Page Num.
        policy: Policy.

    """
    try:
        if current_page_num % 5 == 0:  # 5페이지마다 "다음" 버튼 클릭
            selector = 'a[href*="btnNext"]'
            desc = f"다음 버튼 클릭 ({current_page_num}페이지 후)"
        else:  # 개별 페이지 번호 클릭
            next_page_num = current_page_num + 1
            relative_page_num = ((next_page_num - 1) % 5) + 1
            selector = f'a[href*="btnNo{relative_page_num}"]'
            desc = f"{next_page_num}페이지로 이동 (btnNo{relative_page_num})"

        # 빠른 종료: pagination 컨테이너 자체가 없으면 마지막 페이지
        paging = page.query_selector('td[id*="paging"]')
        if not paging:
            return False

        # 1회만 확인 (리로드 없음)
        btn = page.query_selector(selector)
        if not btn or btn.get_attribute("disabled") or "disabled" in (btn.get_attribute("class") or ""):
            return False

        if policy:
            policy.delay()

        page.click(selector, timeout=SEL_TIMEOUT)
        page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)
    except CRAWLER_EXCEPTIONS:
        logger.exception("❌ 페이지 이동 실패 (%sp -> next)", current_page_num)
        return False
    else:
        logger.info("➡️ %s", desc)
        return True


def _select_year_option(page: Page, year: int, policy: RequestPolicy | None) -> None:
    try:
        season_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason"]'
        if not retry_wait_for_selector(page, season_selector):
            logger.warning("   ⚠️ 연도 선택기를 찾을 수 없습니다.")
        else:
            if policy:
                policy.delay()
            page.select_option(season_selector, str(year))
            page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)
    except CRAWLER_EXCEPTIONS:
        logger.exception("   ⚠️ 연도 선택 중 오류 (무시)")


def _select_series_option(page: Page, series_value: str, policy: RequestPolicy | None) -> None:
    try:
        series_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries"]'
        if retry_wait_for_selector(page, series_selector):
            if policy:
                policy.delay()
            page.select_option(series_selector, value=series_value)
            page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)
    except CRAWLER_EXCEPTIONS:
        logger.exception("   ⚠️ 시리즈 선택 중 오류 (무시)")


def _navigate_to_basic2(page: Page, policy: RequestPolicy | None) -> bool:
    try:
        next_link_selector = 'a[href="/Record/Player/HitterBasic/Basic2.aspx"]'
        if retry_wait_for_selector(page, next_link_selector):
            if policy:
                policy.delay()
            page.click(next_link_selector)
            page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)
            success = True
        else:
            logger.error("   ❌ Basic2 이동 링크를 찾을 수 없습니다.")
            success = False
    except CRAWLER_EXCEPTIONS:
        logger.exception("   ❌ Basic2 이동 중 오류")
        return False
    else:
        return success


def _collect_basic2_pages(
    page: Page,
    year: int,
    all_player_data: dict[int, dict],
    policy: RequestPolicy | None = None,
) -> None:
    page_num = 1
    while True:
        if not retry_wait_for_selector(page, "table.tData01.tt thead th", timeout=SEL_TIMEOUT):
            logger.warning("   ⚠️ %s페이지 테이블 헤더 로딩 실패", page_num)
            break

        current_page_data = parse_batting_stats_table(page, "regular", year)
        for player_stat in current_page_data:
            pid = player_stat["player_id"]
            if pid not in all_player_data:
                all_player_data[pid] = player_stat
            else:
                all_player_data[pid].update(player_stat)

        if not go_to_next_page(page, page_num, policy):
            break
        page_num += 1


def crawl_basic2_with_headers(
    page: Page,
    year: int,
    series_info: dict,
    policy: RequestPolicy | None = None,
) -> dict[int, dict]:
    """
    정규시즌용 Basic2 페이지에서 각 헤더를 클릭하여 고급 통계 데이터 수집.

    Args:
        page: Page.
        year: Season year.
        series_info: Series Info.
        policy: Policy.

    """
    all_player_data = {}

    try:
        logger.info("   🔍 Basic2 접근을 위해 Basic1에서 시작...")

        url = HITTER_BASIC1
        if policy:
            policy.delay()
        if not retry_navigation(page, url, timeout=45000):
            logger.error("   ❌ Basic1 페이지 로딩 실패")
            return {}

        _select_year_option(page, year, policy)
        _select_series_option(page, series_info["value"], policy)

        if not _navigate_to_basic2(page, policy):
            return {}

        _collect_basic2_pages(page, year, all_player_data, policy)
        logger.info("   ✅ Basic2 전체 수집 완료: %s명", len(all_player_data))

    except CRAWLER_EXCEPTIONS:
        logger.exception("   ❌ Basic2 크롤링 중 오류")

    return all_player_data


def _extract_basic2_stat_by_header(
    current_header: str,
    cells: list[str],
    batting_data: dict[str, Any],
) -> None:
    """
    Basic2 테이블의 헤더(BB, IBB, HBP 등)에 맞춰 데이터를 파싱하여 batting_data에 추가합니다.

    Args:
        current_header: Current Header.
        cells: Cells.
        batting_data: Batting Data.

    """
    mapping = {
        "BB": (4, int, "walks"),
        "IBB": (5, int, "intentional_walks"),
        "HBP": (6, int, "hbp"),
        "SO": (7, int, "strikeouts"),
        "GDP": (8, int, "gdp"),
        "SLG": (9, float, "slg"),
        "OBP": (10, float, "obp"),
        "OPS": (11, float, "ops"),
    }

    if current_header in mapping:
        idx, data_type, key = mapping[current_header]
        if len(cells) > idx:
            batting_data[key] = safe_parse_number(cells[idx], data_type)
        return

    extra_mapping = {
        "MH": (12, int, "multi_hits"),
        "RISP": (13, float, "risp_avg"),
        "PH-BA": (14, float, "pinch_hit_avg"),
    }

    if current_header in extra_mapping:
        idx, data_type, key = extra_mapping[current_header]
        if len(cells) > idx:
            batting_data.setdefault("extra_stats", {})
            batting_data["extra_stats"][key] = safe_parse_number(cells[idx], data_type)


def _log_debug_legacy_table(page: Page, rows: list, description: str) -> None:
    thead = page.query_selector("thead")
    if thead:
        header_cells = thead.query_selector_all("th")
        headers = [cell.text_content().strip() for cell in header_cells]
        logger.info("      🔍 %s 기준 테이블 헤더: %s", description, headers)

    if len(rows) > 0:
        first_row_cells = rows[0].query_selector_all("td")
        logger.info("      🔍 %s 기준 첫 행 데이터 (%s개 컬럼):", description, len(first_row_cells))
        for i, cell in enumerate(first_row_cells[:10]):
            content = cell.text_content().strip()
            logger.info("         [%s]: '%s'", i, content)


def _log_first_rows_basic2_legacy(
    row_idx: int,
    player_name: str,
    team_name: str,
    current_header: str,
    batting_data: dict[str, Any],
) -> None:
    if row_idx < 3:
        sort_value = "N/A"
        if current_header in ["BB", "IBB", "HBP", "SO", "GDP", "SLG", "OBP", "OPS"]:
            sort_value = batting_data.get(current_header.lower(), "N/A")
        elif current_header in ["MH", "RISP", "PH-BA"]:
            sort_value = batting_data.get("extra_stats", {}).get(
                current_header.lower().replace("-", "_"),
                "N/A",
            )
        logger.info("      ✅ %s (%s) - %s: %s", player_name, team_name, current_header, sort_value)


def _parse_legacy_row(ctx: LegacyRowContext) -> tuple[int, dict] | None:
    cells = ctx.row.query_selector_all("td")
    if len(cells) < 5:
        return None

    try:
        name_cell = cells[1]
        name_link = name_cell.query_selector("a")
        if not name_link:
            return None

        player_name = name_link.text_content().strip()
        href = name_link.get_attribute("href")
        player_id = _extract_player_id_from_href(href)
        if not player_id:
            return None

        team_name = cells[2].text_content().strip()
        team_code = get_team_code(team_name, ctx.year)
        if not team_code:
            team_code = ctx.team_mapping.get(team_name, team_name)
            logger.warning("⚠️ %s년 '%s' 팀 매핑 실패, 폴백: %s", ctx.year, team_name, team_code)

        batting_data = {
            "player_id": player_id,
            "player_name": player_name,
            "team_code": team_code,
        }

        cell_texts = [c.text_content().strip() for c in cells]
        _extract_basic2_stat_by_header(ctx.current_header, cell_texts, batting_data)

        _log_first_rows_basic2_legacy(ctx.row_idx, player_name, team_name, ctx.current_header, batting_data)
    except (ValueError, AttributeError):
        logger.exception("      ⚠️ %s 행 파싱 오류", ctx.description)
        return None
    else:
        return player_id, batting_data


def _parse_basic2_header_data_legacy(
    page: Page,
    current_header: str,
    description: str,
    year: int | None = None,
) -> dict[int, dict]:
    """
    Basic2 페이지에서 특정 헤더 클릭 후 데이터 파싱
    각 헤더 클릭시 해당 기준으로 정렬된 선수 데이터를 수집.

    Args:
        page: Page.
        current_header: Current Header.
        description: Description.
        year: Season year.

    """
    year = year or datetime.now(KST).year

    players_data = {}
    team_mapping = get_team_mapping_for_year(year)

    try:
        table = page.query_selector("table")
        if not table:
            return players_data

        tbody = table.query_selector("tbody")
        rows = tbody.query_selector_all("tr") if tbody else table.query_selector_all("tr")

        if len(rows) == 0:
            return players_data

        _log_debug_legacy_table(page, rows, description)

        for row_idx, row in enumerate(rows):
            res = _parse_legacy_row(
                LegacyRowContext(
                    row_idx=row_idx,
                    row=row,
                    current_header=current_header,
                    description=description,
                    year=year,
                    team_mapping=team_mapping,
                ),
            )
            if res:
                player_id, batting_data = res
                players_data[player_id] = batting_data

    except CRAWLER_EXCEPTIONS:
        logger.exception("      ❌ %s 테이블 파싱 오류", description)

    return players_data


def _log_debug_fast_table(rows_data: list[dict], description: str, thead_node: ElementHandle | None) -> None:
    if thead_node:
        header_cells = thead_node.query_selector_all("th")
        headers = [cell.text_content().strip() for cell in header_cells]
        logger.info("      🔍 %s 기준 테이블 헤더: %s", description, headers)

    if rows_data:
        first_row = rows_data[0]
        cells = first_row.get("cells") or []
        logger.info("      🔍 %s 기준 첫 행 데이터 (%s개 컬럼):", description, len(cells))
        for idx, value in enumerate(cells[:10]):
            logger.info("         [%s]: '%s'", idx, value)


def _parse_fast_row(
    row: dict,
    current_header: str,
    year: int,
    team_mapping: dict[str, str],
) -> tuple[int, dict] | None:
    cells = row.get("cells") or []
    if len(cells) < 5:
        return None

    href = row.get("linkHref")
    player_id = _extract_player_id_from_href(href)
    if not player_id:
        return None

    player_name = (row.get("linkText") or (cells[1] if len(cells) > 1 else "")).strip()
    team_name = cells[2] if len(cells) > 2 else ""
    team_code = get_team_code(team_name, year)
    if not team_code:
        team_code = team_mapping.get(team_name, team_name)

    batting_data = {
        "player_id": player_id,
        "player_name": player_name,
        "team_code": team_code,
    }

    _extract_basic2_stat_by_header(current_header, cells, batting_data)
    return player_id, batting_data


def _parse_basic2_header_data_fast(
    page: Page,
    current_header: str,
    description: str,
    year: int | None = None,
) -> dict[int, dict]:
    year = year or datetime.now(KST).year
    players_data: dict[int, dict] = {}
    team_mapping = get_team_mapping_for_year(year)

    rows_data = extract_rows_fast(page)
    if not rows_data:
        return players_data

    thead = page.query_selector("thead")
    _log_debug_fast_table(rows_data, description, thead)

    for row in rows_data:
        res = _parse_fast_row(row, current_header, year, team_mapping)
        if res:
            player_id, batting_data = res
            players_data[player_id] = batting_data

    return players_data


def parse_basic2_header_data(
    page: Page,
    current_header: str,
    description: str,
    year: int | None = None,
    *,
    use_fast: bool | None = None,
) -> dict[int, dict]:
    """
    Parse basic2 header data.

    Args:
        page: Page.
        current_header: Current Header.
        description: Description.
        year: Season year.
        use_fast: Use Fast.
        page: Page.
        current_header: Current Header.
        description: Description.
        year: Season year.

    Returns:
        Dictionary result.

    """
    year = year or datetime.now(KST).year

    if use_fast is None:
        use_fast = os.getenv("KBO_FAST_PARSE", "1") != "0"
    if use_fast:
        return _parse_basic2_header_data_fast(page, current_header, description, year)
    return _parse_basic2_header_data_legacy(page, current_header, description, year)


# ---------------------------------------------------------------------------
# Fallback logic
# ---------------------------------------------------------------------------


def fallback_batting_from_db(year: int, series_key: str, reason: str = "Manual Trigger") -> list[dict]:
    """
    KBO 페이지 장애 시 로컬 DB의 상세 기록을 합산하여 타자 시즌 기록을 생성합니다.

    Args:
        year: Season year.
        series_key: Series Key.
        reason: Reason.

    """
    FallbackMonitor.log_fallback(year, series_key, "BATTING", reason)

    logger.info("🔄 로컬 DB 기반 타자 기록 집계 시작 (연도: %s, 시리즈: %s)...", year, series_key)
    all_players_data = []

    with SessionLocal() as session:
        # 1. 벌크 집계 데이터 가져오기
        bulk_stats = SeasonStatAggregator.aggregate_batting_season_bulk(session, year, series_key)
        if not bulk_stats:
            logger.info("✅ DB 집계 완료: 총 0명")
            return []

        player_ids = [s["player_id"] for s in bulk_stats if s.get("player_id")]
        logger.info("🔍 DB에서 %s명의 타자를 발견했습니다.", len(player_ids))

        # 2. 선수 기본 정보 벌크 로드
        players = (
            session.query(PlayerBasic.player_id, PlayerBasic.name).filter(PlayerBasic.player_id.in_(player_ids)).all()
        )
        player_name_map = {p.player_id: p.name for p in players}

        # 3. 최근 소속팀 매핑 벌크 로드 (N+1 방지)
        recent_games = (
            session.query(GameBattingStat.player_id, GameBattingStat.team_code, Game.game_date)
            .join(Game, GameBattingStat.game_id == Game.game_id)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(GameBattingStat.player_id.in_(player_ids))
            .filter(KboSeason.season_year == year)
            .all()
        )

        player_team_map = {}
        player_latest_date = {}
        for pid, team_code, gdate in recent_games:
            if not pid or not team_code:
                continue
            if pid not in player_latest_date or gdate > player_latest_date[pid]:
                player_latest_date[pid] = gdate
                player_team_map[pid] = team_code

        series_mapping = get_series_mapping()
        series_info = series_mapping.get(series_key, {})
        league_name = series_info.get("league", "REGULAR")

        # 4. 데이터 딕셔너리 구성
        for agg_data in bulk_stats:
            pid = agg_data["player_id"]
            player_data = {
                "player_id": pid,
                "player_name": player_name_map.get(pid) or agg_data.get("player_name") or f"Player_{pid}",
                "season": year,
                "league": league_name,
                "source": "FALLBACK",
            }
            player_data.update(agg_data)

            # 최근 팀 보정
            if pid in player_team_map:
                player_data["team_code"] = player_team_map[pid]

            all_players_data.append(player_data)

    logger.info("✅ DB 집계 완료: 총 %s명", len(all_players_data))
    return all_players_data


def _select_season_and_series(
    page: Page,
    year: int,
    series_info: dict,
    policy: RequestPolicy,
) -> None:
    season_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason"]'
    policy.delay()
    page.select_option(season_selector, str(year))
    logger.info("✅ %s년 시즌 선택", year)
    page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)

    series_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries"]'
    policy.delay()
    page.select_option(series_selector, value=series_info["value"])
    logger.info("✅ %s 선택", series_info["name"])
    page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)


def _get_team_options(page: Page, *, by_team: bool) -> list[dict]:
    if not by_team:
        return [{"value": "", "text": "전체"}]
    try:
        team_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlTeam$ddlTeam"]'
        options = page.eval_on_selector_all(
            f"{team_selector} option",
            "options => options.map(o => ({text: o.textContent, value: o.value}))",
        )
        team_options = [opt for opt in options if opt["value"]]  # Empty value is "Team Selection"
        logger.info("ℹ️ 팀별 순회 모드: %s개 팀 발견", len(team_options))
    except CRAWLER_EXCEPTIONS:
        logger.exception("⚠️ 팀 목록 추출 실패, 전체 모드로 진행")
        return [{"value": "", "text": "전체"}]
    else:
        return team_options


def _apply_pa_sorting(page: Page, policy: RequestPolicy) -> None:
    pa_sort_link = "a[href=\"javascript:sort('PA_CN');\"]"
    if page.query_selector(pa_sort_link):
        page.click(pa_sort_link)
        logger.info("✅ 타석(PA) 기준 정렬 적용")
        page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)
        policy.delay()
    else:
        logger.warning("⚠️ 타석 정렬 버튼을 찾을 수 없습니다.")


def _select_team_if_needed(page: Page, tm: dict, *, by_team: bool, policy: RequestPolicy) -> bool:
    if by_team and tm["value"]:
        logger.info("🔍 팀 선택: %s (%s)", tm["text"], tm["value"])
        try:
            page.select_option(
                'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlTeam$ddlTeam"]',
                tm["value"],
            )
            page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)
            policy.delay()
        except CRAWLER_EXCEPTIONS:
            logger.exception("⚠️ 팀 선택 실패 (%s)", tm["text"])
            return False
        else:
            return True
    return True


def _process_current_page_batting(
    page: Page,
    year: int,
    series_key: str,
    unique_players: set[int],
    all_players_data: list[dict],
) -> int:
    current_page_data = parse_batting_stats_table(page, series_key, year)
    for player_stat in current_page_data:
        pid = player_stat["player_id"]
        if pid not in unique_players:
            unique_players.add(pid)
            all_players_data.append(player_stat)
        else:
            for p in all_players_data:
                if p["player_id"] == pid:
                    p.update(player_stat)
                    break
    return len(current_page_data)


def _collect_batting_stats_loop(ctx: BattingCrawlContext) -> None:
    total_collected = 0
    for tm in ctx.iteration_targets:
        if not _select_team_if_needed(ctx.page, tm, by_team=ctx.by_team, policy=ctx.policy):
            continue

        _apply_pa_sorting(ctx.page, ctx.policy)

        page_num = 1
        while True:
            added = _process_current_page_batting(
                page=ctx.page,
                year=ctx.year,
                series_key=ctx.series_key,
                unique_players=ctx.unique_players,
                all_players_data=ctx.all_players_data,
            )
            total_collected += added

            logger.info(
                "   ▶ %s페이지: %s명 처리 (누적 %s명)",
                page_num,
                added,
                total_collected,
            )

            if ctx.limit and total_collected >= ctx.limit:
                logger.info("🎯 목표 수(%s명) 달성. 수집 중단.", ctx.limit)
                return

            if not go_to_next_page(ctx.page, page_num, ctx.policy):
                break

            page_num += 1
            ctx.policy.delay()


def _merge_basic2_data(
    all_players_data: list[dict],
    page: Page,
    year: int,
    series_info: dict,
    policy: RequestPolicy,
) -> list[dict]:
    logger.info("\n🔍 정규시즌 Basic2 추가 데이터 수집 시작...")
    basic2_data = crawl_basic2_with_headers(page, year, series_info, policy)

    if not basic2_data:
        logger.warning("⚠️ Basic2 데이터 수집 실패, Basic1 데이터만 사용")
        return all_players_data

    basic1_dict = {p["player_id"]: p for p in all_players_data}

    for player_id, basic2_player in basic2_data.items():
        if player_id in basic1_dict:
            for key, value in basic2_player.items():
                if value is not None and key not in [
                    "player_id",
                    "player_name",
                    "team_code",
                    "season",
                    "league",
                    "level",
                    "source",
                ]:
                    basic1_dict[player_id][key] = value

    logger.info("✅ Basic1 + Basic2 데이터 병합 완료")
    return list(basic1_dict.values())


def _handle_batting_fallback(
    year: int,
    series_key: str,
    reason: str,
    *,
    save_to_db: bool,
) -> list[dict]:
    all_players_data = fallback_batting_from_db(year, series_key, reason=reason)
    # Set source to FALLBACK_AUTO
    for s in all_players_data:
        s["source"] = "FALLBACK_AUTO"

    FallbackMonitor.log_fallback(
        year,
        series_key,
        "BATTING",
        f"Fallback completed via {reason}",
        player_count=len(all_players_data),
    )
    if save_to_db and all_players_data:
        save_batting_stats_safe(all_players_data)
    return all_players_data


def crawl_series_batting_stats(
    year: int | None = None,
    series_key: str = "regular",
    limit: int | None = None,
    *,
    save_to_db: bool = False,
    headless: bool = False,
    by_team: bool = False,
) -> list[dict]:
    """
    특정 시리즈의 타자 기록을 크롤링.

    Args:
        year: Season year.
        series_key: Series Key.
        limit: Limit.
        save_to_db: Save To Db.
        headless: Whether to run the browser in headless mode.
        by_team: By Team.
        year: 시즌 연도
        series_key: 시리즈 키 (regular, exhibition, wildcard, etc.)
        limit: 수집할 선수 수 제한
        save_to_db: DB에 저장할지 여부
        by_team: 팀별로 순회하며 크롤링할지 여부 (규정타석 미달 선수 포함 위해)

    Returns:
        수집된 타자 기록 리스트

    """
    year = year or datetime.now(KST).year

    series_mapping = get_series_mapping()

    if series_key not in series_mapping:
        logger.error("❌ 지원하지 않는 시리즈: %s", series_key)
        return []

    series_info = series_mapping[series_key]
    all_players_data = []  # List of dicts
    unique_players = set()  # Track by ID

    policy = RequestPolicy()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        # Apply UA rotation via context
        context = browser.new_context(**policy.build_context_kwargs(locale="ko-KR"))
        page = context.new_page()
        page.set_default_timeout(30000)
        install_sync_resource_blocking(page)

        try:
            logger.info("\n📊 %s년 %s 타자 기록 수집 시작 (by_team=%s)", year, series_info["name"], by_team)
            logger.info("-" * 60)

            # 페이지로 이동 (Basic1 사용)
            url = HITTER_BASIC1
            if not compliance.is_allowed_sync(url):
                logger.info("[COMPLIANCE] Navigation to %s aborted.", url)
                return []

            policy.delay(host="www.koreabaseball.com")
            page.goto(url, wait_until="load", timeout=NAV_TIMEOUT)
            page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)

            # 시즌과 시리즈 설정
            try:
                _select_season_and_series(page, year, series_info, policy)
            except CRAWLER_EXCEPTIONS as e:
                reason = f"Season/Series selection error: {e}"
                logger.exception("Season/Series selection error, falling back to DB aggregation")
                browser.close()
                return _handle_batting_fallback(year, series_key, reason, save_to_db=save_to_db)

            # 순회 대상 설정 (팀 옵션이 있으면 팀별, 없으면 전체 1회)
            team_options = _get_team_options(page, by_team=by_team)
            _collect_batting_stats_loop(
                BattingCrawlContext(
                    page=page,
                    year=year,
                    series_key=series_key,
                    iteration_targets=team_options,
                    by_team=by_team,
                    limit=limit,
                    policy=policy,
                    unique_players=unique_players,
                    all_players_data=all_players_data,
                ),
            )

            # 정규시즌인 경우 Basic2 페이지에서 추가 데이터 수집
            if series_key == "regular" and all_players_data:
                all_players_data = _merge_basic2_data(all_players_data, page, year, series_info, policy)

            logger.info("✅ %s 데이터 수집 완료", series_info["name"])

        except DB_SAVE_EXCEPTIONS:
            logger.exception("❌ 크롤링 중 오류")

        finally:
            browser.close()

    all_players_data = _finalize_batting_summary(all_players_data, series_info)
    _save_batting_if_needed(all_players_data, save_to_db=save_to_db)
    return all_players_data


def _finalize_batting_summary(
    all_players_data: list[dict],
    series_info: dict,
) -> list[dict]:
    logger.info("-" * 60)
    logger.info("✅ %s 크롤링 완료! 총 %s명 수집", series_info["name"], len(all_players_data))
    summary, valid_players_data = build_batting_crawl_summary(all_players_data)
    if summary["filtered_rows"]:
        logger.warning("⚠️ 타자 시즌 row 필터링: %s건 (%s)", summary["filtered_rows"], summary["failure_counts"])
    return valid_players_data


def _save_batting_if_needed(all_players_data: list[dict], *, save_to_db: bool) -> None:
    if save_to_db and all_players_data:
        logger.info("\n💾 타자 데이터 DB 저장 시작 (외래키 제약조건 임시 비활성화)...")
        try:
            saved_count = save_batting_stats_safe(all_players_data)
            logger.info("✅ 타자 데이터 저장 완료: %s명", saved_count)
        except DB_SAVE_EXCEPTIONS:
            logger.exception("❌ 타자 데이터 저장 실패")


def crawl_all_series(
    year: int | None = None,
    limit: int | None = None,
    *,
    save_to_db: bool = False,
    headless: bool = False,
    by_team: bool = False,
) -> dict[str, list[dict]]:
    """
    모든 시리즈의 타자 기록을 크롤링.

    Args:
        year: Season year.
        limit: Limit.
        save_to_db: Save To Db.
        headless: Whether to run the browser in headless mode.
        by_team: By Team.

    Returns:
        시리즈별 수집된 데이터 딕셔너리

    """
    year = year or datetime.now(KST).year

    policy = RequestPolicy()
    series_mapping = get_series_mapping()
    all_series_data = {}

    for series_key, series_info in series_mapping.items():
        logger.info("\n🚀 %s 시작...", series_info["name"])
        series_data = crawl_series_batting_stats(
            year,
            series_key,
            limit,
            save_to_db=save_to_db,
            headless=headless,
            by_team=by_team,
        )
        all_series_data[series_key] = series_data

        policy.delay()

    return all_series_data


def main() -> None:
    """Run the main entry point for this CLI command."""
    parser = argparse.ArgumentParser(description="KBO 전체 시리즈 타자 기록 크롤러")

    parser.add_argument("--year", type=int, default=datetime.now(KST).year, help="시즌 연도 (기본값: 당해 연도)")
    parser.add_argument("--series", type=str, help="특정 시리즈만 크롤링 (regular, exhibition, wildcard, etc.)")
    parser.add_argument("--limit", type=int, help="수집할 선수 수 제한")
    parser.add_argument("--save", action="store_true", help="DB에 저장")
    parser.add_argument("--headless", action="store_true", help="헤드리스 모드로 실행")
    parser.add_argument("--by-team", action="store_true", help="팀별로 순회하여 모든 선수(비규정타석 포함) 수집")

    args = parser.parse_args()

    if args.series:
        # 특정 시리즈만 크롤링
        crawl_series_batting_stats(
            args.year,
            args.series,
            args.limit,
            save_to_db=args.save,
            headless=args.headless,
            by_team=args.by_team,
        )
    else:
        # 모든 시리즈 크롤링
        all_data = crawl_all_series(
            args.year,
            args.limit,
            save_to_db=args.save,
            headless=args.headless,
            by_team=args.by_team,
        )

        # 전체 요약
        logger.info("%s", "\n" + "=" * 60)
        logger.info("📈 전체 수집 요약 (%s년)", args.year)
        logger.info("%s", "=" * 60)
        for series_key, data in all_data.items():
            series_name = get_series_mapping()[series_key]["name"]
            logger.info("  %s: %s명", series_name, len(data))

        total_players = sum(len(data) for data in all_data.values())
        logger.info("\n총 수집 선수: %s명", total_players)


if __name__ == "__main__":
    main()
