"""
단순화된 Basic2 크롤러 - BB 헤더만 클릭
OCI 동기화 전 SQLite 저장 테스트용
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

from src.constants import KST

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Page, sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from src.repositories.save_kbo_batting import save_kbo_batting_batch
from src.urls import HITTER_BASIC1
from src.utils.playwright_blocking import install_sync_resource_blocking
from src.utils.playwright_helpers import goto_next_page
from src.utils.playwright_retry import NAV_TIMEOUT
from src.utils.request_policy import RequestPolicy

logger = logging.getLogger(__name__)

BASIC2_CRAWLER_EXCEPTIONS = (
    PlaywrightError,
    PlaywrightTimeoutError,
    TimeoutError,
    RuntimeError,
    ValueError,
    TypeError,
    OSError,
)


def safe_parse_number(value_str: str, data_type: type) -> int | float | None:
    """안전한 숫자 파싱 (0값 보존)"""
    if not value_str:
        return None
    value_str = value_str.strip()
    if not value_str or value_str in ["-", "N/A", ""]:
        return None
    try:
        return data_type(value_str)
    except (ValueError, TypeError):
        return None


def parse_player_id_from_link(link_href: str) -> int | None:
    """링크에서 player_id 추출"""
    try:
        if "playerId=" in link_href:
            player_id_str = link_href.split("playerId=")[1].split("&", maxsplit=1)[0]
            return int(player_id_str)
    except (ValueError, IndexError):
        pass
    return None


def _policy_delay(policy: RequestPolicy | None) -> None:
    if policy:
        policy.delay()


def _prepare_basic2_bb_page(page: Page, year: int, policy: RequestPolicy | None) -> bool:
    url = HITTER_BASIC1
    logger.info("   🔍 Basic1 페이지로 이동: %s", url)
    page.goto(url, wait_until="load", timeout=NAV_TIMEOUT)
    page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)
    _policy_delay(policy)

    season_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason"]'
    page.select_option(season_selector, str(year))
    logger.info("   ✅ %s년 연도 선택", year)
    _policy_delay(policy)

    series_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries"]'
    page.select_option(series_selector, value="0")
    logger.info("   ✅ 정규시즌 선택")
    _policy_delay(policy)

    return _open_basic2_page(page, policy) and _click_bb_header(page, policy) and _log_table_headers(page)


def _open_basic2_page(page: Page, policy: RequestPolicy | None) -> bool:
    next_link = page.query_selector('a.next[href*="Basic2.aspx"]')
    if not next_link:
        logger.error("   ❌ Basic2 '다음' 링크를 찾을 수 없습니다.")
        return False
    logger.info("   🔗 'Basic2' 다음 링크 클릭...")
    next_link.click()
    page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)
    _policy_delay(policy)
    logger.info("   ✅ Basic2 페이지 접속: %s", page.url)
    return True


def _click_bb_header(page: Page, policy: RequestPolicy | None) -> bool:
    logger.info("   📊 BB(볼넷) 헤더 클릭...")
    bb_link = page.query_selector("a[href*=\"sort('BB_CN')\"]")
    if not bb_link:
        logger.error("   ❌ BB 헤더를 찾을 수 없습니다.")
        return False
    bb_link.click()
    page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)
    _policy_delay(policy)
    logger.info("   ✅ BB 헤더 클릭 완료")
    return True


def _log_table_headers(page: Page) -> bool:
    table = page.query_selector("table")
    if not table:
        logger.error("   ❌ 테이블을 찾을 수 없습니다.")
        return False
    thead = table.query_selector("thead")
    if thead:
        headers = [cell.inner_text().strip() for cell in thead.query_selector_all("th")]
        logger.info("   📋 테이블 헤더: %s", headers)
    return True


def _merge_page_data(all_player_data: dict[int, dict], page_data: dict[int, dict]) -> None:
    for player_id, data in page_data.items():
        if player_id not in all_player_data:
            all_player_data[player_id] = data
        else:
            all_player_data[player_id].update(data)


def _collect_all_bb_pages(page: Page, year: int, policy: RequestPolicy | None) -> dict[int, dict]:
    all_player_data = {}
    page_num = 1
    while True:
        logger.info("      📄 페이지 %s 처리 중...", page_num)
        page_data = collect_current_page_bb_data(page, year)
        if not page_data:
            logger.warning("      ⚠️ 페이지 %s에 데이터가 없습니다.", page_num)
            break
        _merge_page_data(all_player_data, page_data)
        logger.info("         ✅ %s명 데이터 수집, 총 %s명", len(page_data), len(all_player_data))
        if not goto_next_page(page, policy=policy):
            break
        page_num += 1
        _policy_delay(policy)
    return all_player_data


def crawl_bb_basic2_data(page: Page, year: int, policy: RequestPolicy | None = None) -> dict[int, dict]:
    """
    BB 헤더만 클릭하는 단순화된 Basic2 크롤링
    """
    logger.info("📊 %s년 정규시즌 BB 헤더 Basic2 크롤링 시작...", year)

    try:
        if not _prepare_basic2_bb_page(page, year, policy):
            return {}
        all_player_data = _collect_all_bb_pages(page, year, policy)

    except BASIC2_CRAWLER_EXCEPTIONS:
        logger.exception("   ❌ Basic2 BB 데이터 수집 중 오류")
        return {}
    else:
        logger.info("   ✅ BB 헤더 기준 데이터 수집 완료: %s명", len(all_player_data))
        return all_player_data


def _current_table_rows(page: Page) -> list:
    table = page.query_selector("table")
    if not table:
        return []
    tbody = table.query_selector("tbody")
    if tbody:
        return tbody.query_selector_all("tr")
    return table.query_selector_all("tr")[1:]


def _base_player_data(cells: list, year: int) -> tuple[int | None, str | None, dict]:
    if len(cells) < 5:
        return None, None, {}
    player_link = cells[1].query_selector("a") if len(cells) > 1 else None
    if not player_link:
        return None, None, {}
    player_id = parse_player_id_from_link(player_link.get_attribute("href"))
    if not player_id:
        return None, None, {}
    player_name = player_link.inner_text().strip()
    return (
        player_id,
        player_name,
        {
            "player_id": player_id,
            "player_name": player_name,
            "team_code": cells[2].inner_text().strip() if len(cells) > 2 else None,
            "year": year,
            "league": "KBO",
            "source": "PROFILE",
            "level": "KBO1",
        },
    )


def _bb_stat_payload(cells: list) -> dict[str, int | float | None]:
    if len(cells) < 15:
        return {}
    return {
        "avg": safe_parse_number(cells[3].inner_text().strip(), float),
        "walks": safe_parse_number(cells[4].inner_text().strip(), int),
        "intentional_walks": safe_parse_number(cells[5].inner_text().strip(), int),
        "hit_by_pitch": safe_parse_number(cells[6].inner_text().strip(), int),
        "strikeouts": safe_parse_number(cells[7].inner_text().strip(), int),
        "gdp": safe_parse_number(cells[8].inner_text().strip(), int),
        "slg": safe_parse_number(cells[9].inner_text().strip(), float),
        "obp": safe_parse_number(cells[10].inner_text().strip(), float),
        "ops": safe_parse_number(cells[11].inner_text().strip(), float),
    }


def _bb_extra_stats(cells: list) -> dict[str, int | float | None]:
    extra_stats = {}
    if len(cells) > 12:
        extra_stats["multi_hits"] = safe_parse_number(cells[12].inner_text().strip(), int)
    if len(cells) > 13:
        extra_stats["risp_avg"] = safe_parse_number(cells[13].inner_text().strip(), float)
    if len(cells) > 14:
        extra_stats["pinch_hit_avg"] = safe_parse_number(cells[14].inner_text().strip(), float)
    return extra_stats


def _parse_bb_row(row: object, year: int) -> tuple[int | None, dict]:
    cells = row.query_selector_all("td")
    player_id, player_name, player_data = _base_player_data(cells, year)
    if not player_id:
        return None, {}
    try:
        player_data.update(_bb_stat_payload(cells))
        if len(cells) >= 15:
            player_data["extra_stats"] = _bb_extra_stats(cells)
    except BASIC2_CRAWLER_EXCEPTIONS:
        logger.exception("         ⚠️ %s 스탯 파싱 오류", player_name)
    return player_id, player_data


def collect_current_page_bb_data(page: Page, year: int) -> dict[int, dict]:
    """현재 페이지의 BB 기준 선수 데이터 수집"""
    page_data = {}
    try:
        for row in _current_table_rows(page):
            player_id, player_data = _parse_bb_row(row, year)
            if player_id:
                page_data[player_id] = player_data
    except BASIC2_CRAWLER_EXCEPTIONS:
        logger.exception("         ⚠️ 페이지 데이터 수집 중 오류")
    return page_data


def main() -> None:
    """메인 실행 함수"""
    YEAR = datetime.now(KST).year

    logger.info("🚀 KBO %s년 BB 헤더 Basic2 크롤링 테스트 시작", YEAR)

    policy = RequestPolicy()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        page = browser.new_page()
        install_sync_resource_blocking(page)

        try:
            # BB 헤더 Basic2 데이터 수집
            bb_data = crawl_bb_basic2_data(page, YEAR, policy=policy)

            if bb_data:
                logger.info("\n📊 수집 결과: %s명", len(bb_data))

                # 샘플 데이터 출력
                if bb_data:
                    first_player = next(iter(bb_data.values()))
                    logger.info("\n📋 샘플 데이터:")
                    for key, value in first_player.items():
                        logger.info("   %s: %s", key, value)

                # SQLite 저장
                logger.info("\n💾 SQLite 저장 시작...")
                saved_count = save_kbo_batting_batch(bb_data, "정규시즌 BB 테스트")

                logger.info("\n🎉 완료!")
                logger.info("   📊 수집: %s명", len(bb_data))
                logger.info("   💾 저장: %s명", saved_count)
                logger.info("   📅 시간: %s", datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"))

            else:
                logger.error("❌ 데이터를 수집하지 못했습니다.")

        except BASIC2_CRAWLER_EXCEPTIONS:
            logger.exception("❌ 크롤링 중 오류 발생")

        finally:
            browser.close()


if __name__ == "__main__":
    main()
