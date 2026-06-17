"""
KBO 선수 타자 기록 완전 크롤러
Docs/schema/player_season_data.md 스키마를 기반으로 구현

정규시즌: Basic1 + Basic2 (헤더 클릭) 데이터 수집
기타시리즈: Basic1 기본 데이터만 수집
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from typing import Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Page, sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from sqlalchemy.exc import SQLAlchemyError

from src.repositories.save_futures_batting import save_futures_batting
from src.urls import HITTER_BASIC1
from src.utils.playwright_blocking import install_sync_resource_blocking
from src.utils.playwright_helpers import goto_next_page
from src.utils.playwright_retry import NAV_TIMEOUT
from src.utils.request_policy import RequestPolicy

logger = logging.getLogger(__name__)
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


def safe_parse_number(value_str: str, data_type: type, allow_zero: bool = True) -> int | float | None:
    """안전한 숫자 파싱 (0값 보존)"""
    if not value_str:
        return None
    value_str = value_str.strip()
    if not value_str or value_str in ["-", "N/A", ""]:
        return None
    try:
        parsed_value = data_type(value_str)
        return parsed_value
    except (ValueError, TypeError):
        return None


def parse_player_id_from_link(link_href: str) -> int | None:
    """링크에서 player_id 추출"""
    try:
        if "playerId=" in link_href:
            player_id_str = link_href.split("playerId=")[1].split("&")[0]
            return int(player_id_str)
    except (ValueError, IndexError):
        pass
    return None


def crawl_regular_season_data(page: Page, year: int, policy: RequestPolicy | None = None) -> dict[int, dict]:
    """
    정규시즌 데이터 크롤링 (Basic1 + Basic2)
    컬럼: AVG,G,PA,AB,R,H,2B,3B,HR,TB,RBI,SAC,SF + BB,IBB,HBP,SO,GDP,SLG,OBP,OPS,MH,RISP,PH-BA
    """
    logger.info("📊 %s년 정규시즌 데이터 크롤링 시작...", year)

    # 1. Basic1 데이터 수집
    basic1_data = crawl_basic1_data(page, year, {"value": "0", "name": "정규시즌"}, policy=policy)
    logger.info("   ✅ Basic1 데이터: %s명", len(basic1_data))

    # 2. Basic2 데이터 수집 (헤더 클릭)
    basic2_data = crawl_basic2_with_headers(page, year, {"value": "0", "name": "정규시즌"}, policy=policy)
    logger.info("   ✅ Basic2 데이터: %s명", len(basic2_data))

    # 3. 데이터 병합
    merged_data = {}
    for player_id in basic1_data:
        merged_data[player_id] = {**basic1_data[player_id], **basic2_data.get(player_id, {})}

    # Basic2에만 있는 선수들도 추가
    for player_id in basic2_data:
        if player_id not in merged_data:
            merged_data[player_id] = basic2_data[player_id]

    logger.info("   ✅ 병합 완료: %s명", len(merged_data))
    return merged_data


def crawl_basic1_data(page: Page, year: int, series_info: dict, policy: RequestPolicy | None = None) -> dict[int, dict]:
    """
    Basic1 페이지 데이터 크롤링
    컬럼: AVG,G,PA,AB,R,H,2B,3B,HR,TB,RBI,SAC,SF (정규시즌)
    컬럼: AVG,G,PA,AB,H,2B,3B,HR,RBI,SB,CS,BB,HBP,SO,GDP,E (기타시리즈)
    """
    logger.info("   🔍 Basic1 데이터 수집: %s", series_info["name"])

    try:
        # Basic1 페이지로 이동
        url = HITTER_BASIC1
        page.goto(url, wait_until="load", timeout=NAV_TIMEOUT)
        page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)
        if policy:
            policy.delay()

        # 연도 선택
        season_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason"]'
        page.select_option(season_selector, str(year))
        if policy:
            policy.delay()

        # 시리즈 선택
        series_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries"]'
        page.select_option(series_selector, value=series_info["value"])
        if policy:
            policy.delay()

        # 페이지별 데이터 수집
        all_player_data = {}
        page_num = 1

        while True:
            logger.info("      📄 페이지 %s 처리 중...", page_num)

            # 테이블 찾기
            table = page.query_selector("table")
            if not table:
                logger.warning("      ⚠️ 페이지 %s에서 테이블을 찾을 수 없습니다.", page_num)
                break

            # 헤더 확인
            thead = table.query_selector("thead")
            if thead:
                header_cells = thead.query_selector_all("th")
                headers = [cell.inner_text().strip() for cell in header_cells]
                logger.info("         📋 헤더: %s", headers)

            # 데이터 행 처리
            tbody = table.query_selector("tbody")
            if tbody:
                rows = tbody.query_selector_all("tr")
            else:
                rows = table.query_selector_all("tr")[1:]  # 첫 번째 행(헤더) 제외

            if not rows:
                logger.warning("      ⚠️ 페이지 %s에 데이터가 없습니다.", page_num)
                break

            for row in rows:
                cells = row.query_selector_all("td")
                if len(cells) < 5:  # 최소 데이터 확인
                    continue

                # 선수 링크에서 player_id 추출
                name_cell = cells[1] if len(cells) > 1 else None
                if not name_cell:
                    continue

                player_link = name_cell.query_selector("a")
                if not player_link:
                    continue

                player_id = parse_player_id_from_link(player_link.get_attribute("href"))
                if not player_id:
                    continue

                player_name = player_link.inner_text().strip()
                team_code = cells[2].inner_text().strip() if len(cells) > 2 else None

                # 기본 정보
                player_data = {
                    "player_id": player_id,
                    "player_name": player_name,
                    "team_code": team_code,
                    "year": year,
                    "league": "KBO",
                    "source": "profile",
                    "series_name": series_info["name"],
                    "series_value": series_info["value"],
                }

                # 시리즈별 컬럼 매핑
                if series_info["value"] == "0":  # 정규시즌
                    player_data.update(parse_regular_season_basic1_stats(cells))
                else:  # 기타 시리즈
                    player_data.update(parse_other_series_stats(cells))

                all_player_data[player_id] = player_data

            logger.info("         ✅ %s개 행 처리, 총 %s명", len(rows), len(all_player_data))

            # 다음 페이지 확인
            if not goto_next_page(page, policy=policy):
                break

            page_num += 1
            if policy:
                policy.delay()

        return all_player_data

    except CRAWLER_EXCEPTIONS:
        logger.exception("   ❌ Basic1 데이터 수집 중 오류")
        return {}


def parse_regular_season_basic1_stats(cells: list) -> dict[str, Any]:
    """정규시즌 Basic1 통계 파싱: AVG,G,PA,AB,R,H,2B,3B,HR,TB,RBI,SAC,SF"""
    stats = {}
    try:
        if len(cells) >= 16:  # 예상 컬럼 수
            stats.update(
                {
                    "avg": safe_parse_number(cells[3].inner_text().strip(), float),
                    "games": safe_parse_number(cells[4].inner_text().strip(), int),
                    "plate_appearances": safe_parse_number(cells[5].inner_text().strip(), int),
                    "at_bats": safe_parse_number(cells[6].inner_text().strip(), int),
                    "runs": safe_parse_number(cells[7].inner_text().strip(), int),
                    "hits": safe_parse_number(cells[8].inner_text().strip(), int),
                    "doubles": safe_parse_number(cells[9].inner_text().strip(), int),
                    "triples": safe_parse_number(cells[10].inner_text().strip(), int),
                    "home_runs": safe_parse_number(cells[11].inner_text().strip(), int),
                    "total_bases": safe_parse_number(cells[12].inner_text().strip(), int),
                    "rbis": safe_parse_number(cells[13].inner_text().strip(), int),
                    "sacrifice_bunts": safe_parse_number(cells[14].inner_text().strip(), int),
                    "sacrifice_flies": safe_parse_number(cells[15].inner_text().strip(), int),
                },
            )
    except CRAWLER_EXCEPTIONS:
        logger.exception("      ⚠️ 정규시즌 Basic1 통계 파싱 오류")

    return stats


def parse_other_series_stats(cells: list) -> dict[str, Any]:
    """기타시리즈 통계 파싱: AVG,G,PA,AB,H,2B,3B,HR,RBI,SB,CS,BB,HBP,SO,GDP,E"""
    stats = {}
    try:
        if len(cells) >= 18:  # 예상 컬럼 수
            stats.update(
                {
                    "avg": safe_parse_number(cells[3].inner_text().strip(), float),
                    "games": safe_parse_number(cells[4].inner_text().strip(), int),
                    "plate_appearances": safe_parse_number(cells[5].inner_text().strip(), int),
                    "at_bats": safe_parse_number(cells[6].inner_text().strip(), int),
                    "hits": safe_parse_number(cells[7].inner_text().strip(), int),
                    "doubles": safe_parse_number(cells[8].inner_text().strip(), int),
                    "triples": safe_parse_number(cells[9].inner_text().strip(), int),
                    "home_runs": safe_parse_number(cells[10].inner_text().strip(), int),
                    "rbis": safe_parse_number(cells[11].inner_text().strip(), int),
                    "stolen_bases": safe_parse_number(cells[12].inner_text().strip(), int),
                    "caught_stealing": safe_parse_number(cells[13].inner_text().strip(), int),
                    "walks": safe_parse_number(cells[14].inner_text().strip(), int),
                    "hit_by_pitch": safe_parse_number(cells[15].inner_text().strip(), int),
                    "strikeouts": safe_parse_number(cells[16].inner_text().strip(), int),
                    "gdp": safe_parse_number(cells[17].inner_text().strip(), int),
                    "errors": safe_parse_number(cells[18].inner_text().strip(), int) if len(cells) > 18 else None,
                },
            )
    except CRAWLER_EXCEPTIONS:
        logger.exception("      ⚠️ 기타시리즈 통계 파싱 오류")

    return stats


def crawl_basic2_with_headers(
    page: Page,
    year: int,
    series_info: dict,
    policy: RequestPolicy | None = None,
) -> dict[int, dict]:
    """
    Basic2 헤더 클릭으로 추가 데이터 수집
    컬럼: BB,IBB,HBP,SO,GDP,SLG,OBP,OPS,MH,RISP,PH-BA
    """
    logger.info("   🔍 Basic2 헤더 클릭 데이터 수집: %s", series_info["name"])

    headers_to_click = [
        ("BB", "BB_CN", "볼넷"),
        ("IBB", "IB_CN", "고의사구"),
        ("HBP", "HP_CN", "사구"),
        ("SO", "KK_CN", "삼진"),
        ("GDP", "GD_CN", "병살타"),
        ("SLG", "SLG_RT", "장타율"),
        ("OBP", "OBP_RT", "출루율"),
        ("OPS", "OPS_RT", "OPS"),
        ("MH", "MH_HITTER_CN", "멀티히트"),
        ("RISP", "SP_HRA_RT", "득점권타율"),
        ("PH-BA", "PH_HRA_RT", "대타타율"),
    ]

    all_player_data = {}

    try:
        # Basic1에서 시작하여 Basic2로 이동
        url = HITTER_BASIC1
        page.goto(url, wait_until="load", timeout=NAV_TIMEOUT)
        page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)
        if policy:
            policy.delay()

        # 연도 및 시리즈 선택
        season_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason"]'
        page.select_option(season_selector, str(year))
        if policy:
            policy.delay()

        series_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries"]'
        page.select_option(series_selector, value=series_info["value"])
        if policy:
            policy.delay()

        # "다음" 링크로 Basic2 접근
        next_link = page.query_selector('a.next[href*="Basic2.aspx"]')
        if not next_link:
            logger.warning("      ⚠️ Basic2 '다음' 링크를 찾을 수 없습니다.")
            return {}

        next_link.click()
        page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)
        if policy:
            policy.delay()

        # 각 헤더별 데이터 수집
        for i, (header_name, sort_code, description) in enumerate(headers_to_click):
            logger.info("      📊 %s(%s) 헤더 클릭... (%s/11)", description, header_name, i + 1)

            try:
                # 헤더 클릭
                header_link = page.query_selector(f"a[href*=\"sort('{sort_code}')\"]")
                if not header_link:
                    logger.warning("         ⚠️ %s 헤더를 찾을 수 없습니다.", header_name)
                    continue

                header_link.click()
                page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)
                if policy:
                    policy.delay()

                # 현재 정렬 기준으로 데이터 수집
                page_data = collect_current_page_data(page, header_name, policy=policy)

                # 데이터 병합
                for player_id, data in page_data.items():
                    if player_id not in all_player_data:
                        all_player_data[player_id] = data
                    else:
                        all_player_data[player_id].update(data)

                logger.info("         ✅ %s명 데이터 수집", len(page_data))

            except CRAWLER_EXCEPTIONS:
                logger.exception("         ❌ %s 헤더 처리 중 오류", header_name)
                continue

        logger.info("   ✅ Basic2 헤더별 데이터 수집 완료: %s명", len(all_player_data))
        return all_player_data

    except CRAWLER_EXCEPTIONS:
        logger.exception("   ❌ Basic2 데이터 수집 중 오류")
        return {}


def collect_current_page_data(page: Page, sort_field: str, policy: RequestPolicy | None = None) -> dict[int, dict]:
    """현재 페이지의 모든 선수 데이터 수집"""
    page_data = {}

    try:
        # 모든 페이지 순회
        page_num = 1
        while True:
            # 테이블 데이터 파싱
            table = page.query_selector("table")
            if not table:
                break

            tbody = table.query_selector("tbody")
            if tbody:
                rows = tbody.query_selector_all("tr")
            else:
                rows = table.query_selector_all("tr")[1:]

            if not rows:
                break

            for row in rows:
                cells = row.query_selector_all("td")
                if len(cells) < 5:
                    continue

                # player_id 추출
                name_cell = cells[1]
                player_link = name_cell.query_selector("a")
                if not player_link:
                    continue

                player_id = parse_player_id_from_link(player_link.get_attribute("href"))
                if not player_id:
                    continue

                # 기본 정보
                player_data = {
                    "player_id": player_id,
                    "player_name": player_link.inner_text().strip(),
                    "team_code": cells[2].inner_text().strip(),
                }

                # sort_field에 따른 데이터 추출
                player_data.update(extract_basic2_stats(cells, sort_field))

                page_data[player_id] = player_data

            # 다음 페이지로 이동
            if not goto_next_page(page, policy=policy):
                break

            page_num += 1
            if policy:
                policy.delay()

    except CRAWLER_EXCEPTIONS:
        logger.exception("         ⚠️ 페이지 데이터 수집 중 오류")

    return page_data


def extract_basic2_stats(cells: list, sort_field: str) -> dict[str, Any]:
    """Basic2 통계 추출"""
    stats = {}
    try:
        # 헤더 위치 매핑 (추정값, 실제 사이트 구조에 따라 조정 필요)
        field_positions = {
            "BB": 4,
            "IBB": 5,
            "HBP": 6,
            "SO": 7,
            "GDP": 8,
            "SLG": 9,
            "OBP": 10,
            "OPS": 11,
            "MH": 12,
            "RISP": 13,
            "PH-BA": 14,
        }

        field_mapping = {
            "BB": "walks",
            "IBB": "intentional_walks",
            "HBP": "hit_by_pitch",
            "SO": "strikeouts",
            "GDP": "gdp",
            "SLG": "slg",
            "OBP": "obp",
            "OPS": "ops",
            "MH": "multi_hits",
            "RISP": "risp_avg",
            "PH-BA": "pinch_hit_avg",
        }

        if sort_field in field_positions and sort_field in field_mapping:
            pos = field_positions[sort_field]
            field_name = field_mapping[sort_field]

            if len(cells) > pos:
                value_str = cells[pos].inner_text().strip()

                # 데이터 타입 결정
                if sort_field in ["SLG", "OBP", "OPS", "RISP", "PH-BA"]:
                    data_type = float
                else:
                    data_type = int

                parsed_value = safe_parse_number(value_str, data_type)
                if parsed_value is not None:
                    stats[field_name] = parsed_value

    except CRAWLER_EXCEPTIONS:
        logger.exception("         ⚠️ Basic2 통계 추출 오류")

    return stats


def crawl_other_series_data(
    page: Page,
    year: int,
    series_list: list[dict],
    policy: RequestPolicy | None = None,
) -> dict[str, dict[int, dict]]:
    """기타 시리즈 데이터 크롤링 (기본 데이터만)"""
    all_series_data = {}

    for series_info in series_list:
        logger.info("📊 %s년 %s 데이터 크롤링...", year, series_info["name"])

        series_data = crawl_basic1_data(page, year, series_info, policy=policy)
        if series_data:
            all_series_data[series_info["name"]] = series_data
            logger.info("   ✅ %s: %s명", series_info["name"], len(series_data))
        else:
            logger.warning("   ⚠️ %s: 데이터 없음", series_info["name"])

    return all_series_data


def save_to_database(player_data: dict[int, dict], series_name: str) -> int:
    """데이터베이스에 저장"""
    try:
        logger.info("💾 %s 데이터 저장 중...", series_name)

        saved_count = 0
        for _player_id, data in player_data.items():
            try:
                # 저장용 데이터 형식 변환
                save_data = {
                    "player_id": data["player_id"],
                    "year": data["year"],
                    "league": data["league"],
                    "source": data["source"],
                    "series_name": data["series_name"],
                    "team_code": data["team_code"],
                    "games": data.get("games"),
                    "plate_appearances": data.get("plate_appearances"),
                    "at_bats": data.get("at_bats"),
                    "runs": data.get("runs"),
                    "hits": data.get("hits"),
                    "doubles": data.get("doubles"),
                    "triples": data.get("triples"),
                    "home_runs": data.get("home_runs"),
                    "rbis": data.get("rbis"),
                    "walks": data.get("walks"),
                    "strikeouts": data.get("strikeouts"),
                    "avg": data.get("avg"),
                    "obp": data.get("obp"),
                    "slg": data.get("slg"),
                    "ops": data.get("ops"),
                    "stolen_bases": data.get("stolen_bases"),
                    "caught_stealing": data.get("caught_stealing"),
                    "hit_by_pitch": data.get("hit_by_pitch"),
                    "intentional_walks": data.get("intentional_walks"),
                    "sacrifice_bunts": data.get("sacrifice_bunts"),
                    "sacrifice_flies": data.get("sacrifice_flies"),
                    "gdp": data.get("gdp"),
                    "errors": data.get("errors"),
                    "total_bases": data.get("total_bases"),
                    "extra_stats": {
                        "multi_hits": data.get("multi_hits"),
                        "risp_avg": data.get("risp_avg"),
                        "pinch_hit_avg": data.get("pinch_hit_avg"),
                    },
                }

                save_futures_batting(save_data)
                saved_count += 1

            except DB_SAVE_EXCEPTIONS:
                logger.exception("   ⚠️ %s 저장 실패", data["player_name"])
                continue

        logger.info("   ✅ %s/%s명 저장 완료", saved_count, len(player_data))
        return saved_count

    except DB_SAVE_EXCEPTIONS:
        logger.exception("   ❌ 데이터베이스 저장 중 오류")
        return 0


def main() -> None:
    """메인 실행 함수"""
    # 크롤링 대상 설정
    YEAR = datetime.now().year

    # 시리즈 정의
    SERIES_LIST = [
        {"value": "1", "name": "KBO 시범경기"},
        {"value": "4", "name": "KBO 와일드카드"},
        {"value": "3", "name": "KBO 준플레이오프"},
        {"value": "5", "name": "KBO 플레이오프"},
        {"value": "7", "name": "KBO 한국시리즈"},
    ]

    logger.info("🚀 KBO %s년 선수 타자 기록 완전 크롤링 시작", YEAR)
    logger.info("📋 대상: 정규시즌(Enhanced) + %s개 시리즈(Basic)", len(SERIES_LIST))

    policy = RequestPolicy()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        page = browser.new_page()
        install_sync_resource_blocking(page)

        try:
            total_saved = 0

            # 1. 정규시즌 데이터 수집 (Basic1 + Basic2)
            logger.info("\n%s", "=" * 50)
            logger.info("📊 1단계: 정규시즌 데이터 수집 (Enhanced)")
            logger.info("%s", "=" * 50)

            regular_season_data = crawl_regular_season_data(page, YEAR, policy=policy)
            if regular_season_data:
                saved = save_to_database(regular_season_data, "정규시즌")
                total_saved += saved

            # 2. 기타 시리즈 데이터 수집 (Basic1만)
            logger.info("\n%s", "=" * 50)
            logger.info("📊 2단계: 기타 시리즈 데이터 수집 (Basic)")
            logger.info("%s", "=" * 50)

            other_series_data = crawl_other_series_data(page, YEAR, SERIES_LIST, policy=policy)
            for series_name, series_data in other_series_data.items():
                if series_data:
                    saved = save_to_database(series_data, series_name)
                    total_saved += saved

            # 3. 최종 결과
            logger.info("\n%s", "=" * 50)
            logger.info("🎉 크롤링 완료")
            logger.info("%s", "=" * 50)
            logger.info("📊 총 저장된 레코드: %s개", total_saved)
            logger.info("📅 크롤링 시간: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        except DB_SAVE_EXCEPTIONS:
            logger.exception("❌ 크롤링 중 오류 발생")

        finally:
            browser.close()


if __name__ == "__main__":
    main()
