"""
전체 선수의 수비 기록을 포지션별 랭킹 페이지에서 크롤링하고 DB에 저장합니다.
(2025년 10월 업데이트: KBO 웹사이트에서 개별 선수 수비 페이지가 제거되어 포지션별 랭킹 페이지 사용)
"""

import logging
import sqlite3
from datetime import datetime
from typing import Any

from playwright.sync_api import sync_playwright

from src.utils.playwright_blocking import install_sync_resource_blocking

logger = logging.getLogger(__name__)
import contextlib

from src.utils.player_season_stat_validation import filter_valid_season_stat_payloads
from src.utils.playwright_retry import LONG_TIMEOUT, SEL_TIMEOUT, retry_navigation, retry_wait_for_selector
from src.utils.request_policy import RequestPolicy
from src.utils.team_codes import resolve_team_code


def parse_inns(txt: str) -> float:
    """이닝 문자열을 float으로 변환 (예: '112 1/3' -> 112.333...)."""
    txt = txt.strip().replace(",", "")
    if not txt or txt == "-":
        return 0.0
    if " " in txt:
        parts = txt.split(" ")
        val = float(parts[0])
        if len(parts) > 1 and "/" in parts[1]:
            frac = parts[1].split("/")
            val += float(frac[0]) / float(frac[1])
        return val
    if "/" in txt:
        frac = txt.split("/")
        return float(frac[0]) / float(frac[1])
    return float(txt)


def _s_int(cell_el: Any) -> int:
    """셀 요소에서 정수 값 추출."""
    try:
        return int(cell_el.inner_text().strip().replace(",", ""))
    except (ValueError, TypeError):
        return 0


def _s_float(cell_el: Any) -> float:
    """셀 요소에서 실수 값 추출."""
    try:
        return float(cell_el.inner_text().strip().replace(",", ""))
    except (ValueError, TypeError):
        return 0.0


def build_fielding_crawl_summary(records) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    valid_records, failure_counts = filter_valid_season_stat_payloads(
        records,
        stat_type="fielding",
    )
    summary = {
        "processed_rows": len(records),
        "valid_rows": len(valid_records),
        "filtered_rows": len(records) - len(valid_records),
        "failure_counts": dict(failure_counts),
    }
    return summary, valid_records


def crawl_all_fielding_stats(year=None) -> list[dict[str, Any]]:
    """
    KBO 공식 홈페이지에서 팀별 수비 기록을 크롤링하여 전체 선수의 수비 기록을 수집합니다.
    팀별로 조회하여 전체 수비수(투수 포함)를 누락 없이 가져옵니다.
    포수의 경우 별도의 포지션 필터링을 통해 상세 지표(도루저지 등)를 추가 수집합니다.

    Args:
        year: 시즌 연도 (None이면 현재 연도)

    Returns:
        list: 수비 기록 딕셔너리 리스트
    """
    if year is None:
        year = datetime.now().year
    fielding_data = []
    fielding_data_map = {}  # (player_id, team_id, position_id) -> record
    policy = RequestPolicy()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(**policy.build_context_kwargs())
        page = context.new_page()
        install_sync_resource_blocking(page)

        # 포지션별 수비 랭킹 페이지
        url = "https://www.koreabaseball.com/Record/Player/Defense/Basic.aspx"
        logger.info("📊 수비 기록 페이지 접속: %s", url)
        try:
            page.goto(url, wait_until="load", timeout=LONG_TIMEOUT)
            page.wait_for_load_state("networkidle", timeout=SEL_TIMEOUT)
        except Exception:
            logger.exception("⚠️ 페이지 초기 대기 중 경고 (무시 가능)")
        policy.delay()

        try:
            # 연도 선택
            year_select = page.query_selector("select#cphContents_cphContents_cphContents_ddlSeason_ddlSeason")
            if year_select:
                page.select_option("select#cphContents_cphContents_cphContents_ddlSeason_ddlSeason", str(year))
                with contextlib.suppress(Exception):
                    page.wait_for_load_state("networkidle", timeout=SEL_TIMEOUT)
                policy.delay()
                logger.info("✅ %s년 데이터 선택 완료", year)

            # 포지션 한글 → ID 매핑
            position_mapping = {
                "투수": "P",
                "포수": "C",
                "1루수": "1B",
                "2루수": "2B",
                "3루수": "3B",
                "유격수": "SS",
                "좌익수": "LF",
                "중견수": "CF",
                "우익수": "RF",
                "외야수": "OF",
                "내야수": "IF",
                "지명타자": "DH",
            }

            # 1. 기본 수집: 팀별 전체 선수 (13개 기본 컬럼)
            team_select = page.query_selector("select#cphContents_cphContents_cphContents_ddlTeam_ddlTeam")
            if not team_select:
                logger.warning("⚠️ 팀 선택 드롭다운을 찾을 수 없습니다.")
                browser.close()
                return []

            options = team_select.query_selector_all("option")
            teams = []
            for opt in options:
                val = opt.get_attribute("value")
                text = opt.inner_text().strip()
                if val and val != "":
                    teams.append((val, text))

            logger.info("📋 발견된 팀 목록: %s", [t[1] for t in teams])

            for team_val, team_name in teams:
                try:
                    logger.info("\n🏢 [%s] 수비 기록 크롤링 중...", team_name)
                    # 포지션 선택을 "전체"로 초기화 (중요)
                    page.select_option("select#cphContents_cphContents_cphContents_ddlPos_ddlPos", value="")
                    policy.delay()

                    with page.expect_response("**/Record/Player/Defense/Basic.aspx", timeout=20000):
                        page.select_option("select#cphContents_cphContents_cphContents_ddlTeam_ddlTeam", value=team_val)
                    page.wait_for_load_state("networkidle", timeout=SEL_TIMEOUT)
                    policy.delay()

                    pagination = page.query_selector(".paging")
                    total_pages = 1
                    if pagination:
                        page_numbers = [
                            int(link.inner_text().strip())
                            for link in pagination.query_selector_all("a")
                            if link.inner_text().strip().isdigit()
                        ]
                        if page_numbers:
                            total_pages = max(page_numbers)

                    for current_page in range(1, total_pages + 1):
                        if current_page > 1:
                            try:
                                page_link = next(
                                    (
                                        link
                                        for link in page.query_selector(".paging").query_selector_all("a")
                                        if link.inner_text().strip() == str(current_page)
                                    ),
                                    None,
                                )
                                if page_link:
                                    with page.expect_response("**/Record/Player/Defense/Basic.aspx", timeout=20000):
                                        page_link.click()
                                    page.wait_for_load_state("networkidle", timeout=SEL_TIMEOUT)
                                    policy.delay()
                            except Exception:
                                logger.exception("   ⚠️ 페이지 %s 이동 중 오류", current_page)
                                break

                        table = page.query_selector("table.tData01.tt")
                        if not table or not table.query_selector("tbody"):
                            continue

                        for row in table.query_selector("tbody").query_selector_all("tr"):
                            cells = row.query_selector_all("td")
                            if len(cells) >= 13:
                                try:
                                    player_link = cells[1].query_selector("a")
                                    player_id = (
                                        player_link.get_attribute("href").split("playerId=")[1].split("&")[0]
                                        if player_link
                                        else None
                                    )
                                    if not player_id:
                                        continue

                                    p_name = cells[1].inner_text().strip()
                                    row_team = cells[2].inner_text().strip()
                                    pos_text = cells[3].inner_text().strip()
                                    pos_id = position_mapping.get(pos_text, pos_text)
                                    team_id = resolve_team_code(row_team, year) or row_team

                                    record = {
                                        "player_id": player_id,
                                        "player_name": p_name,
                                        "team_id": team_id,
                                        "year": year,
                                        "position_id": pos_id,
                                        "games": _s_int(cells[4]),
                                        "games_started": _s_int(cells[5]),
                                        "innings": parse_inns(cells[6].inner_text()),
                                        "errors": _s_int(cells[7]),
                                        "pickoffs": _s_int(cells[8]),
                                        "putouts": _s_int(cells[9]),
                                        "assists": _s_int(cells[10]),
                                        "double_plays": _s_int(cells[11]),
                                        "fielding_pct": _s_float(cells[12]),
                                        "source": "CRAWLER",
                                    }
                                    fielding_data_map[(player_id, team_id, pos_id)] = record
                                except Exception:
                                    logger.exception("   ⚠️ 데이터 행 파싱 오류")
                                    continue
                except Exception:
                    logger.exception("   ⚠️ [%s] 처리 중 오류", team_name)
                    continue

            # 2. 포수 상세 수집 (전체 팀, 17개 컬럼)
            logger.info("\n🏃 [상세] 포수 전문 지표 수집 중 (전체 팀)...")
            try:
                # 페이지 초기화
                page.goto(url, wait_until="load", timeout=LONG_TIMEOUT)
                page.wait_for_load_state("networkidle", timeout=SEL_TIMEOUT)
                policy.delay()

                # 1단계: 포지션 "포수(2)" 선택
                with page.expect_response("**/Record/Player/Defense/Basic.aspx", timeout=20000):
                    page.select_option("select#cphContents_cphContents_cphContents_ddlPos_ddlPos", value="2")
                page.wait_for_load_state("networkidle", timeout=SEL_TIMEOUT)
                policy.delay()

                # 2단계: 팀 "전체" 선택 (이미 전체일 수도 있으므로 확인 후 선택)
                team_val = page.evaluate(
                    "document.querySelector('select#cphContents_cphContents_cphContents_ddlTeam_ddlTeam').value",
                )
                if team_val != "":
                    with page.expect_response("**/Record/Player/Defense/Basic.aspx", timeout=20000):
                        page.select_option("select#cphContents_cphContents_cphContents_ddlTeam_ddlTeam", value="")
                    page.wait_for_load_state("networkidle", timeout=SEL_TIMEOUT)
                    policy.delay()

                # 페이지네이션 (포수가 많을 경우 대비)
                pagination = page.query_selector(".paging")
                total_pages = 1
                if pagination:
                    p_nums = [
                        int(link.inner_text().strip())
                        for link in pagination.query_selector_all("a")
                        if link.inner_text().strip().isdigit()
                    ]
                    if p_nums:
                        total_pages = max(p_nums)

                for current_page in range(1, total_pages + 1):
                    logger.info("   📄 포수 상세 페이지 %s/%s 크롤링 중...", current_page, total_pages)
                    if current_page > 1:
                        p_link = next(
                            (
                                link
                                for link in page.query_selector(".paging").query_selector_all("a")
                                if link.inner_text().strip() == str(current_page)
                            ),
                            None,
                        )
                        if p_link:
                            with page.expect_response("**/Record/Player/Defense/Basic.aspx", timeout=20000):
                                p_link.click()
                            page.wait_for_load_state("networkidle", timeout=SEL_TIMEOUT)
                            policy.delay()

                    table = page.query_selector("table.tData01.tt")
                    if not table or not table.query_selector("tbody"):
                        continue

                    for row in table.query_selector("tbody").query_selector_all("tr"):
                        cells = row.query_selector_all("td")
                        if len(cells) >= 17:
                            player_link = cells[1].query_selector("a")
                            player_id = (
                                player_link.get_attribute("href").split("playerId=")[1].split("&")[0]
                                if player_link
                                else None
                            )
                            if not player_id:
                                continue

                            row_team = cells[2].inner_text().strip()
                            team_id = resolve_team_code(row_team, year) or row_team
                            key = (player_id, team_id, "C")

                            if key in fielding_data_map:
                                fielding_data_map[key].update(
                                    {
                                        "passed_balls": _s_int(cells[13]),
                                        "stolen_bases_allowed": _s_int(cells[14]),
                                        "caught_stealing": _s_int(cells[15]),
                                        "cs_pct": _s_float(cells[16]),
                                    },
                                )
                            else:
                                logger.warning(
                                    "   ⚠️ 포수 상세 데이터에 없던 선수 발견 (player_id=%s, team=%s) - 추가 수집 생략",
                                    player_id,
                                    row_team,
                                )
            except Exception:
                logger.exception("   ⚠️ 포수 상세 수집 실패")

            fielding_data = list(fielding_data_map.values())
            summary, fielding_data = build_fielding_crawl_summary(fielding_data)
            logger.info("\n✅ 총 %s개의 수비 기록 수집 완료!", len(fielding_data))

        except Exception:
            logger.exception("⚠️ 수비 기록 크롤링 중 오류")
            import traceback

            traceback.print_exc()

        browser.close()

    return fielding_data


def save_fielding_stats(year=None, db_path=None) -> None:
    """
    수비 기록을 크롤링하여 DB에 저장합니다.

    Args:
        year: 시즌 연도 (None이면 현재 연도)
        db_path: 데이터베이스 파일 경로 (None이면 data/kbo_{year}.db)
    """
    if year is None:
        year = datetime.now().year
    if db_path is None:
        db_path = f"data/kbo_{year}.db"
    # 수비 기록 크롤링
    fielding_records = crawl_all_fielding_stats(year)

    if not fielding_records:
        logger.warning("⚠️ 수집된 수비 기록이 없습니다.")
        return

    # DB 저장
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    saved_count = 0
    skipped_count = 0

    for record in fielding_records:
        if not record["player_id"]:
            skipped_count += 1
            continue

        try:
            cursor.execute(
                """
                INSERT OR REPLACE INTO player_season_fielding
                (player_id, team_id, year, position_id, games, games_started,
                 innings, putouts, assists, errors, double_plays, fielding_pct, pickoffs,
                 passed_balls, stolen_bases_allowed, caught_stealing, cs_pct,
                 updated_at, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    record["player_id"],
                    record["team_id"],
                    year,
                    record["position_id"],
                    record["games"],
                    record["games_started"],
                    record["innings"],
                    record["putouts"],
                    record["assists"],
                    record["errors"],
                    record["double_plays"],
                    record["fielding_pct"],
                    record.get("pickoffs", 0),
                    record.get("passed_balls"),
                    record.get("stolen_bases_allowed"),
                    record.get("caught_stealing"),
                    record.get("cs_pct"),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "CRAWLER",
                ),
            )

            saved_count += 1

        except Exception:
            logger.exception(f"⚠️ DB 저장 오류: {record['player_name']}")  # noqa: G004
            skipped_count += 1
            continue

    conn.commit()
    conn.close()

    logger.info("✅ 수비 기록 저장 완료! (저장: %s건, 스킵: %s건)", saved_count, skipped_count)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="KBO Fielding stats crawler")
    parser.add_argument("--year", type=int, default=None, help="Season year (default: current year)")
    parser.add_argument("--save", action="store_true", help="Save to local database")
    args = parser.parse_args()
    year = args.year or datetime.now().year

    logger.info("📊 수비 크롤러 실행 (연도: %s, 저장 여부: %s)", year, args.save)
    if args.save:
        save_fielding_stats(year)
    else:
        data = crawl_all_fielding_stats(args.year)
        logger.info("🔍 Dry-run completed: %s records found.", len(data))
