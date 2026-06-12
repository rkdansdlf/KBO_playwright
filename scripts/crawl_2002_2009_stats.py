"""
Crawler for 2002-2009 batting and pitching stats.
Adapts the robust 2001 crawler logic to loop through years.
"""

import logging
import os
import sys
import time

logger = logging.getLogger(__name__)

sys.path.insert(0, os.getcwd())

from playwright.sync_api import sync_playwright
from src.repositories.safe_pitching_repository import save_pitching_stats_safe

from src.crawlers.player_batting_all_series_crawler import _build_batting_data
from src.crawlers.player_batting_all_series_crawler import (
    get_series_mapping as get_batting_series_mapping,
)
from src.crawlers.player_pitching_all_series_crawler import _build_pitching_data
from src.crawlers.player_pitching_all_series_crawler import (
    get_series_mapping as get_pitching_series_mapping,
)
from src.repositories.safe_batting_repository import save_batting_stats_safe
from src.utils.team_codes import resolve_team_code

# Custom extraction scripts
EXTRACT_BATTING_JS = r"""
    () => {
        const table = document.querySelector('table.tData01') || document.querySelector('.record_table table') || document.querySelector('table');
        if (!table) return { error: "Table not found" };

        const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.innerText.trim());
        const is_basic2 = headers.join('').includes('BB') || headers.join('').includes('볼넷');

        const rows = Array.from(table.querySelectorAll('tbody tr'));
        const results = [];

        rows.forEach(row => {
            const cells = Array.from(row.querySelectorAll('td'));
            if (cells.length < 10) return;

            const nameCell = cells[1];
            const a = nameCell.querySelector('a');
            if (!a) return;

            const name = a.innerText.trim();
            const href = a.getAttribute('href') || "";
            const idMatch = href.match(/playerId=(\d+)/);
            const playerId = idMatch ? parseInt(idMatch[1], 10) : null;

            if (!playerId) return;

            results.push({
                player_id: playerId,
                player_name: name,
                team_name: cells[2].innerText.trim(),
                cells: cells.map(c => c.innerText.trim()),
                is_basic2: is_basic2
            });
        });

        return { results };
    }
"""

EXTRACT_PITCHING_JS = r"""
    () => {
        const table = document.querySelector('table.tData01') || document.querySelector('.record_table table') || document.querySelector('table');
        if (!table) return { error: "Table not found" };

        const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.innerText.trim());
        const is_basic2 = headers.join('').includes('W') || headers.join('').includes('승');

        const rows = Array.from(table.querySelectorAll('tbody tr'));
        const results = [];

        rows.forEach(row => {
            const cells = Array.from(row.querySelectorAll('td'));
            if (cells.length < 10) return;

            const nameCell = cells[1];
            const a = nameCell.querySelector('a');
            if (!a) return;

            const name = a.innerText.trim();
            const href = a.getAttribute('href') || "";
            const idMatch = href.match(/playerId=(\d+)/);
            const playerId = idMatch ? parseInt(idMatch[1], 10) : null;

            if (!playerId) return;

            results.push({
                player_id: playerId,
                player_name: name,
                team_name: cells[2].innerText.trim(),
                cells: cells.map(c => c.innerText.trim()),
                is_basic2: is_basic2
            });
        });

        return { results };
    }
"""


def crawl_stats_for_year(page, year, mode="batting"):
    """
    Crawls stats for a specific year and mode (batting/pitching).
    Returns list of data dicts.
    """
    if mode == "batting":
        url = "https://www.koreabaseball.com/Record/Player/HitterBasic/BasicOld.aspx"
        series_key = "regular"
        mapping = get_batting_series_mapping()
        extract_js = EXTRACT_BATTING_JS
        build_func = _build_batting_data
    else:
        url = "https://www.koreabaseball.com/Record/Player/PitcherBasic/BasicOld.aspx"
        series_key = "regular"
        mapping = get_pitching_series_mapping()
        extract_js = EXTRACT_PITCHING_JS
        build_func = _build_pitching_data

    series_info = mapping[series_key]
    league_name = series_info.get("league") or series_info.get("league_name") or "REGULAR"

    logger.info("📡 %s년 %s 데이터 크롤링 시작 (%s)", year, mode, url)
    try:
        page.goto(url, wait_until="load")
        time.sleep(1)

        # Select Year
        season_selector = 'select[name*="ddlSeason"]'
        page.select_option(season_selector, str(year))
        page.wait_for_load_state("networkidle")
        time.sleep(2)

        # Get Teams
        team_selector = 'select[name*="ddlTeam"]'
        options = page.eval_on_selector_all(
            f"{team_selector} option", "options => options.map(o => ({text: o.innerText, value: o.value}))"
        )
        teams = [opt for opt in options if opt["value"]]

        all_players = {}

        for tm in teams:
            logger.info("   🔍 %s (%s) 파싱 중...", tm["text"], tm["value"])

            # Select team
            page.select_option(team_selector, tm["value"])
            page.wait_for_load_state("networkidle")
            time.sleep(1.5)

            # Reset to Page 1
            try:
                page1_btn = page.query_selector('.paging a[id*="btnNo1"]')
                if page1_btn:
                    page1_btn.click()
                    page.wait_for_load_state("networkidle")
                    time.sleep(1)
            except Exception:  # noqa: BLE001
                logger.debug("Failed to reset pagination to page 1; continuing from current page")

            page_num = 1
            while True:
                # Custom Extraction
                try:
                    res = page.evaluate(extract_js)
                    if "error" in res:
                        logger.warning("      ⚠️ 테이블 못찾음: %s", res["error"])
                        break

                    players = []
                    for r in res["results"]:
                        team_code = resolve_team_code(r["team_name"], year) or r["team_name"]
                        data = build_func(
                            cells=r["cells"],
                            player_id=r["player_id"],
                            player_name=r["player_name"],
                            team_code=team_code,
                            series_key=series_key,
                            is_basic2=r["is_basic2"],
                        )
                        if data:
                            data["season"] = year
                            data["league"] = league_name
                            data["level"] = "KBO1"
                            data["source"] = "CRAWLER"
                            players.append(data)

                    len(all_players)
                    for p in players:
                        if p and p.get("player_id"):
                            all_players[p["player_id"]] = p
                    len(all_players)

                    # print(f"      📄 {page_num}페이지: {len(players)}명 (누적: {count_after}명)")

                except Exception as e:  # noqa: BLE001
                    logger.warning("      ⚠️ 파싱 에러: %s", e)

                # Pagination
                next_page_num = page_num + 1
                page_btn_selector = (
                    f'#cphContents_cphContents_cphContents_udpRecord .paging a[id*="btnNo{next_page_num}"]'
                )
                if not page.query_selector(page_btn_selector):
                    page_btn_selector = f'.paging a[href*="btnNo{next_page_num}"]'

                btn = page.query_selector(page_btn_selector)
                if btn:
                    first_player_before = page.evaluate(
                        "() => document.querySelector('table.tData01 tbody tr td:nth-child(2)')?.innerText.trim()"
                    )
                    btn.click()
                    try:
                        page.wait_for_function(
                            "oldName => document.querySelector('table.tData01 tbody tr td:nth-child(2)')?.innerText.trim() !== oldName",
                            arg=first_player_before,
                            timeout=5000,
                        )
                    except Exception:  # noqa: BLE001
                        logger.debug("Timed out waiting for next page table update; continuing")
                    page_num += 1
                else:
                    break

        return list(all_players.values())

    except Exception as e:  # noqa: BLE001
        logger.error("❌ %s년 %s 크롤링 치명적 오류: %s", year, mode, e)
        return []


def main():
    years = list(range(2002, 2010))  # 2002 ~ 2009

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        for year in years:
            logger.info("🗓️ YEAR %s Handling...", year)

            # 1. Batting
            batting_data = crawl_stats_for_year(page, year, "batting")
            if batting_data:
                save_batting_stats_safe(batting_data)
                logger.info("✅ %s년 타자 데이터 %s건 저장 완료", year, len(batting_data))
            else:
                logger.warning("⚠️ %s년 타자 데이터 없음", year)

            # 2. Pitching
            pitching_data = crawl_stats_for_year(page, year, "pitching")
            if pitching_data:
                save_pitching_stats_safe(pitching_data)
                logger.info("✅ %s년 투수 데이터 %s건 저장 완료", year, len(pitching_data))
            else:
                logger.warning("⚠️ %s년 투수 데이터 없음", year)

        browser.close()


if __name__ == "__main__":
    main()
