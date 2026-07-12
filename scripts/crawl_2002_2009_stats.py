"""Crawler for 2002-2009 batting and pitching stats.
Adapts the robust 2001 crawler logic to loop through years.
"""

import logging
import sys
import time
from pathlib import Path

logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path.cwd()))

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright
from typing import Any
from src.repositories.player_season_pitching_repository import save_pitching_stats_to_db as save_pitching_stats_safe
from src.crawlers.player_batting_all_series_crawler import (
    get_series_mapping as get_batting_series_mapping,
)
from src.crawlers.player_pitching_all_series_crawler import (  # type: ignore[attr-defined]
    get_series_mapping as get_pitching_series_mapping,
)
from src.repositories.safe_batting_repository import save_batting_stats_safe


def safe_parse_number(val_str: str | None, data_type: type) -> Any:
    if not val_str:
        return None
    val_str = val_str.strip()
    if not val_str or val_str in ["-", "N/A", ""]:
        return None
    try:
        return data_type(val_str)
    except (ValueError, TypeError):
        return None


def _build_batting_data(
    cells: list[str],
    player_id: int,
    player_name: str,
    team_code: str,
    series_key: str,
    is_basic2: bool,
) -> dict[str, Any]:
    def cell(idx: int) -> str | None:
        return cells[idx] if len(cells) > idx else None

    # wide table old stats mapping:
    return {
        "player_id": player_id,
        "player_name": player_name,
        "team_code": team_code,
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
        "walks": safe_parse_number(cell(16), int),
        "hbp": safe_parse_number(cell(17), int),
        "strikeouts": safe_parse_number(cell(18), int),
        "gdp": safe_parse_number(cell(19), int),
        "slg": safe_parse_number(cell(20), float),
        "obp": safe_parse_number(cell(21), float),
        "ops": safe_parse_number(cell(22), float),
    }


def _build_pitching_data(
    cells: list[str],
    player_id: int,
    player_name: str,
    team_code: str,
    series_key: str,
    is_basic2: bool,
) -> dict[str, Any]:
    def cell(idx: int) -> str | None:
        return cells[idx] if len(cells) > idx else None

    ip_str = cell(10)
    innings_pitched = None
    innings_outs = None
    if ip_str:
        try:
            parts = ip_str.strip().split()
            whole = 0.0
            fraction = 0.0
            if len(parts) == 2:
                whole = float(parts[0])
                frac_str = parts[1]
            else:
                frac_str = parts[0]
                if "/" not in frac_str and "." not in frac_str:
                    whole = float(frac_str)
                    frac_str = ""

            if "/" in frac_str:
                num, denom = map(float, frac_str.split("/"))
                fraction = num / denom
            elif "." in frac_str:
                whole = float(frac_str)

            innings_pitched = whole + fraction
            whole_int = int(whole)
            outs = whole_int * 3
            if "/" in frac_str:
                num, denom = map(int, frac_str.split("/"))
                outs += num
            innings_outs = outs
        except Exception:
            logger.debug("Failed to parse innings pitched value: %s", ip_str, exc_info=True)

    return {
        "player_id": player_id,
        "player_name": player_name,
        "team_code": team_code,
        "era": safe_parse_number(cell(3), float),
        "games": safe_parse_number(cell(4), int),
        "wins": safe_parse_number(cell(5), int),
        "losses": safe_parse_number(cell(6), int),
        "saves": safe_parse_number(cell(7), int),
        "holds": safe_parse_number(cell(8), int),
        "innings_pitched": innings_pitched,
        "innings_outs": innings_outs,
        "hits_allowed": safe_parse_number(cell(11), int),
        "home_runs_allowed": safe_parse_number(cell(12), int),
        "walks_allowed": safe_parse_number(cell(13), int),
        "hit_batters": safe_parse_number(cell(14), int),
        "strikeouts": safe_parse_number(cell(15), int),
        "runs_allowed": safe_parse_number(cell(16), int),
        "earned_runs": safe_parse_number(cell(17), int),
        "whip": safe_parse_number(cell(18), float),
    }


from src.utils.team_codes import resolve_team_code

CRAWLER_EXCEPTIONS = (PlaywrightError, PlaywrightTimeoutError, RuntimeError, ValueError, TypeError, KeyError, OSError)

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


def _setup_page(page, year, mode):
    if mode == "batting":
        url = "https://www.koreabaseball.com/Record/Player/HitterBasic/BasicOld.aspx"
        mapping = get_batting_series_mapping()
        extract_js = EXTRACT_BATTING_JS
        build_func = _build_batting_data
    else:
        url = "https://www.koreabaseball.com/Record/Player/PitcherBasic/BasicOld.aspx"
        mapping = get_pitching_series_mapping()
        extract_js = EXTRACT_PITCHING_JS
        build_func = _build_pitching_data
    series_key = "regular"
    series_info = mapping[series_key]
    league_name = series_info.get("league") or series_info.get("league_name") or "REGULAR"

    page.goto(url, wait_until="load")
    time.sleep(1)
    page.select_option('select[name*="ddlSeason"]', str(year))
    page.wait_for_load_state("networkidle")
    time.sleep(2)
    return series_key, league_name, extract_js, build_func


def _parse_page(page, extract_js, build_func, year, series_key, league_name, all_players):
    res = page.evaluate(extract_js)
    if "error" in res:
        logger.warning("      ⚠️ 테이블 못찾음: %s", res["error"])
        return True
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
            data.update(season=year, league=league_name, level="KBO1", source="CRAWLER")
            all_players[data["player_id"]] = data
    return False


def _go_to_next_page(page, page_num):
    next_page_num = page_num + 1
    sel = f'#cphContents_cphContents_cphContents_udpRecord .paging a[id*="btnNo{next_page_num}"]'
    if not page.query_selector(sel):
        sel = f'.paging a[href*="btnNo{next_page_num}"]'
    btn = page.query_selector(sel)
    if not btn:
        return None
    first_player_before = page.evaluate(
        "() => document.querySelector('table.tData01 tbody tr td:nth-child(2)')?.innerText.trim()",
    )
    btn.click()
    try:
        page.wait_for_function(
            "oldName => document.querySelector('table.tData01 tbody tr td:nth-child(2)')?.innerText.trim() !== oldName",
            arg=first_player_before,
            timeout=5000,
        )
    except CRAWLER_EXCEPTIONS:
        logger.debug("Timed out waiting for next page table update; continuing")
    return next_page_num


def crawl_stats_for_year(page, year, mode="batting"):
    """Crawls stats for a specific year and mode (batting/pitching)."""
    series_key, league_name, extract_js, build_func = _setup_page(page, year, mode)
    logger.info("📡 %s년 %s 데이터 크롤링 시작", year, mode)

    try:
        team_selector = 'select[name*="ddlTeam"]'
        options = page.eval_on_selector_all(
            f"{team_selector} option",
            "options => options.map(o => ({text: o.innerText, value: o.value}))",
        )
        teams = [opt for opt in options if opt["value"]]
        all_players: dict[int, dict] = {}

        for tm in teams:
            page.select_option(team_selector, tm["value"])
            page.wait_for_load_state("networkidle")
            time.sleep(1.5)
            page1_btn = page.query_selector('.paging a[id*="btnNo1"]')
            if page1_btn:
                page1_btn.click()
                page.wait_for_load_state("networkidle")
                time.sleep(1)

            page_num = 1
            while True:
                try:
                    if _parse_page(page, extract_js, build_func, year, series_key, league_name, all_players):
                        break
                except CRAWLER_EXCEPTIONS as e:
                    logger.warning("      ⚠️ 파싱 에러: %s", e)

                next_pn = _go_to_next_page(page, page_num)
                if next_pn is None:
                    break
                page_num = next_pn

        return list(all_players.values())
    except CRAWLER_EXCEPTIONS:
        logger.exception("❌ %s년 %s 크롤링 치명적 오류", year, mode)
        return []


def main():
    years = list(range(2002, 2010))  # 2002 ~ 2009

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
