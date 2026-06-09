"""
Robust 2001 batting crawler with ultra-specific selectors and RELIABLE pagination & state reset.
FIXED: Now includes 'season' and 'league' in the data payload (mapped from 'league' key).
"""

import os
import sys
import time

sys.path.insert(0, os.getcwd())

from playwright.sync_api import sync_playwright

from src.crawlers.player_batting_all_series_crawler import _build_batting_data, get_series_mapping
from src.repositories.safe_batting_repository import save_batting_stats_safe
from src.utils.team_codes import resolve_team_code


def extract_players_custom(page, series_key, year, league_name):
    """Custom JS extraction for BasicOld.aspx"""
    script = r"""
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
    try:
        res = page.evaluate(script)
        if "error" in res:
            return []

        players = []
        for r in res["results"]:
            team_code = resolve_team_code(r["team_name"], year) or r["team_name"]
            data = _build_batting_data(
                cells=r["cells"],
                player_id=r["player_id"],
                player_name=r["player_name"],
                team_code=team_code,
                series_key=series_key,
                is_basic2=r["is_basic2"],
            )
            data["season"] = year
            # Map 'league' (e.g. 'REGULAR') to the season/league field
            data["league"] = league_name
            data["level"] = "KBO1"
            data["source"] = "CRAWLER"

            players.append(data)
        return players
    except Exception as e:  # noqa: BLE001
        print(f"⚠️ Custom extraction error: {e}")
        return []


def robust_crawl_2001():
    year = 2001
    series_key = "regular"
    mapping = get_series_mapping()
    series_info = mapping[series_key]
    league_name = series_info.get("league") or series_info.get("league_name") or "REGULAR"

    url = "https://www.koreabaseball.com/Record/Player/HitterBasic/BasicOld.aspx"

    all_players = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        print(f"📡 {url} 접속 중...")
        page.goto(url, wait_until="load")
        time.sleep(2)

        # Select Year
        season_selector = 'select[name*="ddlSeason"]'
        page.select_option(season_selector, str(year))
        page.wait_for_load_state("networkidle")
        time.sleep(3)

        team_selector = 'select[name*="ddlTeam"]'
        options = page.eval_on_selector_all(
            f"{team_selector} option", "options => options.map(o => ({text: o.innerText, value: o.value}))"
        )
        teams = [opt for opt in options if opt["value"]]

        print(f"ℹ️ {len(teams)}개 팀 발견")

        for tm in teams:
            print(f"🔍 팀 선택: {tm['text']} ({tm['value']})")

            # Select team
            page.select_option(team_selector, tm["value"])
            page.wait_for_load_state("networkidle")
            time.sleep(3)

            # Reset to Page 1 EXPLICITLY
            try:
                page1_btn = page.query_selector('.paging a[id*="btnNo1"]')
                if page1_btn:
                    print("   ↩️ 1페이지로 리셋 중...")
                    page1_btn.click()
                    page.wait_for_load_state("networkidle")
                    time.sleep(2)
            except Exception:  # noqa: BLE001
                pass

            page_num = 1
            while True:
                print(f"   📄 {tm['text']} - {page_num}페이지 파싱 중...")

                # Get first player to detect page change later
                first_player_before = page.evaluate(
                    "() => document.querySelector('table.tData01 tbody tr td:nth-child(2)')?.innerText.trim()"
                )

                players = extract_players_custom(page, series_key, year, league_name)

                count_before = len(all_players)
                for p_data in players:
                    pid = p_data.get("player_id")
                    if pid:
                        all_players[pid] = p_data

                count_after = len(all_players)
                print(f"   ✅ {len(players)}명 파싱 (신규: {count_after - count_before}명)")

                # Pagination
                next_page_num = page_num + 1
                page_btn_selector = (
                    f'#cphContents_cphContents_cphContents_udpRecord .paging a[id*="btnNo{next_page_num}"]'
                )

                if not page.query_selector(page_btn_selector):
                    page_btn_selector = f'.paging a[href*="btnNo{next_page_num}"]'

                btn = page.query_selector(page_btn_selector)
                if btn:
                    print(f"   ➡️ {next_page_num}페이지로 이동 중...")
                    btn.click()

                    try:
                        page.wait_for_function(
                            "oldName => document.querySelector('table.tData01 tbody tr td:nth-child(2)')?.innerText.trim() !== oldName",
                            arg=first_player_before,
                            timeout=5000,
                        )
                    except Exception:  # noqa: BLE001
                        pass

                    time.sleep(2)
                    page_num += 1
                else:
                    print(f"   🏁 {tm['text']} 수집 완료")
                    break

        browser.close()

    final_list = list(all_players.values())
    print(f"✅ 총 {len(final_list)}명 수집 완료")

    if final_list:
        save_batting_stats_safe(final_list)
        print("💾 DB 저장 완료")


if __name__ == "__main__":
    robust_crawl_2001()
