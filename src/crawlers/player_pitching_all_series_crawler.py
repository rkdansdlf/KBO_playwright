"""
KBO 전체 시리즈 투수 기록 크롤러

요구사항 요약:
1. https://www.koreabaseball.com/Record/Player/PitcherBasic/Basic1.aspx 페이지에서
   - 시즌/시리즈 선택 후 `G`(경기) 헤더를 클릭하여 정렬
   - 모든 페이지를 순회하며 정규시즌 투수 기본 기록 수집
2. https://www.koreabaseball.com/Record/Player/PitcherBasic/Basic2.aspx 페이지에서
   - `CG, SHO, QS, BSV, TBF, NP, AVG, 2B, 3B, SAC, SF, IBB, WP, BK` 헤더를 순서대로 클릭
   - 각 정렬마다 전체 페이지를 순회하며 추가 지표 수집 및 기존 데이터 업데이트
3. Docs/schema/KBO_시즌별 투수기록 테이블.csv에 정의된 스키마에 맞춰 데이터 정리
4. 필요 시 Supabase(PostgreSQL)에 UPSERT 저장 (season_id + player_id 기준)

Usage:
    python -m src.crawlers.pitching_stats_crawler --year 2025 --series regular --save --sync-supabase
"""
from __future__ import annotations

import argparse
import os
import re
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeout

from src.repositories.player_season_pitching_repository import save_pitching_stats_to_db
from src.utils.team_codes import resolve_team_code
from src.utils.playwright_blocking import install_sync_resource_blocking
from src.utils.request_policy import RequestPolicy
from src.utils.compliance import compliance




# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------

BASIC1_URL = "https://www.koreabaseball.com/Record/Player/PitcherBasic/Basic1.aspx"
BASIC2_URL = "https://www.koreabaseball.com/Record/Player/PitcherBasic/Basic2.aspx"

BASIC1_SORT_CODE = "IP_CN"  # 'IP' (이닝) 헤더

# 정규시즌 Basic2에서는 NP(투구수)만 수집
BASIC2_SORT_SEQUENCE = [
    ("NP", "PIT_CN"),  # 투구수
]

SERIES_MAPPING: Dict[str, Dict[str, str]] = {
    "regular": {
        "name": "KBO 정규시즌",
        "value": "0",
        "league": "REGULAR",
    },
    "exhibition": {
        "name": "KBO 시범경기",
        "value": "1",
        "league": "EXHIBITION",
    },
    "wildcard": {
        "name": "KBO 와일드카드",
        "value": "4",
        "league": "WILDCARD",
    },
    "semi_playoff": {
        "name": "KBO 준플레이오프",
        "value": "3",
        "league": "SEMI_PLAYOFF",
    },
    "playoff": {
        "name": "KBO 플레이오프",
        "value": "5",
        "league": "PLAYOFF",
    },
    "korean_series": {
        "name": "KBO 한국시리즈",
        "value": "7",
        "league": "KOREAN_SERIES",
    },
}

PRIMARY_SORT_CONFIG = {
    "regular": {"label": "IP", "sort_code": "IP_CN"},
    "default": {"label": "IP", "sort_code": "IP_CN"},
}

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def normalize_header(text: str) -> str:
    if text is None:
        return ""
    cleaned = text.replace('\xa0', ' ').strip()
    if '\n' in cleaned:
        cleaned = cleaned.split('\n')[0].strip()
    parts = cleaned.split()
    if len(parts) > 1:
        cleaned = parts[0]
    return cleaned


def safe_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    cleaned = value.replace(",", "").strip()
    if cleaned in {"", "-", "–"}:
        return None
    try:
        return int(float(cleaned))
    except ValueError:
        return None


def safe_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    cleaned = value.replace(",", "").strip()
    if cleaned in {"", "-", "–"}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_innings(value: Optional[str]) -> Tuple[Optional[float], Optional[int]]:
    """
    Convert inning string (e.g. '180 2/3') into (innings_float, outs_int).
    """
    if value is None:
        return None, None
    cleaned = value.replace(",", "").strip()
    if cleaned in {"", "-", "–"}:
        return None, None

    innings_float: Optional[float] = None
    outs: Optional[int] = None

    try:
        main_part = cleaned
        fraction_part = ""
        if " " in cleaned:
            main_part, fraction_part = cleaned.split()
        elif "/" in cleaned:
            main_part, fraction_part = "0", cleaned

        # main innings
        main_int = int(float(main_part))
        outs = main_int * 3

        frac_value = 0.0
        if fraction_part:
            if "/" in fraction_part:
                num, den = fraction_part.split("/")
                num_i, den_i = int(num), int(den)
                outs += int(round(num_i * 3 / den_i))
                frac_value = num_i / den_i
            else:
                # decimal form (rare)
                frac_value = float(fraction_part)
                outs += int(round(frac_value * 3))
        innings_float = main_int + frac_value

        # handle decimals without space (e.g., '12.1')
        if not fraction_part and "." in cleaned:
            innings_float = float(cleaned)
            fractional = innings_float - int(innings_float)
            if abs(fractional - 0.1) < 0.05:
                outs = int(innings_float) * 3 + 1
            elif abs(fractional - 0.2) < 0.05:
                outs = int(innings_float) * 3 + 2
            else:
                outs = int(round(innings_float * 3))

        return round(innings_float, 2) if innings_float is not None else None, outs
    except (ValueError, ZeroDivisionError):
        return None, None


def extract_player_id(href: Optional[str]) -> Optional[int]:
    if not href:
        return None
    match = re.search(r"playerId=(\d+)", href)
    return int(match.group(1)) if match else None


def _extract_rows_fast(page: Page, table_selector: str = "table.tData01.tt") -> Optional[List[Dict[str, object]]]:
    try:
        payload = page.evaluate(
            """
            (selector) => {
                const table = document.querySelector(selector);
                if (!table) return null;
                const body = table.tBodies && table.tBodies.length ? table.tBodies[0] : table;
                const rows = Array.from(body.querySelectorAll('tr'));
                return rows.map((row) => {
                    const cells = Array.from(row.querySelectorAll('td')).map(td => (td.innerText || '').trim());
                    const link = row.querySelector('a');
                    return {
                        cells,
                        linkText: link ? (link.innerText || '').trim() : null,
                        linkHref: link ? link.getAttribute('href') : null,
                    };
                });
            }
            """,
            table_selector,
        )
        return payload or []
    except Exception:
        return None


def wait_for_table(page: Page, timeout: int = 30000) -> None:
    try:
        page.wait_for_selector(
            "table.tData01.tt tbody tr",
            timeout=timeout,
            state="attached",
        )
    except PlaywrightTimeout:
        print("   ⚠️  테이블 행이 표시되지 않았습니다. (데이터 없음 가능성)")
    finally:
        page.wait_for_timeout(500)


def go_to_next_page(page: Page, current_page: int) -> bool:
    """
    다음 페이지로 이동 (1→2,3,4,5→다음→6,7,8,9,10→다음 반복)
    타자 크롤러와 동일한 개선된 로직
    """
    try:
        # 1→2,3,4,5→다음→6,7,8,9,10→다음 패턴
        if current_page % 5 == 0:  # 5페이지마다 "다음" 버튼 클릭
            # 다음 버튼 찾기
            next_button_selector = 'a[href*="btnNext"]'
            next_button = page.query_selector(next_button_selector)
            
            if not next_button:
                print("   📄 다음 페이지 버튼을 찾을 수 없습니다.")
                return False
            
            # 버튼이 비활성화되어 있는지 확인
            disabled_attr = next_button.get_attribute("disabled")
            class_attr = next_button.get_attribute("class") or ""
            if disabled_attr or "disabled" in class_attr:
                print("   📄 마지막 페이지에 도달했습니다.")
                return False
            
            print(f"   ➡️ 다음 버튼 클릭 ({current_page}페이지 후)")
            next_button.click()
            page.wait_for_load_state('networkidle', timeout=30000)
            page.wait_for_timeout(2000)  # 2초 대기
            
        else:
            # 5페이지 내에서 번호 버튼 클릭
            next_page = current_page + 1
            relative = ((next_page - 1) % 5) + 1
            selector = f'a[href*="btnNo{relative}"]'
            page_button = page.query_selector(selector)
            
            if not page_button:
                print(f"   📄 {relative}번 페이지 버튼을 찾을 수 없습니다.")
                return False
            
            print(f"   ➡️ {relative}번 페이지 버튼 클릭")
            page_button.click()
            page.wait_for_load_state("networkidle", timeout=30000)
            page.wait_for_timeout(1000)  # 1초 대기
        
        # 페이지 이동 후 테이블 대기
        wait_for_table(page)
        return True
        
    except PlaywrightTimeout as e:
        print(f"   ⚠️ 페이지 이동 중 타임아웃: {e}")
        return False
    except Exception as e:
        print(f"   ⚠️ 페이지 이동 중 오류: {e}")
        return False


def apply_sort(page: Page, header_label: str, sort_code: Optional[str] = None) -> bool:
    if sort_code:
        selector = f"a[href=\"javascript:sort('{sort_code}');\"]"
        anchor = page.query_selector(selector)
        if anchor:
            anchor.click()
            page.wait_for_load_state("networkidle", timeout=60000)
            page.wait_for_timeout(800)
            return True

    anchors = page.query_selector_all("table.tData01.tt thead a")
    for anchor in anchors:
        label = normalize_header(anchor.inner_text())
        if label == header_label:
            anchor.click()
            page.wait_for_load_state("networkidle", timeout=60000)
            page.wait_for_timeout(800)
            return True

    print(f"⚠️  '{header_label}' 정렬 링크를 찾지 못했습니다.")
    return False


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class PitcherStats:
    player_id: int
    season: int
    league: str
    level: str = "KBO1"
    source: str = "CRAWLER"
    player_name: Optional[str] = None
    team_name: Optional[str] = None
    team_code: Optional[str] = None
    games: Optional[int] = None
    games_started: Optional[int] = None
    wins: Optional[int] = None
    losses: Optional[int] = None
    saves: Optional[int] = None
    holds: Optional[int] = None
    innings_pitched: Optional[float] = None
    innings_outs: Optional[int] = None
    hits_allowed: Optional[int] = None
    runs_allowed: Optional[int] = None
    earned_runs: Optional[int] = None
    home_runs_allowed: Optional[int] = None
    walks_allowed: Optional[int] = None
    intentional_walks: Optional[int] = None
    hit_batters: Optional[int] = None
    strikeouts: Optional[int] = None
    wild_pitches: Optional[int] = None
    balks: Optional[int] = None
    era: Optional[float] = None
    whip: Optional[float] = None
    fip: Optional[float] = None
    k_per_nine: Optional[float] = None
    bb_per_nine: Optional[float] = None
    kbb: Optional[float] = None
    extra_stats: Dict[str, object] = field(default_factory=lambda: {"rankings": {}})

    def to_repository_payload(self) -> Dict[str, Optional[object]]:
        """타자 크롤러 방식의 단순 데이터 구조"""
        data = {
            "player_id": self.player_id,
            "season": self.season,
            "league": self.league,
            "level": self.level,
            "source": self.source,
            "team_code": self.team_code,
            # 투수 기본 스탯
            "games": self.games,
            "games_started": self.games_started, 
            "wins": self.wins,
            "losses": self.losses,
            "saves": self.saves,
            "holds": self.holds,
            "innings_pitched": self.innings_pitched,  # 타자처럼 단순 필드명
            "hits_allowed": self.hits_allowed,
            "runs_allowed": self.runs_allowed,
            "earned_runs": self.earned_runs,
            "home_runs_allowed": self.home_runs_allowed,
            "walks_allowed": self.walks_allowed,
            "intentional_walks": self.intentional_walks,
            "hit_batters": self.hit_batters,
            "strikeouts": self.strikeouts,
            "wild_pitches": self.wild_pitches,
            "balks": self.balks,
            "era": self.era,
            "whip": self.whip,
            "extra_stats": self.extra_stats,
        }
        # innings_outs를 extra_stats에 따로 저장
        if self.innings_outs is not None:
            data.setdefault("extra_stats", {})
            if isinstance(data["extra_stats"], dict):
                data["extra_stats"]["innings_outs"] = self.innings_outs
        return data


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_basic1_page(
    page: Page,
    season: int,
    league: str,
    pitchers: Dict[int, PitcherStats],
    max_players: Optional[int] = None,
) -> int:
    headers = [normalize_header(th.inner_text()) for th in page.query_selector_all("table.tData01.tt thead th")]
    header_index = {name: idx for idx, name in enumerate(headers)}
    team_mapping = get_team_mapping_for_year(season)
    use_fast = os.getenv("KBO_FAST_PARSE", "1") != "0"

    core_headers = ["선수명", "팀명", "IP", "G", "ERA"]
    missing_core = [h for h in core_headers if h not in header_index]
    if missing_core:
        print(f"⚠️  Basic1 테이블 헤더에 필수 컬럼이 없습니다: {', '.join(missing_core)}")
        print("   헤더 목록:", headers)
        return 0

    rows_data = _extract_rows_fast(page) if use_fast else None
    rows = rows_data if rows_data is not None else page.query_selector_all("table.tData01.tt tbody tr")
    processed = 0

    for row in rows:
        if rows_data is not None:
            cells = row.get("cells") or []
            if len(cells) < len(headers):
                continue
    """
    Basic1 Page Parsing with JS Fast Path
    """
    
    # JavaScript Payload Extraction
    extraction_script = """
    () => {
        const rows = document.querySelectorAll('table.tData01.tt tbody tr');
        if (rows.length === 0) return [];
        
        const headers = Array.from(document.querySelectorAll('table.tData01.tt thead th')).map(th => th.innerText.trim());
        const headerIndex = {};
        headers.forEach((h, i) => headerIndex[h] = i);
        
        const results = [];
        
        rows.forEach(row => {
            const cells = Array.from(row.querySelectorAll('td'));
            if (cells.length < headers.length) return;
            
            // Player Info (Assuming "선수명" is present)
            let nameIndex = headerIndex["선수명"];
            if (nameIndex === undefined) return;
            
            const nameCell = cells[nameIndex];
            const link = nameCell.querySelector('a');
            if (!link) return; // Should have link
            
            const href = link.getAttribute('href');
            const idMatch = href.match(/playerId=(\\d+)/);
            if (!idMatch) return;
            
            const player_id = parseInt(idMatch[1]);
            const player_name = link.innerText.trim();
            const team_name = cells[headerIndex["팀명"]].innerText.trim();
            
            // Extract raw text for mapping in Python
            const raw = {};
            for (const [key, idx] of Object.entries(headerIndex)) {
                raw[key] = cells[idx].innerText.trim();
            }
            
            results.push({ player_id, player_name, team_name, raw });
        });
        return results;
    }
    """

    try:
        extracted_rows = page.evaluate(extraction_script)
        team_mapping = get_team_mapping_for_year(season)
        processed = 0

        for row in extracted_rows:
            player_id = row['player_id']
            
            if max_players and player_id not in pitchers and len(pitchers) >= max_players:
                continue
            
            player_name = row['player_name']
            team_name = row['team_name']
            raw = row['raw']
            
            # Map Team Code
            team_code = resolve_team_code(team_name, season) or team_name

            stats = pitchers.get(player_id)
            if not stats:
                stats = PitcherStats(
                    player_id=player_id,
                    season=season,
                    league=league,
                )
                pitchers[player_id] = stats
            
            stats.player_name = player_name
            stats.team_name = team_name
            stats.team_code = team_code
            
            # Helper to get raw value safely
            def get_val(key): return raw.get(key)

            stats.games = safe_int(get_val("G")) if "G" in raw else stats.games
            stats.wins = safe_int(get_val("W")) if "W" in raw else stats.wins
            stats.losses = safe_int(get_val("L")) if "L" in raw else stats.losses
            stats.saves = safe_int(get_val("SV")) if "SV" in raw else stats.saves
            stats.holds = safe_int(get_val("HLD")) if "HLD" in raw else stats.holds
            
            if "IP" in raw:
                ip_value, outs_value = parse_innings(get_val("IP"))
                stats.innings_pitched = ip_value
                stats.innings_outs = outs_value

            stats.hits_allowed = safe_int(get_val("H")) if "H" in raw else stats.hits_allowed
            stats.home_runs_allowed = safe_int(get_val("HR")) if "HR" in raw else stats.home_runs_allowed
            stats.walks_allowed = safe_int(get_val("BB")) if "BB" in raw else stats.walks_allowed
            stats.hit_batters = safe_int(get_val("HBP")) if "HBP" in raw else stats.hit_batters
            stats.strikeouts = safe_int(get_val("SO")) if "SO" in raw else stats.strikeouts
            stats.runs_allowed = safe_int(get_val("R")) if "R" in raw else stats.runs_allowed
            stats.earned_runs = safe_int(get_val("ER")) if "ER" in raw else stats.earned_runs
            stats.era = safe_float(get_val("ERA")) if "ERA" in raw else stats.era
            stats.whip = safe_float(get_val("WHIP")) if "WHIP" in raw else stats.whip

            # Extra metrics
            metrics = stats.extra_stats.setdefault("metrics", {})
            
            for header, key in [
                ("CG", "complete_games"), ("SHO", "shutouts"), ("TBF", "tbf"),
            ]:
                if header in raw:
                    val = safe_int(get_val(header))
                    if val is not None: metrics[key] = val
            
            rank_value = safe_int(get_val("순위")) if "순위" in raw else None
            win_pct = safe_float(get_val("WPCT")) if "WPCT" in raw else None
            
            rankings = stats.extra_stats.setdefault("rankings", {})
            rankings["basic1"] = rank_value
            if stats.era is not None: metrics["era"] = stats.era
            if win_pct is not None: metrics["win_pct"] = win_pct
            
            processed += 1
            
        return processed
        
    except Exception as e:
        print(f"❌ Basic1 파싱 오류 (JS): {e}")
        return 0


def parse_basic2_page(
    page: Page,
    season: int,
    league: str,
    pitchers: Dict[int, PitcherStats],
    sort_key: str,
    max_players: Optional[int] = None,
) -> int:
    headers = [normalize_header(th.inner_text()) for th in page.query_selector_all("table.tData01.tt thead th")]
    header_index = {name: idx for idx, name in enumerate(headers)}
    team_mapping = get_team_mapping_for_year(season)
    use_fast = os.getenv("KBO_FAST_PARSE", "1") != "0"

    # Basic2 헤더는 정규시즌과 포스트시즌에서 다를 수 있음
    if "선수명" not in header_index or "팀명" not in header_index:
        print("⚠️  Basic2 테이블 헤더 파싱 실패")
        return 0

    rows_data = _extract_rows_fast(page) if use_fast else None
    rows = rows_data if rows_data is not None else page.query_selector_all("table.tData01.tt tbody tr")
    processed = 0

    for row in rows:
        if rows_data is not None:
            cells = row.get("cells") or []
            if len(cells) < len(headers):
                continue
            link_href = row.get("linkHref")
            player_id = extract_player_id(link_href)
            if not player_id:
                continue

            def cell_text(idx: int) -> Optional[str]:
                return cells[idx] if len(cells) > idx else None

            player_name = (row.get("linkText") or cell_text(header_index["선수명"]) or "").strip()
            team_name = (cell_text(header_index["팀명"]) or "").strip()
        else:
            cells = row.query_selector_all("td")
            if len(cells) < len(headers):
                continue

            def cell_text(idx: int) -> Optional[str]:
                return cells[idx].inner_text() if len(cells) > idx else None

            link = cells[header_index["선수명"]].query_selector("a")
            player_id = extract_player_id(link.get_attribute("href") if link else None)
            if not player_id:
                continue
            player_name = link.inner_text().strip() if link else cells[header_index["선수명"]].inner_text().strip()
            team_name = cells[header_index["팀명"]].inner_text().strip()

        if max_players and player_id not in pitchers and len(pitchers) >= max_players:
            continue

        stats = pitchers.get(player_id)
        if not stats:
            stats = PitcherStats(player_id=player_id, season=season, league=league)
            pitchers[player_id] = stats
            stats.player_name = player_name
            stats.team_name = team_name
            team_code = resolve_team_code(team_name, season) or team_name
            stats.team_code = team_code

        metrics = stats.extra_stats.setdefault("metrics", {})

        def set_metric(header_name: str, key: str, caster):
            if header_name in header_index:
                value = caster(cell_text(header_index[header_name]))
                if value is not None:
                    metrics[key] = value

        set_metric("CG", "complete_games", safe_int)
        set_metric("SHO", "shutouts", safe_int)
        set_metric("QS", "quality_starts", safe_int)
        set_metric("BSV", "blown_saves", safe_int)
        set_metric("TBF", "tbf", safe_int)
        set_metric("NP", "np", safe_int)
        set_metric("AVG", "avg_against", safe_float)
        set_metric("2B", "doubles_allowed", safe_int)
        set_metric("3B", "triples_allowed", safe_int)
        set_metric("SAC", "sacrifices_allowed", safe_int)
        set_metric("SF", "sacrifice_flies_allowed", safe_int)

        if "IBB" in header_index:
            val = safe_int(cell_text(header_index["IBB"]))
            if val is not None:
                stats.intentional_walks = val
        if "WP" in header_index:
            val = safe_int(cell_text(header_index["WP"]))
            if val is not None:
                stats.wild_pitches = val
        if "BK" in header_index:
            val = safe_int(cell_text(header_index["BK"]))
            if val is not None:
                stats.balks = val

        # 랭킹 기록
        rank_val = safe_int(cell_text(header_index.get("순위", 0))) if "순위" in header_index else None
        if rank_val is not None:
            rankings = stats.extra_stats.setdefault("rankings", {})
            rankings[sort_key] = rank_val

        processed += 1

    return processed


# ---------------------------------------------------------------------------
# Crawling logic
# ---------------------------------------------------------------------------

def setup_pitcher_page(page: Page, url: str, year: int, series_value: str, policy: Optional[RequestPolicy] = None) -> bool:
    if policy:
        policy.delay(host="www.koreabaseball.com")
    
    if not compliance.is_allowed_sync(url):
        print(f"[COMPLIANCE] Navigation to {url} aborted.")
        return False

    page.goto(url, wait_until="load", timeout=60000)
    page.wait_for_load_state("networkidle", timeout=60000)
    page.wait_for_timeout(1000)

    try:
        season_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeason$ddlSeason"]'
        series_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlSeries$ddlSeries"]'
        page.select_option(season_selector, str(year))
        page.wait_for_timeout(300)
        page.select_option(series_selector, value=series_value)
        page.wait_for_timeout(500)
        page.wait_for_load_state("networkidle", timeout=60000)
        page.wait_for_timeout(500)
        return True
    except PlaywrightTimeout:
        return False


def crawl_pitcher_series(
    year: int,
    series_key: str,
    limit: Optional[int] = None,
    headless: bool = True,
    save_to_db: bool = False,
) -> List[PitcherStats]:
    if series_key not in SERIES_MAPPING:
        raise ValueError(f"지원하지 않는 시리즈 키: {series_key}")

    series_info = SERIES_MAPPING[series_key]
    league_name = series_info.get("league", "REGULAR")
    print(f"\n📊 {year}년 {series_info['name']} 수집 시작")

    policy = RequestPolicy()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        # Apply UA rotation via context
        context = browser.new_context(**policy.build_context_kwargs(locale='ko-KR'))
        page = context.new_page()
        page.set_default_timeout(60000)
        install_sync_resource_blocking(page)

        # Step 1: Basic1 - 시리즈별 정렬 후 전체 페이지 수집
        if not setup_pitcher_page(page, BASIC1_URL, year, series_info["value"], policy=policy):
            print("❌ Basic1 페이지 설정 실패")
            browser.close()
            return []

        primary_sort = PRIMARY_SORT_CONFIG.get(
            series_key, PRIMARY_SORT_CONFIG["default"]
        )
        apply_sort(
            page,
            header_label=primary_sort["label"],
            sort_code=primary_sort["sort_code"],
        )

        wait_for_table(page)

        page_number = 1
        while True:
            parsed = parse_basic1_page(
                page,
                season=year,
                league=league_name,
                pitchers=pitchers,
                max_players=limit,
            )
            print(f"   ▶ Basic1 {page_number}페이지: {parsed}명 처리 (누적 {len(pitchers)}명)")

            if limit and len(pitchers) >= limit:
                print("   🎯 수집 제한에 도달했습니다.")
                break

            if not go_to_next_page(page, page_number):
                break
            page_number += 1

        print(f"✅ Basic1 수집 완료: 총 {len(pitchers)}명")

        # Step 2: Basic2 (정규시즌만 실행)
        if series_key == "regular":
            if not setup_pitcher_page(page, BASIC2_URL, year, series_info["value"], policy=policy):
                print("⚠️  Basic2 페이지 설정 실패. 추가 지표 없이 종료합니다.")
                browser.close()
                return list(pitchers.values()) if not limit else list(pitchers.values())[:limit]

            for display_name, sort_code in BASIC2_SORT_SEQUENCE:
                if not apply_sort(page, display_name, sort_code):
                    continue
                wait_for_table(page)

                page_number = 1
                total_processed = 0

                while True:
                    processed = parse_basic2_page(
                        page,
                        season=year,
                        league=league_name,
                        pitchers=pitchers,
                        sort_key=display_name,
                        max_players=limit,
                    )
                    total_processed += processed

                    if not go_to_next_page(page, page_number):
                        break
                    page_number += 1

                print(f"   ✅ Basic2 {display_name} 정렬 처리: {total_processed}행")

        browser.close()

    stats_list = list(pitchers.values())
    if limit:
        stats_list = stats_list[:limit]

    print(f"✅ {series_info['name']} 크롤링 완료: {len(stats_list)}명")

    # 투수 전용 테이블에 저장
    if save_to_db and stats_list:
        print(f"\n💾 투수 데이터 저장 시작 (player_season_pitching 테이블)...")
        try:
            payloads = [stat.to_repository_payload() for stat in stats_list]
            saved_count = save_pitching_stats_to_db(payloads)
            print(f"✅ 투수 데이터 저장 완료: {saved_count}명")
            print(f"📌 다음 단계: ./venv/bin/python3 src/sync/supabase_sync.py 실행하여 Supabase 동기화")
        except Exception as e:
            print(f"❌ 투수 데이터 저장 실패: {e}")

    return stats_list


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="KBO 투수 기록 크롤러 (Basic1/Basic2)")
    parser.add_argument("--year", type=int, default=2025, help="시즌 연도 (기본: 2025)")
    parser.add_argument(
        "--series",
        type=str,
        choices=list(SERIES_MAPPING.keys()),
        help="특정 시리즈만 수집 (기본값: 전체 시리즈)",
    )
    parser.add_argument("--limit", type=int, help="수집할 선수 수 제한 (디버깅용)")
    parser.add_argument("--headless", action="store_true", help="헤드리스 모드 사용")
    parser.add_argument(
        "--save",
        action="store_true",
        help="DB에 저장",
    )
    return parser.parse_args()


def main():
    args = parse_arguments()

    if args.series:
        # 특정 시리즈만 크롤링
        crawl_pitcher_series(
            year=args.year,
            series_key=args.series,
            limit=args.limit,
            headless=args.headless,
            save_to_db=args.save,
        )
    else:
        # 모든 시리즈 크롤링 (타자 크롤러와 동일한 패턴)
        all_data = {}
        for series_key in SERIES_MAPPING.keys():
            series_info = SERIES_MAPPING[series_key]
            print(f"\n🚀 {series_info['name']} 시작...")
            series_data = crawl_pitcher_series(
                year=args.year,
                series_key=series_key,
                limit=args.limit,
                headless=args.headless,
                save_to_db=args.save,  # 각 시리즈별로 저장
            )
            all_data[series_key] = series_data
            time.sleep(3)

        # 전체 요약
        print(f"\n" + "=" * 60)
        print(f"📈 전체 수집 요약 ({args.year}년)")
        print("=" * 60)
        total_players = 0
        for series_key, data in all_data.items():
            series_name = SERIES_MAPPING[series_key]["name"]
            print(f"  {series_name}: {len(data)}명")
            total_players += len(data)

        print(f"\n총 수집 선수: {total_players}명")


if __name__ == "__main__":
    main()
