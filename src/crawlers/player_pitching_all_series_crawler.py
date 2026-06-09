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
4. 필요 시 OCI(PostgreSQL)에 UPSERT 동기화 (season_id + player_id 기준)

Usage:
    python -m src.crawlers.player_pitching_all_series_crawler --year 2025 --series regular --save
    python -m src.cli.sync_oci --season-stats --year 2025
"""

from __future__ import annotations

import argparse
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime

from playwright.sync_api import Page, sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeout

from src.aggregators.season_stat_aggregator import SeasonStatAggregator
from src.db.engine import SessionLocal
from src.models.game import Game, GamePitchingStat
from src.models.player import PlayerBasic
from src.models.season import KboSeason
from src.repositories.player_season_pitching_repository import save_pitching_stats_to_db
from src.utils.fallback_monitor import FallbackMonitor
from src.utils.player_season_stat_validation import filter_valid_season_stat_payloads
from src.utils.player_stats_helpers import extract_rows_fast
from src.utils.playwright_retry import LONG_TIMEOUT, NAV_TIMEOUT, SEL_TIMEOUT, retry_wait_for_selector
from src.utils.request_policy import RequestPolicy
from src.utils.team_codes import resolve_team_code
from src.utils.team_mapping import get_team_mapping_for_year

logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------

BASIC1_URL = "https://www.koreabaseball.com/Record/Player/PitcherBasic/Basic1.aspx"
BASIC2_URL = "https://www.koreabaseball.com/Record/Player/PitcherBasic/Basic2.aspx"

BASIC1_SORT_CODE = "G_CN"  # 'G' (경기) 헤더

# 정규시즌 Basic2에서는 NP(투구수)만 수집
BASIC2_SORT_SEQUENCE = [
    ("NP", "PIT_CN"),  # 투구수
]

SERIES_MAPPING: dict[str, dict[str, str]] = {
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
    "regular": {"label": "G", "sort_code": "G_CN"},
    "default": {"label": "G", "sort_code": "G_CN"},
}

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def normalize_header(text: str) -> str:
    if text is None:
        return ""
    cleaned = text.replace("\xa0", " ").strip()
    if "\n" in cleaned:
        cleaned = cleaned.split("\n")[0].strip()
    parts = cleaned.split()
    if len(parts) > 1:
        cleaned = parts[0]
    return cleaned


def safe_int(value: str | None) -> int | None:
    if value is None:
        return None
    cleaned = value.replace(",", "").strip()
    if cleaned in {"", "-", "–"}:
        return None
    try:
        return int(float(cleaned))
    except ValueError:
        return None


def safe_float(value: str | None) -> float | None:
    if value is None:
        return None
    cleaned = value.replace(",", "").strip()
    if cleaned in {"", "-", "–"}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_innings(value: str | None) -> tuple[float | None, int | None]:
    """
    Convert inning string (e.g. '180 2/3') into (innings_float, outs_int).
    """
    if value is None:
        return None, None
    cleaned = value.replace(",", "").strip()
    if cleaned in {"", "-", "–"}:
        return None, None

    innings_float: float | None = None
    outs: int | None = None

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


def extract_player_id(href: str | None) -> int | None:
    if not href:
        return None
    match = re.search(r"playerId=(\d+)", href)
    return int(match.group(1)) if match else None


def wait_for_table(page: Page, timeout: int = 30000) -> None:
    try:
        page.wait_for_selector(
            "table.tData01 tbody tr",
            timeout=timeout,
            state="attached",
        )
    except PlaywrightTimeout:
        logger.exception("   ⚠️  테이블 행이 표시되지 않았습니다. (데이터 없음 가능성)")
    finally:
        page.wait_for_timeout(500)


def go_to_next_page(page: Page, current_page: int, policy: RequestPolicy | None = None) -> bool:
    """
    다음 페이지로 이동 (1→2,3,4,5→다음→6,7,8,9,10→다음 반복)
    """
    try:
        # 1→2,3,4,5→다음→6,7,8,9,10→다음 패턴
        if current_page % 5 == 0:  # 5페이지마다 "다음" 버튼 클릭
            selector = 'a[href*="btnNext"]'
            desc = f"다음 버튼 클릭 ({current_page}페이지 후)"
        else:
            # 5페이지 내에서 번호 버튼 클릭
            next_page = current_page + 1
            relative = ((next_page - 1) % 5) + 1
            selector = f'a[href*="btnNo{relative}"]'
            desc = f"{next_page}페이지로 이동 (btnNo{relative})"

        # 버튼 존재 여부 및 상태 확인 (reload+retry 포함)
        if not retry_wait_for_selector(page, selector, timeout=SEL_TIMEOUT, state="visible"):
            return False
        btn = page.query_selector(selector)
        if not btn or btn.get_attribute("disabled") or "disabled" in (btn.get_attribute("class") or ""):
            return False

        if policy:
            policy.delay()

        # 직접 클릭 시도
        page.click(selector, timeout=SEL_TIMEOUT)
        page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)
        logger.info("➡️ %s", desc)

        # 페이지 이동 후 테이블 대기
        wait_for_table(page)
        return True

    except Exception:
        logger.exception("❌ 페이지 이동 실패 (%sp -> next)", current_page)
        return False


def apply_sort(
    page: Page,
    header_label: str,
    sort_code: str | None = None,
    policy: RequestPolicy | None = None,
) -> bool:
    try:
        if sort_code:
            # Prioritize actual DOM click (safer postback triggers)
            selector = f"a[href=\"javascript:sort('{sort_code}');\"]"
            try:
                page.wait_for_selector(selector, timeout=SEL_TIMEOUT)
                anchor = page.query_selector(selector)
                if anchor:
                    if policy:
                        policy.delay()
                    anchor.click()
                    page.wait_for_load_state("networkidle", timeout=LONG_TIMEOUT)
                    if policy:
                        policy.delay()
                    return True
            except Exception:
                logger.warning("Sort toggle click failed, falling back to JS execution")

            # Fallback to direct JS execution if DOM is un-clickable
            has_sort_fn = page.evaluate("typeof sort === 'function'")
            if has_sort_fn:
                if policy:
                    policy.delay()
                page.evaluate(f"sort('{sort_code}')")
                page.wait_for_load_state("networkidle", timeout=LONG_TIMEOUT)
                if policy:
                    policy.delay()
                return True

        anchors = page.query_selector_all("table.tData01 thead a")
        for anchor in anchors:
            # Re-check attachment
            if not anchor.is_visible():
                continue

            label = normalize_header(anchor.text_content())
            if label == header_label:
                anchor.click()
                page.wait_for_load_state("networkidle", timeout=LONG_TIMEOUT)
                page.wait_for_timeout(800)
                return True

        logger.warning(f"⚠️  '{header_label}' 정렬 링크를 찾지 못했습니다.")  # noqa: G004
        return False
    except Exception:
        logger.exception("⚠️ 정렬 적용 실패")
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
    player_name: str | None = None
    team_name: str | None = None
    team_code: str | None = None
    games: int | None = None
    games_started: int | None = None
    wins: int | None = None
    losses: int | None = None
    saves: int | None = None
    holds: int | None = None
    innings_pitched: float | None = None
    innings_outs: int | None = None
    hits_allowed: int | None = None
    runs_allowed: int | None = None
    earned_runs: int | None = None
    home_runs_allowed: int | None = None
    walks_allowed: int | None = None
    intentional_walks: int | None = None
    hit_batters: int | None = None
    strikeouts: int | None = None
    wild_pitches: int | None = None
    balks: int | None = None
    era: float | None = None
    whip: float | None = None
    fip: float | None = None
    k_per_nine: float | None = None
    bb_per_nine: float | None = None
    kbb: float | None = None
    extra_stats: dict[str, object] = field(default_factory=lambda: {"rankings": {}})

    def to_repository_payload(self) -> dict[str, object | None]:
        """타자 크롤러 방식의 단순 데이터 구조"""
        data = {
            "player_id": self.player_id,
            "player_name": self.player_name,
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
            "innings_outs": self.innings_outs,
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
            "fip": self.fip,
            "k_per_nine": self.k_per_nine,
            "bb_per_nine": self.bb_per_nine,
            "kbb": self.kbb,
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
    pitchers: dict[int, PitcherStats],
    max_players: int | None = None,
) -> int:
    # Wait for the table to be visible (more resilient than specific header th)
    try:
        page.wait_for_selector("table.tData01", timeout=SEL_TIMEOUT)
    except Exception:
        logger.warning("기록 테이블을 찾을 수 없습니다. (타임아웃)")
        content = page.content()
        logger.debug("Page content length: %d | tData01 found: %s", len(content), "tData01" in content)
        return 0

    # Small stability delay to ensure AJAX completion if any
    page.wait_for_timeout(2000)

    # Use evaluate for atomic header extraction
    headers = page.evaluate("""
        () => {
            const table = document.querySelector('table.tData01');
            if (!table) return [];
            const ths = table.querySelectorAll('thead th');
            return Array.from(ths).map(th => th.textContent.trim());
        }
    """)
    headers = [normalize_header(h) for h in headers]
    header_index = {name: idx for idx, name in enumerate(headers)}

    core_headers = ["선수명", "팀명", "IP", "G", "ERA"]
    missing_core = [h for h in core_headers if h not in header_index]
    if missing_core:
        logger.warning(f"   ⚠️  Basic1 테이블 헤더에 필수 컬럼이 없습니다: {', '.join(missing_core)}")  # noqa: G004
        logger.info("   현재 헤더: %s", headers)
        # If headers are still empty, try a more lenient selector
        if not headers:
            logger.info("   🔍 Lenient header check (tData01)...")
            headers = page.evaluate("""
                () => Array.from(document.querySelectorAll('table.tData01 thead th')).map(th => th.textContent.trim())
            """)
            logger.info("   Lenient headers: %s", headers)
            headers = [normalize_header(h) for h in headers]
            header_index = {name: idx for idx, name in enumerate(headers)}
            missing_core = [h for h in core_headers if h not in header_index]
            if missing_core:
                return 0
        else:
            return 0

    # JavaScript Payload Extraction (Unified and robust)

    # JavaScript Payload Extraction
    extraction_script = """
    () => {
        const rows = document.querySelectorAll('table.tData01 tbody tr');
        if (rows.length === 0) return [];

        const headers = Array.from(document.querySelectorAll('table.tData01 thead th')).map(th => th.textContent.trim());
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
            const player_name = link.textContent.trim();
            const team_name = cells[headerIndex["팀명"]].textContent.trim();

            // Extract raw text for mapping in Python
            const raw = {};
            for (const [key, idx] of Object.entries(headerIndex)) {
                raw[key] = cells[idx].textContent.trim();
            }

            results.push({ player_id, player_name, team_name, raw });
        });
        return results;
    }
    """

    try:
        extracted_rows = page.evaluate(extraction_script)
        get_team_mapping_for_year(season)
        processed = 0

        for row in extracted_rows:
            player_id = row["player_id"]

            if max_players and player_id not in pitchers and len(pitchers) >= max_players:
                continue

            player_name = row["player_name"]
            team_name = row["team_name"]
            raw = row["raw"]

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
            def get_val(key: str) -> str | None:
                return raw.get(key)

            stats.games = safe_int(get_val("G")) if "G" in raw else stats.games
            if "GS" in raw:
                stats.games_started = safe_int(get_val("GS"))
            elif "선발" in raw:
                stats.games_started = safe_int(get_val("선발"))
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
                ("CG", "complete_games"),
                ("SHO", "shutouts"),
                ("TBF", "tbf"),
            ]:
                if header in raw:
                    val = safe_int(get_val(header))
                    if val is not None:
                        metrics[key] = val

            rank_value = safe_int(get_val("순위")) if "순위" in raw else None
            win_pct = safe_float(get_val("WPCT")) if "WPCT" in raw else None

            rankings = stats.extra_stats.setdefault("rankings", {})
            rankings["basic1"] = rank_value
            if stats.era is not None:
                metrics["era"] = stats.era
            if win_pct is not None:
                metrics["win_pct"] = win_pct

            processed += 1

        return processed

    except Exception:
        logger.exception("❌ Basic1 파싱 오류 (JS)")
        return 0


def parse_basic2_page(
    page: Page,
    season: int,
    league: str,
    pitchers: dict[int, PitcherStats],
    sort_key: str,
    max_players: int | None = None,
) -> int:
    if not retry_wait_for_selector(page, "table.tData01 thead th", timeout=NAV_TIMEOUT):
        logger.warning("⚠️  Basic2 테이블 헤더 파싱 실패 (타임아웃)")
        return 0

    headers = [normalize_header(th.text_content()) for th in page.query_selector_all("table.tData01 thead th")]
    header_index = {name: idx for idx, name in enumerate(headers)}
    get_team_mapping_for_year(season)
    use_fast = os.getenv("KBO_FAST_PARSE", "1") != "0"

    # Basic2 헤더는 정규시즌과 포스트시즌에서 다를 수 있음
    if "선수명" not in header_index or "팀명" not in header_index:
        logger.warning("⚠️  Basic2 테이블 헤더 파싱 실패")
        return 0

    rows_data = extract_rows_fast(page, selector="table.tData01", link_query="a") if use_fast else None
    rows = rows_data if rows_data is not None else page.query_selector_all("table.tData01 tbody tr")
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

            def cell_text(idx: int) -> str | None:
                return cells[idx] if len(cells) > idx else None

            (row.get("linkText") or cell_text(header_index["선수명"]) or "").strip()
            (cell_text(header_index["팀명"]) or "").strip()
        else:
            cells = row.query_selector_all("td")
            if len(cells) < len(headers):
                continue

            def cell_text(idx: int) -> str | None:
                return cells[idx].text_content() if len(cells) > idx else None

            link = cells[header_index["선수명"]].query_selector("a")
            player_id = extract_player_id(link.get_attribute("href") if link else None)
            if not player_id:
                continue
            link.text_content().strip() if link else cells[header_index["선수명"]].text_content().strip()
            cells[header_index["팀명"]].text_content().strip()

        if max_players and player_id not in pitchers and len(pitchers) >= max_players:
            continue

        stats = pitchers.get(player_id)
        if not stats:
            continue

        metrics = stats.extra_stats.setdefault("metrics", {})

        def set_metric(header_name: str, key: str, caster) -> None:
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


def setup_pitcher_page(page: Page, url: str, year: int, series_value: str, policy: RequestPolicy | None = None) -> bool:
    if policy:
        policy.delay(host="www.koreabaseball.com")

    logger.info("   🌐 Navigating to %s...", url)
    try:
        page.goto(url, wait_until="networkidle", timeout=LONG_TIMEOUT)
    except Exception:
        logger.exception("   ❌ %s 페이지 로딩 실패", url)
        return False

    if policy:
        policy.delay()

    try:
        season_selector = 'select[name*="ddlSeason"]'
        series_selector = 'select[name*="ddlSeries"]'

        logger.info("   ⚙️  Selecting Season %s...", year)
        page.select_option(season_selector, str(year))
        page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)
        if policy:
            policy.delay()

        logger.info("   ⚙️  Selecting Series %s...", series_value)
        page.select_option(series_selector, value=series_value)
        page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT)
        if policy:
            policy.delay()

        # KBO 페이지 에러 감지 (500 에러 등)
        title = page.title()
        if "에러" in title or "Error" in title:
            logger.error("   ❌ KBO 페이지 에러 감지: %s", title)
            return False

        return True
    except Exception:
        logger.exception("   ⚠️ 페이지 설정 중 오류")
        return False


def build_pitching_crawl_summary(stats_list: list[PitcherStats]) -> tuple[dict[str, object], list[PitcherStats]]:
    payloads = [stat.to_repository_payload() for stat in stats_list]
    valid_payloads, failure_counts = filter_valid_season_stat_payloads(payloads, stat_type="pitching")
    valid_ids = {payload["player_id"] for payload in valid_payloads}
    valid_stats = [stat for stat in stats_list if stat.player_id in valid_ids]
    summary = {
        "processed_rows": len(stats_list),
        "valid_rows": len(valid_stats),
        "filtered_rows": len(stats_list) - len(valid_stats),
        "failure_counts": dict(failure_counts),
    }
    return summary, valid_stats


# ---------------------------------------------------------------------------
# Fallback logic
# ---------------------------------------------------------------------------


def fallback_pitching_from_db(year: int, series_key: str, reason: str = "Manual Trigger") -> list[PitcherStats]:
    """
    KBO 페이지 장애 시 로컬 DB의 상세 기록을 합산하여 투수 시즌 기록을 생성합니다.
    """
    FallbackMonitor.log_fallback(year, series_key, "PITCHING", reason)
    logger.info("🔄 로컬 DB 기반 투수 기록 집계 시작 (연도: %s, 시리즈: %s)...", year, series_key)
    pitchers: dict[int, PitcherStats] = {}

    with SessionLocal() as session:
        # 1. 해당 시즌/시리즈에 경기를 뛴 모든 투수 ID 조회
        pattern = SeasonStatAggregator._get_league_name_pattern(series_key)

        player_ids_query = (
            session.query(GamePitchingStat.player_id)
            .join(Game, GamePitchingStat.game_id == Game.game_id)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(KboSeason.season_year == year)
            .filter(KboSeason.league_type_name.like(f"%{pattern}%"))
            .distinct()
        )

        player_ids = [p[0] for p in player_ids_query.all() if p[0]]
        logger.info("🔍 DB에서 %s명의 투수를 발견했습니다.", len(player_ids))

        series_info = SERIES_MAPPING.get(series_key, {})
        league_name = series_info.get("league", "REGULAR")

        for pid in player_ids:
            # 2. 개별 투수별 집계
            agg_data = SeasonStatAggregator.aggregate_pitching_season(session, pid, year, series_key)
            if not agg_data:
                continue

            # 3. 선수 기본 정보 조회
            player_basic = session.query(PlayerBasic).filter_by(player_id=pid).first()

            # 4. PitcherStats 객체 생성
            stats = PitcherStats(
                player_id=pid,
                season=year,
                league=league_name,
                source="FALLBACK",
                player_name=player_basic.name if player_basic else f"Player_{pid}",
            )

            # 데이터 매핑
            for key, value in agg_data.items():
                if hasattr(stats, key):
                    setattr(stats, key, value)

            # 팀 정보 보정 (집계에 참여한 가장 최근 경기 팀 코드 사용 시도)
            last_game_stat = (
                session.query(GamePitchingStat.team_code)
                .join(Game, GamePitchingStat.game_id == Game.game_id)
                .join(KboSeason, Game.season_id == KboSeason.season_id)
                .filter(GamePitchingStat.player_id == pid)
                .filter(KboSeason.season_year == year)
                .order_by(Game.game_date.desc())
                .first()
            )
            if last_game_stat:
                stats.team_code = last_game_stat[0]

            pitchers[pid] = stats

    logger.info("✅ DB 집계 완료: 총 %s명", len(pitchers))
    return list(pitchers.values())


def crawl_pitcher_series(
    year: int,
    series_key: str,
    limit: int | None = None,
    headless: bool = True,
    save_to_db: bool = False,
    by_team: bool = False,
) -> list[PitcherStats]:
    if series_key not in SERIES_MAPPING:
        raise ValueError(f"지원하지 않는 시리즈 키: {series_key}")

    series_info = SERIES_MAPPING[series_key]
    league_name = series_info.get("league", "REGULAR")
    logger.info(f"\n📊 {year}년 {series_info['name']} 수집 시작 (by_team={by_team})")  # noqa: G004

    pitchers: dict[int, PitcherStats] = {}
    policy = RequestPolicy()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        # Apply UA rotation via context
        context = browser.new_context(**policy.build_context_kwargs(locale="ko-KR"))
        page = context.new_page()
        page.set_default_timeout(60000)
        # install_sync_resource_blocking(page)

        # Step 1: Basic1 - 시리즈별 정렬 후 전체 페이지 수집
        if not setup_pitcher_page(page, BASIC1_URL, year, series_info["value"], policy=policy):
            reason = "Basic1 page setup failed (possible KBO site error)"
            logger.error("❌ Basic1 페이지 설정 실패. %s. DB에서 직접 집계하여 폴백(Fallback)을 시도합니다.", reason)
            browser.close()
            stats_list = fallback_pitching_from_db(year, series_key, reason=reason)
            # Use FALLBACK_AUTO if triggered during crawl
            for s in stats_list:
                s.source = "FALLBACK_AUTO"

            FallbackMonitor.log_fallback(
                year,
                series_key,
                "PITCHING",
                f"Fallback completed via {reason}",
                player_count=len(stats_list),
            )
            if save_to_db and stats_list:
                payloads = [stat.to_repository_payload() for stat in stats_list]
                save_pitching_stats_to_db(payloads)
            return stats_list

        # 팀별 순회 로직
        team_options = []
        if by_team:
            try:
                team_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlTeam$ddlTeam"]'
                # Check if selector exists (it should)
                if page.query_selector(team_selector):
                    options = page.eval_on_selector_all(
                        f"{team_selector} option",
                        "options => options.map(o => ({text: o.textContent, value: o.value}))",
                    )
                    team_options = [opt for opt in options if opt["value"]]  # Empty value is "Team Selection"
                    logger.info("ℹ️ 팀별 순회 모드: %s개 팀 발견", len(team_options))
                else:
                    logger.warning("⚠️ 팀 선택 드롭다운을 찾을 수 없습니다. 전체 모드로 진행.")
            except Exception:
                logger.exception("⚠️ 팀 목록 추출 실패, 전체 모드로 진행")
                team_options = []

        iteration_targets = team_options if team_options else [{"value": "", "text": "전체"}]

        for tm in iteration_targets:
            policy.delay()
            if team_options:
                logger.info(f"🔍 팀 선택: {tm['text']} ({tm['value']})")  # noqa: G004
                try:
                    page.select_option(
                        'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlTeam$ddlTeam"]',
                        tm["value"],
                    )
                    page.wait_for_load_state("networkidle", timeout=LONG_TIMEOUT)
                    policy.delay()
                except Exception:
                    logger.exception(f"⚠️ 팀 선택 실패 ({tm['text']})")  # noqa: G004
                    continue

            # 정렬 적용 (팀 선택 후 리셋될 수 있으므로 다시 적용)
            # 단, 중복 포스트백으로 인한 KBO 에러 발생을 방지하기 위해 apply_sort는 생략합니다.
            if False:
                primary_sort = PRIMARY_SORT_CONFIG.get(series_key, PRIMARY_SORT_CONFIG["default"])
                apply_sort(
                    page,
                    header_label=primary_sort["label"],
                    sort_code=primary_sort["sort_code"],
                    policy=policy,
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
                logger.info("   ▶ Basic1 %s페이지: %s명 처리 (누적 %s명)", page_number, parsed, len(pitchers))

                if limit and len(pitchers) >= limit:
                    logger.info("   🎯 수집 제한에 도달했습니다.")
                    break

                if not go_to_next_page(page, page_number, policy=policy):
                    break
                page_number += 1

            if limit and len(pitchers) >= limit:
                break

        logger.info("✅ Basic1 수집 완료: 총 %s명", len(pitchers))

        if series_key == "regular" and not by_team:
            if not setup_pitcher_page(page, BASIC2_URL, year, series_info["value"], policy=policy):
                logger.warning("⚠️  Basic2 페이지 설정 실패. 추가 지표 없이 종료합니다.")
                browser.close()
                return list(pitchers.values()) if not limit else list(pitchers.values())[:limit]

            for display_name, sort_code in BASIC2_SORT_SEQUENCE:
                if not apply_sort(page, display_name, sort_code, policy=policy):
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

                    if not go_to_next_page(page, page_number, policy=policy):
                        break
                    page_number += 1

                logger.info("   ✅ Basic2 %s 정렬 처리: %s행", display_name, total_processed)
        elif by_team:
            logger.info("ℹ️ 팀별 순회 모드에서는 Basic2(상세 지표) 수집을 건너뜁니다.")

        browser.close()

    stats_list = list(pitchers.values())
    if limit:
        stats_list = stats_list[:limit]

    logger.info(f"✅ {series_info['name']} 크롤링 완료: {len(stats_list)}명")  # noqa: G004
    summary, valid_stats_list = build_pitching_crawl_summary(stats_list)
    if summary["filtered_rows"]:
        logger.warning(f"⚠️ 투수 시즌 row 필터링: {summary['filtered_rows']}건 ({summary['failure_counts']})")  # noqa: G004
    stats_list = valid_stats_list

    # 투수 전용 테이블에 저장
    if save_to_db and stats_list:
        logger.info("\n💾 투수 데이터 저장 시작 (player_season_pitching 테이블)...")
        try:
            payloads = [stat.to_repository_payload() for stat in stats_list]
            saved_count = save_pitching_stats_to_db(payloads)
            logger.info("✅ 투수 데이터 저장 완료: %s명", saved_count)
            logger.info(
                "📌 다음 단계: ./venv/bin/python3 -m src.cli.sync_oci --season-stats --year %s 실행하여 OCI 동기화",
                year,
            )
        except Exception:
            logger.exception("❌ 투수 데이터 저장 실패")

    return stats_list


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="KBO 투수 기록 크롤러 (Basic1/Basic2)")
    parser.add_argument("--year", type=int, default=datetime.now().year, help="시즌 연도 (기본: 당해 연도)")
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
    parser.add_argument("--by-team", action="store_true", help="팀별로 순회하여 모든 선수(비규정타석 포함) 수집")
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    policy = RequestPolicy()

    if args.series:
        # 특정 시리즈만 크롤링
        crawl_pitcher_series(
            year=args.year,
            series_key=args.series,
            limit=args.limit,
            headless=args.headless,
            save_to_db=args.save,
            by_team=args.by_team,
        )
    else:
        # 모든 시리즈 크롤링 (타자 크롤러와 동일한 패턴)
        all_data = {}
        for series_key in SERIES_MAPPING:
            series_info = SERIES_MAPPING[series_key]
            logger.info(f"\n🚀 {series_info['name']} 시작...")  # noqa: G004
            series_data = crawl_pitcher_series(
                year=args.year,
                series_key=series_key,
                limit=args.limit,
                headless=args.headless,
                save_to_db=args.save,  # 각 시리즈별로 저장
                by_team=args.by_team,
            )
            all_data[series_key] = series_data
            policy.delay()

        # 전체 요약
        logger.info("\n" + "=" * 60)  # noqa: G003
        logger.info("📈 전체 수집 요약 (%s년)", args.year)
        logger.info("=" * 60)
        total_players = 0
        for series_key, data in all_data.items():
            series_name = SERIES_MAPPING[series_key]["name"]
            logger.info("  %s: %s명", series_name, len(data))
            total_players += len(data)

        logger.info("\n총 수집 선수: %s명", total_players)


if __name__ == "__main__":
    main()
