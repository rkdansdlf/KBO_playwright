"""
KBO 전체 시리즈 투수 기록 크롤러.

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

from playwright.sync_api import ElementHandle, Page, sync_playwright
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeout
from sqlalchemy.exc import SQLAlchemyError

from src.aggregators.season_stat_aggregator import SeasonStatAggregator
from src.constants import KST
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
from src.utils.type_helpers import (
    parse_innings,
    parse_innings_to_outs,
    safe_float_or_none,
    safe_int_or_none,
)

logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------

from typing import TYPE_CHECKING

from src.urls import PITCHER_BASIC1, PITCHER_BASIC2

if TYPE_CHECKING:
    from collections.abc import Callable

BASIC1_URL = PITCHER_BASIC1
BASIC2_URL = PITCHER_BASIC2

BASIC1_SORT_CODE = "G_CN"  # 'G' (경기) 헤더

# 정규시즌 Basic2에서는 NP(투구수)만 수집
BASIC2_SORT_SEQUENCE = [
    ("NP", "PIT_CN"),  # 투구수
]
CRAWLER_EXCEPTIONS = (
    PlaywrightError,
    PlaywrightTimeout,
    RuntimeError,
    ValueError,
    TypeError,
    AttributeError,
    OSError,
)
DB_SAVE_EXCEPTIONS = (*CRAWLER_EXCEPTIONS, SQLAlchemyError)


@dataclass
class Basic2PageContext:
    """Basic2PageContext class."""

    page: Page
    season: int
    league: str
    pitchers: dict[int, PitcherStats]
    sort_key: str
    max_players: int | None = None


@dataclass
class PitcherBasic1Context:
    """PitcherBasic1Context class."""

    page: Page
    year: int
    league_name: str
    iteration_targets: list[dict]
    by_team: bool
    limit: int | None
    policy: RequestPolicy
    pitchers: dict[int, PitcherStats]


@dataclass
class Basic2AdditionalContext:
    """Basic2AdditionalContext class."""

    page: Page
    year: int
    league_name: str
    series_info: dict
    limit: int | None
    policy: RequestPolicy
    pitchers: dict[int, PitcherStats]


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
    """
    Normalizes header.

    Args:
        text: Text.

    Returns:
        String result.

    """
    if text is None:
        return ""
    cleaned = text.replace("\xa0", " ").strip()
    if "\n" in cleaned:
        cleaned = cleaned.split("\n")[0].strip()
    parts = cleaned.split()
    if len(parts) > 1:
        cleaned = parts[0]
    return cleaned


def extract_player_id(href: str | None) -> int | None:
    """
    Extracts player id.

    Args:
        href: Href.

    Returns:
        The result of the operation.

    """
    if not href:
        return None
    match = re.search(r"playerId=(\d+)", href)
    return int(match.group(1)) if match else None


def wait_for_table(page: Page, timeout: int = 30000) -> None:
    """
    Handles the wait for table operation.

    Args:
        page: Page.
        timeout: Timeout in seconds.

    """
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
    """다음 페이지로 이동 (1→2,3,4,5→다음→6,7,8,9,10→다음 반복)."""
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

        # 페이지 이동 후 테이블 대기
        wait_for_table(page)
    except CRAWLER_EXCEPTIONS:
        logger.exception("❌ 페이지 이동 실패 (%sp -> next)", current_page)
        return False
    else:
        logger.info("➡️ %s", desc)
        return True


def _apply_sort_by_code(
    page: Page,
    sort_code: str,
    policy: RequestPolicy | None = None,
) -> bool:
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
    except (PlaywrightError, PlaywrightTimeout):
        logger.warning("Sort toggle click failed, falling back to JS execution")

    has_sort_fn = page.evaluate("typeof sort === 'function'")
    if has_sort_fn:
        if policy:
            policy.delay()
        page.evaluate(f"sort('{sort_code}')")
        page.wait_for_load_state("networkidle", timeout=LONG_TIMEOUT)
        if policy:
            policy.delay()
        return True
    return False


def _apply_sort_by_label(page: Page, header_label: str) -> bool:
    anchors = page.query_selector_all("table.tData01 thead a")
    for anchor in anchors:
        if not anchor.is_visible():
            continue

        label = normalize_header(anchor.text_content())
        if label == header_label:
            anchor.click()
            page.wait_for_load_state("networkidle", timeout=LONG_TIMEOUT)
            page.wait_for_timeout(800)
            return True
    return False


def apply_sort(
    page: Page,
    header_label: str,
    sort_code: str | None = None,
    policy: RequestPolicy | None = None,
) -> bool:
    """
    Handles the apply sort operation.

    Args:
        page: Page.
        header_label: Header Label.
        sort_code: Sort Code.
        policy: Policy.

    Returns:
        True if successful, False otherwise.

    """
    try:
        if sort_code and _apply_sort_by_code(page, sort_code, policy):
            return True
        return _apply_sort_by_label(page, header_label)
    except CRAWLER_EXCEPTIONS:
        logger.exception("⚠️ 정렬 적용 실패")
        return False


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class PitcherStats:
    """PitcherStats class."""

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
        """타자 크롤러 방식의 단순 데이터 구조."""
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


def _get_and_validate_basic1_headers(page: Page) -> dict[str, int] | None:
    try:
        page.wait_for_selector("table.tData01", timeout=SEL_TIMEOUT)
    except (PlaywrightError, PlaywrightTimeout):
        logger.warning("기록 테이블을 찾을 수 없습니다. (타임아웃)")
        return None

    page.wait_for_timeout(2000)

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
        logger.warning("   ⚠️  Basic1 테이블 헤더에 필수 컬럼이 없습니다: %s", ", ".join(missing_core))
        headers = page.evaluate("""
            () => Array.from(document.querySelectorAll('table.tData01 thead th')).map(th => th.textContent.trim())
        """)
        headers = [normalize_header(h) for h in headers]
        header_index = {name: idx for idx, name in enumerate(headers)}
        if any(h not in header_index for h in core_headers):
            return None

    return header_index


def _map_pitcher_basic1_stats(
    row: dict,
    season: int,
    league: str,
    pitchers: dict[int, PitcherStats],
    max_players: int | None,
) -> bool:
    player_id = row["player_id"]
    if max_players and player_id not in pitchers and len(pitchers) >= max_players:
        return False

    player_name = row["player_name"]
    team_name = row["team_name"]
    raw = row["raw"]

    team_code = resolve_team_code(team_name, season) or team_name

    stats = pitchers.get(player_id)
    if not stats:
        stats = PitcherStats(player_id=player_id, season=season, league=league)
        pitchers[player_id] = stats

    stats.player_name = player_name
    stats.team_name = team_name
    stats.team_code = team_code

    def get_val(key: str) -> str | None:
        """
        Gets val.

        Args:
            key: Key.

        Returns:
            The result of the operation.

        """
        return raw.get(key)

    stats.games = safe_int_or_none(get_val("G")) if "G" in raw else stats.games
    stats.games_started = (
        safe_int_or_none(get_val("GS") or get_val("선발")) if "GS" in raw or "선발" in raw else stats.games_started
    )
    stats.wins = safe_int_or_none(get_val("W")) if "W" in raw else stats.wins
    stats.losses = safe_int_or_none(get_val("L")) if "L" in raw else stats.losses
    stats.saves = safe_int_or_none(get_val("SV")) if "SV" in raw else stats.saves
    stats.holds = safe_int_or_none(get_val("HLD")) if "HLD" in raw else stats.holds

    if "IP" in raw:
        stats.innings_pitched = parse_innings(get_val("IP"))
        stats.innings_outs = parse_innings_to_outs(get_val("IP"))

    stats.hits_allowed = safe_int_or_none(get_val("H")) if "H" in raw else stats.hits_allowed
    stats.home_runs_allowed = safe_int_or_none(get_val("HR")) if "HR" in raw else stats.home_runs_allowed
    stats.walks_allowed = safe_int_or_none(get_val("BB")) if "BB" in raw else stats.walks_allowed
    stats.hit_batters = safe_int_or_none(get_val("HBP")) if "HBP" in raw else stats.hit_batters
    stats.strikeouts = safe_int_or_none(get_val("SO")) if "SO" in raw else stats.strikeouts
    stats.runs_allowed = safe_int_or_none(get_val("R")) if "R" in raw else stats.runs_allowed
    stats.earned_runs = safe_int_or_none(get_val("ER")) if "ER" in raw else stats.earned_runs
    stats.era = safe_float_or_none(get_val("ERA")) if "ERA" in raw else stats.era
    stats.whip = safe_float_or_none(get_val("WHIP")) if "WHIP" in raw else stats.whip

    metrics = stats.extra_stats.setdefault("metrics", {})
    for header, key in [("CG", "complete_games"), ("SHO", "shutouts"), ("TBF", "tbf")]:
        if header in raw:
            val = safe_int_or_none(get_val(header))
            if val is not None:
                metrics[key] = val

    rank_value = safe_int_or_none(get_val("순위")) if "순위" in raw else None
    win_pct = safe_float_or_none(get_val("WPCT")) if "WPCT" in raw else None

    rankings = stats.extra_stats.setdefault("rankings", {})
    rankings["basic1"] = rank_value
    if stats.era is not None:
        metrics["era"] = stats.era
    if win_pct is not None:
        metrics["win_pct"] = win_pct

    return True


def parse_basic1_page(
    page: Page,
    season: int,
    league: str,
    pitchers: dict[int, PitcherStats],
    max_players: int | None = None,
) -> int:
    """
    Parses basic1 page.

    Args:
        page: Page.
        season: Season year.
        league: League.
        pitchers: Pitchers.
        max_players: Max Players.

    Returns:
        Integer result.

    """
    header_index = _get_and_validate_basic1_headers(page)
    if not header_index:
        return 0

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
            if _map_pitcher_basic1_stats(row, season, league, pitchers, max_players):
                processed += 1

    except CRAWLER_EXCEPTIONS:
        logger.exception("❌ Basic1 파싱 오류 (JS)")
        return 0
    else:
        return processed


def _extract_basic2_row_info(
    row: ElementHandle | dict,
    header_index: dict[str, int],
    *,
    use_fast: bool,
) -> tuple[int | None, Callable[[int], str | None]] | None:
    if use_fast:
        cells = row.get("cells") or []
        if len(cells) < len(header_index):
            return None
        link_href = row.get("linkHref")
        player_id = extract_player_id(link_href)
        if not player_id:
            return None

        def cell_text_fast(idx: int) -> str | None:
            """
            Handles the cell text fast operation.

            Args:
                idx: Idx.

            Returns:
                The result of the operation.

            """
            return cells[idx] if len(cells) > idx else None

        return player_id, cell_text_fast

    cells = row.query_selector_all("td")
    if len(cells) < len(header_index):
        return None
    link = cells[header_index["선수명"]].query_selector("a")
    player_id = extract_player_id(link.get_attribute("href") if link else None)
    if not player_id:
        return None

    def cell_text_slow(idx: int) -> str | None:
        """
        Handles the cell text slow operation.

        Args:
            idx: Idx.

        Returns:
            The result of the operation.

        """
        return cells[idx].text_content() if len(cells) > idx else None

    return player_id, cell_text_slow


def _update_pitcher_basic2_stats(
    stats: PitcherStats,
    header_index: dict[str, int],
    cell_text_fn: Callable[[int], str | None],
    sort_key: str,
) -> None:
    metrics = stats.extra_stats.setdefault("metrics", {})

    metric_mapping = {
        "CG": ("complete_games", safe_int_or_none),
        "SHO": ("shutouts", safe_int_or_none),
        "QS": ("quality_starts", safe_int_or_none),
        "BSV": ("blown_saves", safe_int_or_none),
        "TBF": ("tbf", safe_int_or_none),
        "NP": ("np", safe_int_or_none),
        "AVG": ("avg_against", safe_float_or_none),
        "2B": ("doubles_allowed", safe_int_or_none),
        "3B": ("triples_allowed", safe_int_or_none),
        "SAC": ("sacrifices_allowed", safe_int_or_none),
        "SF": ("sacrifice_flies_allowed", safe_int_or_none),
    }

    for header_name, (key, caster) in metric_mapping.items():
        if header_name in header_index:
            value = caster(cell_text_fn(header_index[header_name]))
            if value is not None:
                metrics[key] = value

    single_mapping = {
        "IBB": "intentional_walks",
        "WP": "wild_pitches",
        "BK": "balks",
    }

    for header_name, field_name in single_mapping.items():
        if header_name in header_index:
            val = safe_int_or_none(cell_text_fn(header_index[header_name]))
            if val is not None:
                setattr(stats, field_name, val)

    rank_val = safe_int_or_none(cell_text_fn(header_index.get("순위", 0))) if "순위" in header_index else None
    if rank_val is not None:
        rankings = stats.extra_stats.setdefault("rankings", {})
        rankings[sort_key] = rank_val


def parse_basic2_page(ctx: Basic2PageContext) -> int:
    """
    Parses basic2 page.

    Args:
        ctx: Ctx.

    Returns:
        Integer result.

    """
    if not retry_wait_for_selector(ctx.page, "table.tData01 thead th", timeout=NAV_TIMEOUT):
        logger.warning("⚠️  Basic2 테이블 헤더 파싱 실패 (타임아웃)")
        return 0

    headers = [normalize_header(th.text_content()) for th in ctx.page.query_selector_all("table.tData01 thead th")]
    header_index = {name: idx for idx, name in enumerate(headers)}
    get_team_mapping_for_year(ctx.season)
    use_fast = os.getenv("KBO_FAST_PARSE", "1") != "0"

    if "선수명" not in header_index or "팀명" not in header_index:
        logger.warning("⚠️  Basic2 테이블 헤더 파싱 실패")
        return 0

    rows_data = extract_rows_fast(ctx.page, selector="table.tData01", link_query="a") if use_fast else None
    rows = rows_data if rows_data is not None else ctx.page.query_selector_all("table.tData01 tbody tr")
    processed = 0

    for row in rows:
        res = _extract_basic2_row_info(row, header_index, use_fast=rows_data is not None)
        if not res:
            continue

        player_id, cell_text_fn = res
        if ctx.max_players and player_id not in ctx.pitchers and len(ctx.pitchers) >= ctx.max_players:
            continue

        stats = ctx.pitchers.get(player_id)
        if not stats:
            continue

        _update_pitcher_basic2_stats(stats, header_index, cell_text_fn, ctx.sort_key)
        processed += 1

    return processed


# ---------------------------------------------------------------------------
# Crawling logic
# ---------------------------------------------------------------------------


def setup_pitcher_page(page: Page, url: str, year: int, series_value: str, policy: RequestPolicy | None = None) -> bool:
    """
    Set up pitcher page.

    Args:
        page: Page.
        url: Url.
        year: Season year.
        series_value: Series Value.
        policy: Policy.

    Returns:
        True if successful, False otherwise.

    """
    if policy:
        policy.delay(host="www.koreabaseball.com")

    logger.info("   🌐 Navigating to %s...", url)
    try:
        page.goto(url, wait_until="networkidle", timeout=LONG_TIMEOUT)
    except CRAWLER_EXCEPTIONS:
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

    except CRAWLER_EXCEPTIONS:
        logger.exception("   ⚠️ 페이지 설정 중 오류")
        return False
    else:
        return True


def build_pitching_crawl_summary(stats_list: list[PitcherStats]) -> tuple[dict[str, object], list[PitcherStats]]:
    """
    Builds pitching summary.

    Args:
        stats_list: Stats List.

    Returns:
        Tuple result.

    """
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
    """KBO 페이지 장애 시 로컬 DB의 상세 기록을 합산하여 투수 시즌 기록을 생성합니다."""
    FallbackMonitor.log_fallback(year, series_key, "PITCHING", reason)
    logger.info("🔄 로컬 DB 기반 투수 기록 집계 시작 (연도: %s, 시리즈: %s)...", year, series_key)
    pitchers: dict[int, PitcherStats] = {}

    with SessionLocal() as session:
        # 1. 벌크 집계 데이터 가져오기
        bulk_stats = SeasonStatAggregator.aggregate_pitching_season_bulk(session, year, series_key)
        if not bulk_stats:
            logger.info("✅ DB 집계 완료: 총 0명")
            return []

        player_ids = [s["player_id"] for s in bulk_stats if s.get("player_id")]
        logger.info("🔍 DB에서 %s명의 투수를 발견했습니다.", len(player_ids))

        # 2. 선수 기본 정보 벌크 로드
        players = (
            session.query(PlayerBasic.player_id, PlayerBasic.name).filter(PlayerBasic.player_id.in_(player_ids)).all()
        )
        player_name_map = {p.player_id: p.name for p in players}

        # 3. 최근 소속팀 매핑 벌크 로드 (N+1 방지)
        recent_games = (
            session.query(GamePitchingStat.player_id, GamePitchingStat.team_code, Game.game_date)
            .join(Game, GamePitchingStat.game_id == Game.game_id)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(GamePitchingStat.player_id.in_(player_ids))
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

        series_info = SERIES_MAPPING.get(series_key, {})
        league_name = series_info.get("league", "REGULAR")

        # 4. PitcherStats 생성 및 데이터 매핑
        for agg_data in bulk_stats:
            pid = agg_data["player_id"]
            stats = PitcherStats(
                player_id=pid,
                season=year,
                league=league_name,
                source="FALLBACK",
                player_name=player_name_map.get(pid) or agg_data.get("player_name") or f"Player_{pid}",
            )

            # 데이터 매핑
            for key, value in agg_data.items():
                if hasattr(stats, key):
                    setattr(stats, key, value)

            # 최근 팀 보정
            if pid in player_team_map:
                stats.team_code = player_team_map[pid]

            pitchers[pid] = stats

    logger.info("✅ DB 집계 완료: 총 %s명", len(pitchers))
    return list(pitchers.values())


def _handle_pitching_fallback(
    year: int,
    series_key: str,
    reason: str,
    *,
    save_to_db: bool,
) -> list[PitcherStats]:
    stats_list = fallback_pitching_from_db(year, series_key, reason=reason)
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


def _get_pitcher_team_options(page: Page, *, by_team: bool) -> list[dict]:
    if not by_team:
        return [{"value": "", "text": "전체"}]
    try:
        team_selector = 'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlTeam$ddlTeam"]'
        if page.query_selector(team_selector):
            options = page.eval_on_selector_all(
                f"{team_selector} option",
                "options => options.map(o => ({text: o.textContent, value: o.value}))",
            )
            team_options = [opt for opt in options if opt["value"]]
            logger.info("ℹ️ 팀별 순회 모드: %s개 팀 발견", len(team_options))
        else:
            logger.warning("⚠️ 팀 선택 드롭다운을 찾을 수 없습니다. 전체 모드로 진행.")
            return [{"value": "", "text": "전체"}]
    except CRAWLER_EXCEPTIONS:
        logger.exception("⚠️ 팀 목록 추출 실패, 전체 모드로 진행")
        return [{"value": "", "text": "전체"}]
    else:
        return team_options


def _select_pitcher_team_if_needed(page: Page, tm: dict, *, by_team: bool, policy: RequestPolicy) -> bool:
    if by_team and tm["value"]:
        logger.info("🔍 팀 선택: %s (%s)", tm["text"], tm["value"])
        try:
            page.select_option(
                'select[name="ctl00$ctl00$ctl00$cphContents$cphContents$cphContents$ddlTeam$ddlTeam"]',
                tm["value"],
            )
            page.wait_for_load_state("networkidle", timeout=LONG_TIMEOUT)
            policy.delay()
        except CRAWLER_EXCEPTIONS:
            logger.exception("⚠️ 팀 선택 실패 (%s)", tm["text"])
            return False
        else:
            return True
    return True


def _collect_pitcher_basic1_loop(ctx: PitcherBasic1Context) -> None:
    for tm in ctx.iteration_targets:
        ctx.policy.delay()
        if not _select_pitcher_team_if_needed(ctx.page, tm, by_team=ctx.by_team, policy=ctx.policy):
            continue

        wait_for_table(ctx.page)

        page_number = 1
        while True:
            parsed = parse_basic1_page(
                ctx.page,
                season=ctx.year,
                league=ctx.league_name,
                pitchers=ctx.pitchers,
                max_players=ctx.limit,
            )
            logger.info("   ▶ Basic1 %s페이지: %s명 처리 (누적 %s명)", page_number, parsed, len(ctx.pitchers))

            if ctx.limit and len(ctx.pitchers) >= ctx.limit:
                logger.info("   🎯 수집 제한에 도달했습니다.")
                return

            if not go_to_next_page(ctx.page, page_number, policy=ctx.policy):
                break
            page_number += 1


def _collect_pitcher_basic2_additional(ctx: Basic2AdditionalContext) -> None:
    if not setup_pitcher_page(ctx.page, BASIC2_URL, ctx.year, ctx.series_info["value"], policy=ctx.policy):
        logger.warning("⚠️  Basic2 페이지 설정 실패. 추가 지표 없이 종료합니다.")
        return

    league = ctx.series_info.get("league", ctx.league_name)

    for display_name, sort_code in BASIC2_SORT_SEQUENCE:
        if not apply_sort(ctx.page, display_name, sort_code, policy=ctx.policy):
            continue
        wait_for_table(ctx.page)

        page_ctx = Basic2PageContext(
            page=ctx.page,
            season=ctx.year,
            league=league,
            pitchers=ctx.pitchers,
            sort_key=display_name,
            max_players=ctx.limit,
        )

        page_number = 1
        total_processed = 0

        while True:
            processed = parse_basic2_page(page_ctx)
            total_processed += processed

            if not go_to_next_page(ctx.page, page_number, policy=ctx.policy):
                break
            page_number += 1

        logger.info("   ✅ Basic2 %s 정렬 처리: %s행", display_name, total_processed)


def crawl_pitcher_series(
    year: int,
    series_key: str,
    limit: int | None = None,
    *,
    headless: bool = True,
    save_to_db: bool = False,
    by_team: bool = False,
) -> list[PitcherStats]:
    """
    Crawls pitcher series.

    Args:
        year: Season year.
        series_key: Series Key.
        limit: Limit.

    Returns:
        List of results.

    """
    if series_key not in SERIES_MAPPING:
        msg = f"지원하지 않는 시리즈 키: {series_key}"
        raise ValueError(msg)

    series_info = SERIES_MAPPING[series_key]
    league_name = series_info.get("league", "REGULAR")
    logger.info("\n📊 %s년 %s 수집 시작 (by_team=%s)", year, series_info["name"], by_team)

    pitchers: dict[int, PitcherStats] = {}
    policy = RequestPolicy()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        # Apply UA rotation via context
        context = browser.new_context(**policy.build_context_kwargs(locale="ko-KR"))
        page = context.new_page()
        page.set_default_timeout(60000)

        # Step 1: Basic1 - 시리즈별 정렬 후 전체 페이지 수집
        if not setup_pitcher_page(page, BASIC1_URL, year, series_info["value"], policy=policy):
            reason = "Basic1 page setup failed (possible KBO site error)"
            logger.error("❌ Basic1 페이지 설정 실패. %s. DB에서 직접 집계하여 폴백(Fallback)을 시도합니다.", reason)
            browser.close()
            return _handle_pitching_fallback(year, series_key, reason, save_to_db=save_to_db)

        # 순회 대상 설정 (팀 옵션이 있으면 팀별, 없으면 전체 1회)
        team_options = _get_pitcher_team_options(page, by_team=by_team)
        _collect_pitcher_basic1_loop(
            PitcherBasic1Context(
                page=page,
                year=year,
                league_name=league_name,
                iteration_targets=team_options,
                by_team=by_team,
                limit=limit,
                policy=policy,
                pitchers=pitchers,
            ),
        )

        logger.info("✅ Basic1 수집 완료: 총 %s명", len(pitchers))

        if series_key == "regular" and not by_team:
            _collect_pitcher_basic2_additional(
                Basic2AdditionalContext(
                    page=page,
                    year=year,
                    league_name=league_name,
                    series_info=series_info,
                    limit=limit,
                    policy=policy,
                    pitchers=pitchers,
                ),
            )
        elif by_team:
            logger.info("ℹ️ 팀별 순회 모드에서는 Basic2(상세 지표) 수집을 건너뜁니다.")

        browser.close()

    stats_list = list(pitchers.values())
    if limit:
        stats_list = stats_list[:limit]

    logger.info("✅ %s 크롤링 완료: %s명", series_info["name"], len(stats_list))
    summary, valid_stats_list = build_pitching_crawl_summary(stats_list)
    if summary["filtered_rows"]:
        logger.warning("⚠️ 투수 시즌 row 필터링: %s건 (%s)", summary["filtered_rows"], summary["failure_counts"])
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
        except DB_SAVE_EXCEPTIONS:
            logger.exception("❌ 투수 데이터 저장 실패")

    return stats_list


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_arguments() -> argparse.Namespace:
    """
    Parses arguments.

    Returns:
        The result of the operation.

    """
    parser = argparse.ArgumentParser(description="KBO 투수 기록 크롤러 (Basic1/Basic2)")
    parser.add_argument("--year", type=int, default=datetime.now(KST).year, help="시즌 연도 (기본: 당해 연도)")
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
    """Main entry point for this CLI command."""
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
        for series_key, series_info in SERIES_MAPPING.items():
            logger.info("\n🚀 %s 시작...", series_info["name"])
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
        logger.info("%s", "\n" + "=" * 60)
        logger.info("📈 전체 수집 요약 (%s년)", args.year)
        logger.info("%s", "=" * 60)
        total_players = 0
        for series_key, data in all_data.items():
            series_name = SERIES_MAPPING[series_key]["name"]
            logger.info("  %s: %s명", series_name, len(data))
            total_players += len(data)

        logger.info("\n총 수집 선수: %s명", total_players)


if __name__ == "__main__":
    main()
