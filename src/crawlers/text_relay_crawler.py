"""Text Relay Crawler - KBO 문자중계(Play-by-Play) 수집 모듈.

KBO 공식 웹사이트의 LiveText.aspx 페이지에 접근하여
경기별 타석/투구 기록을 스크래핑하고 DataFrame으로 변환 후 CSV로 저장합니다.

수집 데이터 구조:
    [이닝, 타석번호, 투수명, 타자명, 구종, 구속, 결과]

사용 예시:
    crawler = TextRelayCrawler()
    df = await crawler.crawl_game_relay("20260412SKLG0", save=True)
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page

from src.utils.compliance import compliance
from src.utils.playwright_pool import AsyncPlaywrightPool
from src.utils.playwright_retry import LONG_TIMEOUT, NAV_TIMEOUT, SEL_TIMEOUT
from src.utils.request_policy import RequestPolicy
from src.utils.text_parser import KBOTextParser

logger = logging.getLogger(__name__)

TEXT_RELAY_CRAWLER_EXCEPTIONS = (
    PlaywrightError,
    TimeoutError,
    asyncio.TimeoutError,
    RuntimeError,
    ValueError,
    TypeError,
    OSError,
)

KBO_BASE_URL = "https://www.koreabaseball.com"
LIVE_TEXT_URL = f"{KBO_BASE_URL}/Game/LiveText.aspx"
SCOREBOARD_URL = f"{KBO_BASE_URL}/Schedule/ScoreBoard.aspx"

DEFAULT_CSV_ENCODING = "utf-8-sig"
DEFAULT_OUTPUT_DIR = "data"


@dataclass
class RelayRow:
    """문자중계 단일 행 데이터 구조."""

    inning: int = 0
    inning_half: str = ""
    at_bat_num: int = 0
    pitcher_name: str = ""
    batter_name: str = ""
    pitch_type: str = ""
    pitch_speed: str = ""
    result: str = ""
    balls: int = 0
    strikes: int = 0
    outs: int = 0
    runners: str = ""
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "이닝": self.inning,
            "이닝_초말": self.inning_half,
            "타석번호": self.at_bat_num,
            "투수명": self.pitcher_name,
            "타자명": self.batter_name,
            "구종": self.pitch_type,
            "구속": self.pitch_speed,
            "결과": self.result,
            "볼": self.balls,
            "스트라이크": self.strikes,
            "아웃": self.outs,
            "주자": self.runners,
            "상세": self.description,
        }


@dataclass
class RelayCrawlResult:
    """문자중계 크롤링 결과 컨테이너."""

    game_id: str
    game_date: str
    rows: list[RelayRow] = field(default_factory=list)
    status: str = "pending"
    error_message: str | None = None

    def to_dataframe(self) -> pd.DataFrame:
        if not self.rows:
            return pd.DataFrame(
                columns=[
                    "이닝",
                    "이닝_초말",
                    "타석번호",
                    "투수명",
                    "타자명",
                    "구종",
                    "구속",
                    "결과",
                    "볼",
                    "스트라이크",
                    "아웃",
                    "주자",
                    "상세",
                ]
            )
        data = [row.to_dict() for row in self.rows]
        return pd.DataFrame(data)

    def save_csv(self, output_dir: str = DEFAULT_OUTPUT_DIR) -> Path:
        df = self.to_dataframe()
        output_path = Path(output_dir) / f"{self.game_id}_text_relay.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False, encoding=DEFAULT_CSV_ENCODING)
        logger.info("[SAVE] Text relay saved to %s (%d rows)", output_path, len(df))
        return output_path


class TextRelayCrawler:
    """KBO 문자중계 크롤러.

    Playwright 비동기 기반으로 특정 경기 ID의 문자중계 페이지에 진입하여
    전체 투구 기록 리스트를 스크래핑합니다.

    수집된 데이터는 DataFrame으로 변환되어 CSV로 저장됩니다.

    Attributes:
        base_url: KBO LiveText.aspx URL
        policy: 요청 정책 (딜레이, 재시도 등)
        pool: Playwright 브라우저 풀
        output_dir: CSV 출력 디렉토리
    """

    def __init__(
        self,
        request_delay: float = 1.0,
        policy: RequestPolicy | None = None,
        pool: AsyncPlaywrightPool | None = None,
        output_dir: str = DEFAULT_OUTPUT_DIR,
    ) -> None:
        self.base_url = LIVE_TEXT_URL
        self.policy = policy or RequestPolicy.with_delay(request_delay, request_delay + 0.5)
        self.pool = pool
        self._context_kwargs = self.policy.build_context_kwargs(locale="ko-KR")
        self.output_dir = output_dir
        self.last_failure_reason: str | None = None

    @staticmethod
    def _is_auth_redirect(page: Page) -> bool:
        """인증 리다이렉트 여부 확인."""
        return "Error.html" in page.url or "Login.aspx" in page.url

    async def _prepare_live_text_page(self, page: Page, game_date: str, url: str) -> bool:
        """LiveText 페이지 준비 (Referer warmup 포함)."""
        logger.info("[FETCH] Text Relay Data: %s", url)
        if not await compliance.is_allowed(url):
            logger.info("[COMPLIANCE] Navigation to %s aborted.", url)
            return False

        await self.policy.delay_async(host="www.koreabaseball.com")
        parent_url = f"{SCOREBOARD_URL}?gameDate={game_date}"
        logger.info("[AUTH] Warming up session on Scoreboard: %s", parent_url)
        await page.goto(parent_url, wait_until="networkidle", timeout=NAV_TIMEOUT)
        await asyncio.sleep(2)

        logger.info("[FETCH] Navigating to Relay page with Referer: %s", url)
        await page.goto(url, wait_until="domcontentloaded", timeout=LONG_TIMEOUT, referer=parent_url)
        return True

    async def _wait_for_relay_container(self, page: Page, game_id: str) -> bool:
        """문자중계 컨테이너 대기."""
        try:
            await page.wait_for_selector('div[id^="numCont"]', timeout=SEL_TIMEOUT)
        except (PlaywrightError, TimeoutError):
            logger.warning("No relay containers found for %s", game_id)
            body = await page.content()
            if "데이터가 없습니다" in body or "취소" in body:
                self.last_failure_reason = "empty"
                return False
        return True

    @staticmethod
    def _build_extraction_script() -> str:
        """Playwright page.evaluate용 JS 추출 스크립트 빌드."""
        return """
        () => {
            const getSpans = (container) => {
                if (!container) return [];
                return Array.from(container.querySelectorAll('span')).map(span => ({
                    text: span.innerText.trim(),
                    class: span.className
                })).filter(item => item.text !== "");
            };

            const mainContainer = document.querySelector('#numCont11');
            let results = getSpans(mainContainer);

            if (results.length === 0) {
                for (let i = 1; i <= 12; i++) {
                    if (i === 11) continue;
                    const container = document.querySelector('#numCont' + i);
                    const inningSpans = getSpans(container);
                    results = results.concat(inningSpans);
                }
            }
            return results;
        }
        """

    @staticmethod
    def _parse_inning_header(text: str, cls: str) -> tuple[int, str] | None:
        """이닝 헤더 파싱 (예: '3회초' -> (3, '초'))."""
        if "blue" not in cls or "회" not in text:
            return None
        match = re.search(r"(\d+)회(초|말)", text)
        if match:
            inning = int(match.group(1))
            half = "초" if match.group(2) == "초" else "말"
            return inning, half
        return None

    @staticmethod
    def _is_event_text(text: str, cls: str) -> bool:
        """이벤트 텍스트 여부 확인."""
        if "normaiflTxt" not in cls and "red" not in cls:
            return False
        return "경기 준비중" not in text and "경기 시작" not in text

    @staticmethod
    def _extract_pitch_info(text: str) -> tuple[str, str]:
        """구종/구속 추출 (예: '145km/h 슬라이더' -> ('슬라이더', '145'))."""
        pitch_type = ""
        pitch_speed = ""

        # 구속 패턴: 145km/h, 150km 등
        speed_match = re.search(r"(\d+)\s*km", text)
        if speed_match:
            pitch_speed = speed_match.group(1)

        # 구종 패턴
        pitch_types = [
            "포심",
            "슬라이더",
            "커브",
            "체인지업",
            "싱커",
            "커터",
            "스플리터",
            "투심",
            "너클볼",
            "스크루볼",
            "패스트볼",
            "슬로커브",
            "하드슬라이더",
        ]
        for pt in pitch_types:
            if pt in text:
                pitch_type = pt
                break

        return pitch_type, pitch_speed

    @staticmethod
    def _extract_player_names(text: str) -> tuple[str, str]:
        """투수명/타자명 추출."""
        pitcher = ""
        batter = ""

        # 타자명 추출: "타자: 김하성" 패턴
        batter_match = re.search(r"타자[:\s]+([가-힣]+)", text)
        if batter_match:
            batter = batter_match.group(1)

        # 투수명 추출: "투수: 류현진" 패턴
        pitcher_match = re.search(r"투수[:\s]+([가-힣]+)", text)
        if pitcher_match:
            pitcher = pitcher_match.group(1)

        return pitcher, batter

    @staticmethod
    def _extract_result(text: str) -> str:
        """결과 추출 (스트라이크/볼/아웃/안타 등)."""
        result_keywords = [
            "스트라이크",
            "볼",
            "파울",
            "헛스윙",
            "안타",
            "1루타",
            "2루타",
            "3루타",
            "홈런",
            "아웃",
            "삼진",
            "볼넷",
            "사구",
            "실책",
            "희생번트",
            "희생플라이",
            "병살",
            "삼중살",
            "도루",
            "주루사",
            "견제사",
        ]
        for keyword in result_keywords:
            if keyword in text:
                return keyword
        return text.strip()

    @staticmethod
    def _extract_pitch_count(text: str) -> tuple[int, int]:
        """볼/스트라이크 카운트 추출."""
        balls = 0
        strikes = 0

        # "2볼 1스트라이크" 패턴
        ball_match = re.search(r"(\d+)\s*볼", text)
        strike_match = re.search(r"(\d+)\s*스트라이크", text)

        if ball_match:
            balls = int(ball_match.group(1))
        if strike_match:
            strikes = int(strike_match.group(1))

        return balls, strikes

    @staticmethod
    def _extract_runners(text: str) -> str:
        """주자 상태 추출 (예: '1루', '1,2루', '만루')."""
        if "만루" in text:
            return "만루"

        runners = [f"{base}루" for base in ("1", "2", "3") if re.search(rf"{base}(?:루|$|[,\s])", text)]

        return ",".join(runners) if runners else ""

    def _parse_relay_spans(self, raw_spans: list[dict[str, str]]) -> list[RelayRow]:
        """원시 span 데이터를 RelayRow 리스트로 파싱."""
        rows: list[RelayRow] = []
        current_inning = 0
        current_half = ""
        at_bat_num = 0
        current_outs = 0
        current_runners = ""

        for item in raw_spans:
            text = item["text"]
            cls = item["class"]

            # 이닝 헤더 처리
            inning_info = self._parse_inning_header(text, cls)
            if inning_info:
                current_inning, current_half = inning_info
                at_bat_num = 0
                current_outs = 0
                continue

            # 구분선 스킵
            if "---" in text and len(text) > 10:
                continue

            # 이벤트 텍스트 확인
            if not self._is_event_text(text, cls):
                continue

            # 타석번호 증가
            at_bat_num += 1

            # 아웃/주자 상태 업데이트
            parsed_outs = KBOTextParser.parse_outs(text)
            if parsed_outs > 0:
                current_outs = parsed_outs

            # 정보 추출
            pitcher, batter = self._extract_player_names(text)
            pitch_type, pitch_speed = self._extract_pitch_info(text)
            result = self._extract_result(text)
            balls, strikes = self._extract_pitch_count(text)
            runners = self._extract_runners(text)

            row = RelayRow(
                inning=current_inning,
                inning_half=current_half,
                at_bat_num=at_bat_num,
                pitcher_name=pitcher,
                batter_name=batter,
                pitch_type=pitch_type,
                pitch_speed=pitch_speed,
                result=result,
                balls=balls,
                strikes=strikes,
                outs=current_outs,
                runners=runners or current_runners,
                description=text,
            )
            rows.append(row)

        return rows

    async def _extract_relay_rows(self, page: Page) -> list[RelayRow]:
        """페이지에서 문자중계 행 추출."""
        extraction_script = self._build_extraction_script()

        try:
            raw_spans = await page.evaluate(extraction_script)
            if not raw_spans:
                return []

            # 역순으로 저장되므로 순서 뒤집기
            raw_spans.reverse()
            return self._parse_relay_spans(raw_spans)
        except TEXT_RELAY_CRAWLER_EXCEPTIONS:
            logger.exception("Error extracting relay data (JS)")
            return []

    async def crawl_game_relay(
        self,
        game_id: str,
        *,
        save: bool = False,
    ) -> RelayCrawlResult:
        """특정 경기의 문자중계 데이터를 수집합니다.

        Args:
            game_id: KBO 경기 ID (예: "20260412SKLG0")
            save: CSV 저장 여부

        Returns:
            RelayCrawlResult 객체
        """
        self.last_failure_reason = None
        game_date = game_id[:8]
        url = f"{self.base_url}?leagueId=1&seriesId=0&gameId={game_id}&gyear={game_date[:4]}"

        result = RelayCrawlResult(game_id=game_id, game_date=game_date)

        pool = self.pool or AsyncPlaywrightPool(max_pages=1, context_kwargs=self._context_kwargs, requires_auth=True)
        owns_pool = self.pool is None

        if owns_pool:
            await pool.start()

        try:
            page = await pool.acquire()
            try:
                # 페이지 준비
                if not await self._prepare_live_text_page(page, game_date, url):
                    result.status = "blocked"
                    result.error_message = "compliance_blocked"
                    return result

                # 인증 리다이렉트 확인
                if self._is_auth_redirect(page):
                    logger.warning("[AUTH] Redirected to %s, retrying...", page.url)
                    await pool.close()
                    await pool.start()
                    page = await pool.acquire()
                    if not await self._prepare_live_text_page(page, game_date, url):
                        result.status = "auth_failed"
                        result.error_message = "authentication_required"
                        return result

                # 컨테이너 대기
                if not await self._wait_for_relay_container(page, game_id):
                    result.status = "empty"
                    result.error_message = "no_data"
                    return result

                # 데이터 추출
                logger.info("[INFO] Extracting Text Relay Data for %s...", game_id)
                rows = await self._extract_relay_rows(page)

                if not rows:
                    result.status = "empty"
                    result.error_message = "no_events_found"
                    return result

                result.rows = rows
                result.status = "success"

                # CSV 저장
                if save:
                    output_path = result.save_csv(self.output_dir)
                    logger.info("[SAVE] Saved %d rows to %s", len(rows), output_path)

            finally:
                await pool.release(page)
        except TEXT_RELAY_CRAWLER_EXCEPTIONS:
            logger.exception("Text relay crawl failed for %s", game_id)
            result.status = "error"
            result.error_message = "crawl_exception"
            self.last_failure_reason = "error"
        finally:
            if owns_pool:
                await pool.close()

        return result

    async def crawl_games(
        self,
        game_ids: list[str],
        *,
        save: bool = False,
    ) -> list[RelayCrawlResult]:
        """여러 경기의 문자중계 데이터를 일괄 수집합니다.

        Args:
            game_ids: KBO 경기 ID 리스트
            save: CSV 저장 여부

        Returns:
            RelayCrawlResult 리스트
        """
        results: list[RelayCrawlResult] = []
        for game_id in game_ids:
            result = await self.crawl_game_relay(game_id, save=save)
            results.append(result)
            logger.info(
                "[PROGRESS] %s: %s (%d rows)",
                game_id,
                result.status,
                len(result.rows),
            )
        return results


async def crawl_text_relay(
    game_id: str,
    *,
    save: bool = False,
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> pd.DataFrame | None:
    """단일 경기 문자중계 수집 편의 함수.

    Args:
        game_id: KBO 경기 ID
        save: CSV 저장 여부
        output_dir: 출력 디렉토리

    Returns:
        DataFrame 또는 None (실패 시)
    """
    crawler = TextRelayCrawler(output_dir=output_dir)
    result = await crawler.crawl_game_relay(game_id, save=save)
    if result.status == "success":
        return result.to_dataframe()
    logger.error("Failed to crawl %s: %s", game_id, result.error_message)
    return None


async def crawl_text_relays(
    game_ids: list[str],
    *,
    save: bool = False,
    output_dir: str = DEFAULT_OUTPUT_DIR,
) -> list[str]:
    """여러 경기 문자중계 수집 편의 함수.

    Args:
        game_ids: KBO 경기 ID 리스트
        save: CSV 저장 여부
        output_dir: 출력 디렉토리

    Returns:
        저장된 파일 경로 리스트
    """
    crawler = TextRelayCrawler(output_dir=output_dir)
    results = await crawler.crawl_games(game_ids, save=save)
    saved_paths: list[str] = []
    for result in results:
        if result.status == "success":
            path = result.save_csv(output_dir)
            saved_paths.append(str(path))
    return saved_paths
