"""Declarative Crawler Registry and Factory for KBO data collection components."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from src.crawlers.base import BaseCrawler

logger = logging.getLogger(__name__)


class CrawlerCategory(StrEnum):
    """Domain categories for KBO Crawlers."""

    SCHEDULE = "schedule"
    GAME_DETAIL = "game_detail"
    STATS = "stats"
    ROSTER = "roster"
    AWARDS = "awards"
    FACILITIES = "facilities"
    FUTURES = "futures"
    MEDIA = "media"
    GENERAL = "general"


@dataclass
class CrawlerMetadata:
    """Metadata describing a crawler's domain, class, and capabilities."""

    crawler_name: str
    category: CrawlerCategory
    crawler_cls: type[BaseCrawler]
    description: str = ""
    supports_playwright: bool = True
    default_request_delay: float = 1.0


class CrawlerRegistry:
    """Registry maintaining all available KBO crawlers and providing instantiation factories."""

    _registry: ClassVar[dict[str, CrawlerMetadata]] = {}
    _builtins_loaded: ClassVar[bool] = False

    @classmethod
    def _ensure_builtins(cls) -> None:
        """Lazy load built-in crawlers on first access to avoid eager DB imports."""
        if not cls._builtins_loaded:
            cls._builtins_loaded = True
            _register_builtin_crawlers()

    @classmethod
    def register(cls, metadata: CrawlerMetadata) -> None:
        """Register a crawler metadata definition."""
        cls._registry[metadata.crawler_name] = metadata

    @classmethod
    def get(cls, crawler_name: str) -> CrawlerMetadata | None:
        """Get crawler metadata by name."""
        cls._ensure_builtins()
        return cls._registry.get(crawler_name)

    @classmethod
    def list_all(cls) -> list[CrawlerMetadata]:
        """List all registered crawler metadata."""
        cls._ensure_builtins()
        return list(cls._registry.values())

    @classmethod
    def list_by_category(cls, category: CrawlerCategory | str) -> list[CrawlerMetadata]:
        """List crawler metadata filtered by category."""
        cls._ensure_builtins()
        cat_str = category.value if isinstance(category, CrawlerCategory) else str(category)
        return [m for m in cls._registry.values() if m.category.value == cat_str]

    @classmethod
    def instantiate(cls, crawler_name: str, **kwargs: object) -> BaseCrawler:
        """Instantiate a crawler instance by its registered name."""
        meta = cls.get(crawler_name)
        if not meta:
            err_msg = f"Crawler '{crawler_name}' is not registered in CrawlerRegistry."
            raise KeyError(err_msg)
        return meta.crawler_cls(**kwargs)  # type: ignore[arg-type]


# Register initial core crawlers lazily
def _register_builtin_crawlers() -> None:
    try:
        from src.crawlers.award_crawler import AwardCrawler
        from src.crawlers.daily_roster_crawler import DailyRosterCrawler
        from src.crawlers.fan_culture_crawler import FanCultureCrawler
        from src.crawlers.food_crawler import FoodCrawler
        from src.crawlers.parking_crawler import ParkingCrawler
        from src.crawlers.roster_transaction_crawler import RosterTransactionCrawler
        from src.crawlers.schedule_crawler import ScheduleCrawler
        from src.crawlers.seat_crawler import SeatCrawler
        from src.crawlers.ticket_crawler import TicketCrawler

        builtins = [
            CrawlerMetadata(
                "schedule",
                CrawlerCategory.SCHEDULE,
                ScheduleCrawler,
                "KBO 정규/포스트시즌 경기 일정 크롤러",
            ),
            CrawlerMetadata(
                "daily_roster",
                CrawlerCategory.ROSTER,
                DailyRosterCrawler,
                "KBO 구단 일별 1군 엔트리 로스터 크롤러",
            ),
            CrawlerMetadata(
                "roster_transactions",
                CrawlerCategory.ROSTER,
                RosterTransactionCrawler,
                "KBO 일별 선수단 등록/말소 이동 크롤러",
            ),
            CrawlerMetadata(
                "awards",
                CrawlerCategory.AWARDS,
                AwardCrawler,
                "KBO 역대 수상 내역 (MVP, 신인상, GG 등) 크롤러",
            ),
            CrawlerMetadata(
                "seat",
                CrawlerCategory.FACILITIES,
                SeatCrawler,
                "KBO 구장별 좌석 배치도 및 구역 정보 크롤러",
            ),
            CrawlerMetadata(
                "parking",
                CrawlerCategory.FACILITIES,
                ParkingCrawler,
                "KBO 구장별 주차장 및 주차 요금 크롤러",
            ),
            CrawlerMetadata(
                "food",
                CrawlerCategory.FACILITIES,
                FoodCrawler,
                "KBO 구장별 식음료 매장 및 메뉴 크롤러",
            ),
            CrawlerMetadata(
                "ticket",
                CrawlerCategory.FACILITIES,
                TicketCrawler,
                "KBO 티켓 예매 오픈 일정 및 가격 크롤러",
            ),
            CrawlerMetadata(
                "fan_culture",
                CrawlerCategory.MEDIA,
                FanCultureCrawler,
                "KBO 응원가 및 팬 문화 미디어 크롤러",
            ),
        ]

        for m in builtins:
            CrawlerRegistry.register(m)
    except (ImportError, TypeError, AttributeError) as exc:
        logger.debug("Deferred crawler registration encountered: %s", exc)
