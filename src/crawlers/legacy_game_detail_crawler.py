"""Legacy KBO game detail crawler for historical (2001-2009) review & boxscore pages."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from src.parsers.game_detail_parser import parse_game_detail_html
from src.utils.request_policy import RequestPolicy

if TYPE_CHECKING:
    from playwright.async_api import Page as AsyncPage
    from playwright.sync_api import Page as SyncPage
    from sqlalchemy.orm import Session

    from src.services.player_id_resolver import PlayerIdResolver

logger = logging.getLogger(__name__)


class LegacyGameDetailCrawler:
    """Crawl and extract historical (2001-2009) KBO GameCenter/Schedule review boxscore pages."""

    def __init__(
        self,
        request_delay: float | None = None,
        resolver: PlayerIdResolver | None = None,
    ) -> None:
        """Initialize the legacy game detail crawler.

        Args:
            request_delay: Throttle delay between HTTP requests in seconds.
            resolver: Optional PlayerIdResolver for player ID mapping.

        """
        self.policy = RequestPolicy.with_delay(request_delay)
        self.resolver = resolver

    def extract_from_html(
        self,
        html: str,
        game_id: str,
        game_date: str,
        db_session: Session | None = None,
    ) -> dict[str, Any]:
        """Extract structured game details and boxscore payload from raw HTML.

        Args:
            html: Raw HTML content of the boxscore/review page.
            game_id: KBO Game ID (e.g. 20090404HHSK0).
            game_date: Game Date string (YYYYMMDD).
            db_session: Database session for resolving missing player IDs.

        Returns:
            Structured dictionary payload matching save_game_detail requirements.

        """
        return parse_game_detail_html(
            html=html,
            game_id=game_id,
            game_date=game_date,
            db_session=db_session or (self.resolver.session if self.resolver else None),
        )

    def extract_game_details(
        self,
        page_or_html: SyncPage | AsyncPage | str,
        game_id: str,
        game_date: str,
        db_session: Session | None = None,
    ) -> dict[str, Any]:
        """Extract game details from a Playwright Page object or HTML string.

        Args:
            page_or_html: Playwright Page instance or HTML string.
            game_id: KBO Game ID.
            game_date: Game Date.
            db_session: Optional DB session.

        Returns:
            Structured boxscore dictionary.

        """
        html = page_or_html if isinstance(page_or_html, str) else page_or_html.content()
        return self.extract_from_html(html, game_id, game_date, db_session=db_session)
