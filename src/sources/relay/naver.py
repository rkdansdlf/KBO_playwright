"""데이터 소스: naver."""

from __future__ import annotations

from src.crawlers.relay_crawler import RelayCrawler

from .base import NormalizedRelayResult, RelaySourceAdapter, events_have_minimum_state


class NaverRelayAdapter(RelaySourceAdapter):
    """NaverRelayAdapter class."""

    def __init__(self, crawler: RelayCrawler | None = None) -> None:
        """Initialize a new instance.

        Args:
            crawler: Crawler.
            crawler: Crawler.

        """
        super().__init__("naver")

        self.crawler = crawler or RelayCrawler()

    async def fetch_game(
        self,
        game_id: str,
        last_payload_hash: str | None = None,
    ) -> NormalizedRelayResult:
        """Fetch game.

        Args:
            game_id: Game ID.
            last_payload_hash: Last seen payload hash.

        Returns:
            NormalizedRelayResult instance.

        """
        result = await self.crawler.crawl_game_relay(game_id, last_payload_hash=last_payload_hash)

        events = list((result or {}).get("events") or [])
        raw_pbp_rows = list((result or {}).get("raw_pbp_rows") or [])
        status = (result or {}).get("status")
        failure_reason = None
        getter = getattr(self.crawler, "get_last_failure_reason", None)
        if callable(getter):
            failure_reason = getter(game_id)

        notes: str | None
        if status == "not_modified":
            notes = "not_modified"
        else:
            notes = None if events or raw_pbp_rows else failure_reason or "No events extracted from Naver relay"

        return NormalizedRelayResult(
            game_id=game_id,
            source_name=self.source_name,
            events=events,
            raw_pbp_rows=raw_pbp_rows,
            has_event_state=events_have_minimum_state(events),
            has_raw_pbp=bool(raw_pbp_rows),
            notes=notes,
            parser_version=(result or {}).get("parser_version"),
            source_schema_version=(result or {}).get("source_schema_version"),
            payload_hash=(result or {}).get("payload_hash"),
        )
