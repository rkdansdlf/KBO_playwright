from __future__ import annotations

from src.crawlers.relay_crawler import RelayCrawler

from .base import NormalizedRelayResult, RelaySourceAdapter, events_have_minimum_state


class NaverRelayAdapter(RelaySourceAdapter):
    def __init__(self, crawler: RelayCrawler | None = None):
        super().__init__("naver")
        self.crawler = crawler or RelayCrawler()

    async def fetch_game(self, game_id: str) -> NormalizedRelayResult:
        result = await self.crawler.crawl_game_relay(game_id)
        events = list((result or {}).get("events") or [])
        raw_pbp_rows = list((result or {}).get("raw_pbp_rows") or [])
        notes = None if events else "No events extracted from Naver relay"
        return NormalizedRelayResult(
            game_id=game_id,
            source_name=self.source_name,
            events=events,
            raw_pbp_rows=raw_pbp_rows,
            has_event_state=events_have_minimum_state(events),
            has_raw_pbp=bool(raw_pbp_rows),
            notes=notes,
        )
