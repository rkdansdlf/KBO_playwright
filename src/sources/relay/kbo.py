from __future__ import annotations

from src.crawlers.pbp_crawler import PBPCrawler
from src.utils.team_codes import normalize_kbo_game_id

from .base import NormalizedRelayResult, RelaySourceAdapter, events_have_minimum_state


class KboRelayAdapter(RelaySourceAdapter):
    def __init__(self, crawler: PBPCrawler | None = None):
        super().__init__("kbo")
        self.crawler = crawler or PBPCrawler()

    async def fetch_game(self, game_id: str) -> NormalizedRelayResult:
        game_id = normalize_kbo_game_id(game_id)
        result = await self.crawler.crawl_game_events(game_id)
        events = list((result or {}).get("events") or [])
        failure_reason = getattr(self.crawler, "last_failure_reason", None)
        note_map = {
            "auth_required": "unsupported: kbo relay auth required",
            "redirected": "unsupported: kbo relay redirected",
            "empty": "No events extracted from KBO relay",
            "error": "KBO relay crawl failed",
        }
        notes = None if events else note_map.get(failure_reason, "No events extracted from KBO relay")
        return NormalizedRelayResult(
            game_id=game_id,
            source_name=self.source_name,
            events=events,
            raw_pbp_rows=[],
            has_event_state=events_have_minimum_state(events),
            has_raw_pbp=False,
            notes=notes,
        )
