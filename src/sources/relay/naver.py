"""데이터 소스: naver."""

from __future__ import annotations

from src.crawlers.relay_crawler import RelayCrawler
from src.crawlers.relay_outcome import RelayStatus

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
        # The typed entrypoint, not the raw one. Reading `crawl_game_relay`
        # directly meant this adapter could only see a payload or nothing, so a
        # game that stopped mid-game -- eight innings in, the ninth never
        # fetched -- arrived here indistinguishable from a finished one, and the
        # recovery path would store it as complete. Going through the attempt
        # keeps every relay consumer on one vocabulary.
        attempt = await self.crawler.crawl_relay_attempt(game_id, last_payload_hash=last_payload_hash)
        result = attempt.result

        events = list((result or {}).get("events") or [])
        raw_pbp_rows = list((result or {}).get("raw_pbp_rows") or [])
        status = (result or {}).get("status") or attempt.status.value

        notes: str | None
        if attempt.status is RelayStatus.NOT_MODIFIED:
            notes = "not_modified"
        elif attempt.status is RelayStatus.PARTIAL:
            # Kept as a note rather than dropped: the rows are stored, and the
            # note is what tells a later recovery that this game is short.
            notes = attempt.error_message or attempt.reason or "relay fetch stopped mid-game"
        elif attempt.status is RelayStatus.EMPTY:
            notes = attempt.error_message or attempt.reason or "No events extracted from Naver relay"
        else:
            notes = None if events or raw_pbp_rows else attempt.error_message or attempt.reason

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
            status=status,
            source_payload=(result or {}).get("source_payload"),
        )
