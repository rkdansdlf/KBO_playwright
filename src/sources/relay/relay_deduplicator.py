"""Deduplication utility for play-by-play text relay events."""

from __future__ import annotations

import hashlib
from typing import Any


class RelayDeduplicator:
    """Sliding-window deduplication queue for incremental streaming.

    Tracks previously processed event identifiers/hashes to filter out duplicates
    across consecutive crawls of the same game, with support for cross-provider
    semantic deduplication, in-place corrections, and canonical ordering.
    """

    def __init__(self, window_size: int = 200) -> None:
        """Initialize the deduplicator with a target window size.

        Args:
            window_size: Maximum number of historical event IDs to keep in memory.

        """
        self.window_size = window_size
        self._seen_ids: list[str] = []
        self._seen_content_hashes: dict[str, str] = {}

    @staticmethod
    def semantic_event_key(event: dict[str, Any]) -> str:
        """Compute a canonical semantic key for baseball events independent of provider ID."""
        inn = str(event.get("inning") or "")
        half = str(event.get("inning_half") or "").lower()
        seq = str(event.get("source_row_index") or event.get("event_seq") or "")
        desc = str(event.get("play_description") or event.get("text") or event.get("description") or "").strip()
        outs = str(event.get("outs") if event.get("outs") is not None else "")
        h_score = str(event.get("home_score") if event.get("home_score") is not None else "")
        a_score = str(event.get("away_score") if event.get("away_score") is not None else "")
        batter = str(event.get("batter_name") or event.get("batter_id") or "")
        raw = f"{inn}:{half}:{seq}:{desc}:{outs}:{h_score}:{a_score}:{batter}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def event_content_hash(event: dict[str, Any]) -> str:
        """Hash substantive event content to detect revisions/corrections."""
        desc = str(event.get("play_description") or event.get("text") or event.get("description") or "")
        res = str(event.get("result_code") or event.get("result") or "")
        h_score = str(event.get("home_score") or "")
        a_score = str(event.get("away_score") or "")
        raw = f"{desc}|{res}|{h_score}|{a_score}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def filter_new_events(
        self,
        events: list[dict[str, Any]],
        key_field: str = "provider_log_id",
        *,
        use_semantic_key: bool = False,
        allow_corrections: bool = True,
    ) -> list[dict[str, Any]]:
        """Filter a list of events to return only new events or revisions.

        Args:
            events: List of event dictionaries to filter.
            key_field: Dictionary key to use as the unique identifier.
            use_semantic_key: Whether to match events across providers by baseball semantics.
            allow_corrections: Whether revisions with changed content are yielded as updates.

        Returns:
            Filtered list containing only new or revised events.

        """
        new_events = []
        for event in events:
            if use_semantic_key:
                event_id = self.semantic_event_key(event)
            else:
                event_id = event.get(key_field)
                if not event_id:
                    event_id = self.semantic_event_key(event)

            c_hash = self.event_content_hash(event)

            if event_id not in self._seen_ids:
                new_events.append(event)
                self._seen_ids.append(event_id)
                self._seen_content_hashes[event_id] = c_hash
                if len(self._seen_ids) > self.window_size:
                    dropped = self._seen_ids.pop(0)
                    self._seen_content_hashes.pop(dropped, None)
            elif allow_corrections:
                prev_hash = self._seen_content_hashes.get(event_id)
                if prev_hash is not None and prev_hash != c_hash:
                    revised_event = dict(event)
                    revised_event["_is_correction"] = True
                    revised_event["_previous_content_hash"] = prev_hash
                    new_events.append(revised_event)
                    self._seen_content_hashes[event_id] = c_hash

        return new_events

    @staticmethod
    def order_events_canonically(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Order out-of-order events into chronological baseball sequence."""

        def sort_key(ev: dict[str, Any]) -> tuple[int, int, int, int]:
            inn = int(ev.get("inning") or 1)
            half = 0 if str(ev.get("inning_half") or "").lower() in ("top", "t") else 1
            seq = int(ev.get("event_seq") or ev.get("source_row_index") or 0)
            outs = int(ev.get("outs") or 0)
            return (inn, half, seq, outs)

        return sorted(events, key=sort_key)

    def reset(self) -> None:
        """Clear the deduplication cache."""
        self._seen_ids.clear()
        self._seen_content_hashes.clear()
