"""
At-Bat grouping logic for KBO PBP events.

Groups sequential game_event dicts into at-bats by tracking
batter identity, result events, and inning/half boundaries.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

# Event types that definitively end an at-bat
AT_BAT_TERMINAL_EVENTS = frozenset(
    {
        "batting",
    },
)

# Event roles within an at-bat
ROLE_AT_BAT_RESULT = "at_bat_result"
ROLE_RUNNER_ADVANCE = "runner_advance"
ROLE_RUNNER_OUT = "runner_out"
ROLE_STOLEN_BASE = "steal"
ROLE_AT_BAT_START = "at_bat_start"
ROLE_AT_BAT_PITCH = "at_bat_pitch"
ROLE_UNKNOWN = "unknown"


def group_events_into_at_bats(
    events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Annotate event dicts with at_bat_seq, at_bat_event_role, at_bat_confidence.

    Each event dict is mutated in-place AND returned in a new list.
    """
    if not events:
        return events

    at_bat_seq = 0
    current_batter = None
    current_batter_key = None
    has_seen_result_this_at_bat = False
    result_seen_count = 0

    for event in events:
        batter_name = event.get("batter_name") or event.get("batter") or ""
        inning = event.get("inning")
        half = event.get("inning_half")
        event_type = event.get("event_type", "unknown")
        description = str(event.get("description") or "")

        # Build a batter key that includes inning/half context
        new_batter_key = (inning, half, batter_name)

        if _needs_new_at_bat(
            ctx=AtBatContext(
                current_batter_key=current_batter_key,
                inning=inning,
                half=half,
                batter_name=batter_name,
                current_batter=current_batter,
                has_seen_result_this_at_bat=has_seen_result_this_at_bat,
                event_type=event_type,
            ),
        ):
            at_bat_seq += 1
            has_seen_result_this_at_bat = False

        if batter_name:
            current_batter = batter_name
            current_batter_key = new_batter_key

        event["at_bat_seq"] = at_bat_seq

        # Determine event role within the at-bat
        role = _event_role(event_type, description)
        event["at_bat_event_role"] = role
        if role == ROLE_AT_BAT_RESULT:
            has_seen_result_this_at_bat = True
            result_seen_count += 1

        # Confidence: high if batter is explicitly named, medium otherwise
        if batter_name:
            event["at_bat_confidence"] = "high"
        else:
            event["at_bat_confidence"] = "medium"

    return events


@dataclass
class AtBatContext:
    current_batter_key: tuple[int | str | None, str | None, str] | None
    inning: int | str | None
    half: str | None
    batter_name: str
    current_batter: str | None
    has_seen_result_this_at_bat: bool
    event_type: str


def _needs_new_at_bat(*, ctx: AtBatContext) -> bool:
    return (
        (ctx.current_batter_key is not None and ctx.current_batter_key[:2] != (ctx.inning, ctx.half))
        or (ctx.batter_name and ctx.current_batter is not None and ctx.batter_name != ctx.current_batter)
        or (ctx.has_seen_result_this_at_bat and ctx.event_type in AT_BAT_TERMINAL_EVENTS)
        or (ctx.current_batter is None and bool(ctx.batter_name))
    )


def _event_role(event_type: str, description: str) -> str:
    if event_type in AT_BAT_TERMINAL_EVENTS:
        desc_clean = description.replace(" ", "")
        if "구" in desc_clean and not any(kw in desc_clean for kw in ["안타", "아웃", "홈런", "볼넷", "삼진"]):
            return ROLE_AT_BAT_PITCH
        return ROLE_AT_BAT_RESULT
    if event_type == "steal":
        return ROLE_STOLEN_BASE
    if event_type == "runner_advance":
        return ROLE_RUNNER_ADVANCE
    if event_type == "runner_out":
        return ROLE_RUNNER_OUT
    return ROLE_UNKNOWN


def compute_at_bat_pitch_count(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Accumulate balls/strikes across pitch-count events within each at-bat.

    Mutates events in-place. Requires at_bat_seq to already be set.
    """
    current_at_bat = None
    balls = 0
    strikes = 0

    for event in events:
        at_bat = event.get("at_bat_seq")
        if at_bat is None:
            continue

        if at_bat != current_at_bat:
            current_at_bat = at_bat
            balls = 0
            strikes = 0

        description = str(event.get("description") or "")
        from src.utils.relay_text import advance_pitch_count

        preset_balls = event.get("balls")
        preset_strikes = event.get("strikes")
        if preset_balls is not None or preset_strikes is not None:
            try:
                balls = max(balls, int(preset_balls or 0))
                strikes = max(strikes, int(preset_strikes or 0))
            except (TypeError, ValueError):
                logger.debug("Invalid preset ball/strike value: balls=%s strikes=%s", preset_balls, preset_strikes)

        balls, strikes, _matched = advance_pitch_count(description, balls, strikes)

        # Update event with current count
        event["balls"] = balls
        event["strikes"] = strikes

    return events
