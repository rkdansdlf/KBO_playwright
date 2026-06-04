"""
At-Bat grouping logic for KBO PBP events.

Groups sequential game_event dicts into at-bats by tracking
batter identity, result events, and inning/half boundaries.
"""

from __future__ import annotations

from typing import Any

# Event types that definitively end an at-bat
AT_BAT_TERMINAL_EVENTS = frozenset(
    {
        "batting",
    }
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

        # Detect at-bat boundary conditions
        needs_new_at_bat = False

        # 1. Inning or half change → new at-bat
        if (
            current_batter_key is not None
            and current_batter_key[:2] != (inning, half)
            or batter_name
            and current_batter is not None
            and batter_name != current_batter
            or has_seen_result_this_at_bat
            and event_type in AT_BAT_TERMINAL_EVENTS
            or current_batter is None
            and batter_name
        ):
            needs_new_at_bat = True

        if needs_new_at_bat:
            at_bat_seq += 1
            has_seen_result_this_at_bat = False

        if batter_name:
            current_batter = batter_name
            current_batter_key = new_batter_key

        event["at_bat_seq"] = at_bat_seq

        # Determine event role within the at-bat
        if event_type in AT_BAT_TERMINAL_EVENTS:
            # Check if this is a pitch-count text embedded before the result
            desc_clean = description.replace(" ", "")
            if "구" in desc_clean and not any(kw in desc_clean for kw in ["안타", "아웃", "홈런", "볼넷", "삼진"]):
                event["at_bat_event_role"] = ROLE_AT_BAT_PITCH
            else:
                event["at_bat_event_role"] = ROLE_AT_BAT_RESULT
                has_seen_result_this_at_bat = True
                result_seen_count += 1
        elif event_type == "steal":
            event["at_bat_event_role"] = ROLE_STOLEN_BASE
        elif event_type == "runner_advance":
            event["at_bat_event_role"] = ROLE_RUNNER_ADVANCE
        elif event_type == "runner_out":
            event["at_bat_event_role"] = ROLE_RUNNER_OUT
        else:
            event["at_bat_event_role"] = ROLE_UNKNOWN

        # Confidence: high if batter is explicitly named, medium otherwise
        if batter_name:
            event["at_bat_confidence"] = "high"
        else:
            event["at_bat_confidence"] = "medium"

    return events


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
                pass

        balls, strikes, _matched = advance_pitch_count(description, balls, strikes)

        # Update event with current count
        event["balls"] = balls
        event["strikes"] = strikes

    return events
