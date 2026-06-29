"""Shared WPA transition helpers for relay event pipelines."""

from __future__ import annotations

from collections.abc import Mapping, MutableSequence
from typing import Any

from src.services.wpa_calculator import WPACalculator


def get_event_value(event: object, key: str) -> object | None:
    """
    Get event value.

    Args:
        event: Event.
        key: Key.
        event: Event.
        key: Key.
        event: Event.
        key: Key.

    Returns:
        The result of the operation.

    """
    if isinstance(event, Mapping):
        return event.get(key)
    return getattr(event, key, None)


def format_base_string(runners: int | None) -> str:
    """
    Format base string.

    Args:
        runners: Runners.
        runners: Runners.
        runners: Runners.

    Returns:
        String result.

    """
    value = int(runners or 0)

    return f"{'1' if (value & 1) else '-'}{'2' if (value & 2) else '-'}{'3' if (value & 4) else '-'}"


def parse_base_string(value: object) -> int | None:
    """
    Parse base string.

    Args:
        value: Value.
        value: Value.
        value: Value.

    Returns:
        The result of the operation.

    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    runners = 0
    if len(text) >= 1 and text[0] not in {"-", "0"}:
        runners |= 1
    if len(text) >= 2 and text[1] not in {"-", "0"}:
        runners |= 2
    if len(text) >= 3 and text[2] not in {"-", "0"}:
        runners |= 4
    return runners


def coerce_int(value: object) -> int | None:
    """
    Handle the coerce int operation.

    Args:
        value: Value.
        value: Value.
        value: Value.

    Returns:
        The result of the operation.

    """
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def event_runner_state(event: object) -> int | None:
    """
    Handle the event runner state operation.

    Args:
        event: Event.
        event: Event.
        event: Event.

    Returns:
        The result of the operation.

    """
    base_state = coerce_int(get_event_value(event, "base_state"))

    if base_state is not None:
        return base_state
    for key in ("bases_after", "bases_before"):
        parsed = parse_base_string(get_event_value(event, key))
        if parsed is not None:
            return parsed
    return None


def event_has_transition_state(event: object) -> bool:
    """
    Handle the event has transition state operation.

    Args:
        event: Event.
        event: Event.
        event: Event.

    Returns:
        True if successful, False otherwise.

    """
    inning_half = get_event_value(event, "inning_half")

    return all(
        (
            coerce_int(get_event_value(event, "inning")) is not None,
            inning_half in {"top", "bottom"},
            coerce_int(get_event_value(event, "outs")) is not None,
            str(get_event_value(event, "description") or "").strip() != "",
            coerce_int(get_event_value(event, "home_score")) is not None,
            coerce_int(get_event_value(event, "away_score")) is not None,
            event_runner_state(event) is not None,
        ),
    )


def event_has_wpa_state(event: object) -> bool:
    """
    Handle the event has wpa state operation.

    Args:
        event: Event.
        event: Event.
        event: Event.

    Returns:
        True if successful, False otherwise.

    """
    return all(
        (
            event_has_transition_state(event),
            get_event_value(event, "wpa") is not None,
            get_event_value(event, "win_expectancy_before") is not None,
            get_event_value(event, "win_expectancy_after") is not None,
        ),
    )


def apply_wpa_transitions(
    events: MutableSequence[dict[str, Any]],
    *,
    calculator: WPACalculator | None = None,
    only_missing: bool = False,
) -> None:
    """
    Handle the apply wpa transitions operation.

    Args:
        events: Events.
        calculator: Calculator instance.
        only_missing: If True, only process missing entries.
        events: Events.
        calculator: Calculator instance.
        only_missing: If True, only process missing entries.
        events: Events.

    """
    calculator = calculator or WPACalculator()

    previous: dict[str, Any] | None = None

    for event in events:
        if not event_has_transition_state(event):
            continue
        if only_missing and event_has_wpa_state(event):
            previous = event
            continue

        inning = coerce_int(event.get("inning")) or 1
        inning_half = event.get("inning_half")
        is_bottom = inning_half == "bottom"

        if previous is None:
            outs_before, runners_before, score_diff_before = 0, 0, 0
        else:
            if previous.get("inning") != event.get("inning") or previous.get("inning_half") != inning_half:
                outs_before, runners_before = 0, 0
            else:
                outs_before = coerce_int(previous.get("outs")) or 0
                runners_before = event_runner_state(previous) or 0
            score_diff_before = (coerce_int(previous.get("home_score")) or 0) - (
                coerce_int(previous.get("away_score")) or 0
            )

        outs_after = coerce_int(event.get("outs")) or 0
        runners_after = event_runner_state(event) or 0
        score_diff_after = (coerce_int(event.get("home_score")) or 0) - (coerce_int(event.get("away_score")) or 0)

        we_before = calculator.get_win_probability(
            inning,
            is_bottom=is_bottom,
            outs=outs_before,
            runners=runners_before,
            score_diff=score_diff_before,
        )
        we_after = calculator.get_win_probability(
            inning,
            is_bottom=is_bottom,
            outs=outs_after,
            runners=runners_after,
            score_diff=score_diff_after,
        )

        event["bases_before"] = format_base_string(runners_before)
        event["bases_after"] = format_base_string(runners_after)
        event["score_diff"] = score_diff_after
        event["base_state"] = runners_after
        event["win_expectancy_before"] = we_before
        event["win_expectancy_after"] = we_after
        event["wpa"] = round(we_after - we_before if is_bottom else we_before - we_after, 4)
        previous = event
