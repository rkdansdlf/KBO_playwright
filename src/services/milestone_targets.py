"""Milestone target calculation utilities."""

from __future__ import annotations

import re


def initial_milestone_target(category: str, current_val: int) -> int:
    """Calculate the initial target value for a milestone row.

    Used only for rows that do not exist yet.

    If ``category`` starts with an integer (e.g. '2000안타', '300세이브', '1,000탈삼진'),
    returns that integer with commas stripped. Otherwise returns the next 'round'
    milestone strictly greater than ``current_val``:
    - for ``current_val < 100``: step is 10 (with negative or 0 returning 10).
    - for ``current_val >= 100``: step is ``10 ** (len(str(current_val)) - 1) // 2``.

    Args:
        category: Milestone category name or description.
        current_val: Current accumulated value of the player.

    Returns:
        Initial milestone target value.

    """
    match = re.match(r"^\s*(\d[\d,]*)", category)
    if match:
        num_str = match.group(1).rstrip(",").replace(",", "")
        if num_str.isdigit():
            return int(num_str)

    if current_val <= 0:
        return 10

    if current_val < 100:  # noqa: PLR2004
        step = 10
    else:
        num_digits = len(str(current_val))
        step = (10 ** (num_digits - 1)) // 2

    return ((current_val // step) + 1) * step
