"""Play identity for PBP chunks that survives a team-code change.

The same play is stored under the old team codes (``OB vs HT``) and the current ones
(``DB vs KIA``). Comparing the text directly misses that, so a play is identified by its
``(date, inning, batter, event)`` with both team codes mapped to the current ones and sorted.
The team pair is part of the key, so the same batter in another game on the same day does not
match. Doubleheaders between the same two teams on one day are not separated by this key.
"""

from __future__ import annotations

import re
from typing import NamedTuple

PLAY_PATTERN = re.compile(
    r"경기일: (?P<date>[^,]+), 경기: (?P<teams>.*?), 이닝: (?P<inning>[^,]+), "
    r"투수: .*?, 타자: (?P<batter>[^,]+), 내용: (?P<event>.*)$"
)
#: Chunk text is cut at this many characters of event, so compare only the leading part.
EVENT_PREFIX = 120
#: Team codes that changed over time, mapped to the code in use now.
CODE_ALIAS = {"OB": "DB", "HT": "KIA", "SK": "SSG", "WO": "KH"}


class PlayIdentity(NamedTuple):
    """A play that can be recognised across team-code generations."""

    date: str
    teams: tuple[str, str]
    inning: str
    batter: str
    event: str


def _canonical_team(code: str) -> str:
    return CODE_ALIAS.get(code, code)


def play_identity(content: str) -> PlayIdentity | None:
    """Return the play identity of a PBP chunk text, or ``None`` when the text does not parse."""
    match = PLAY_PATTERN.search(content or "")
    if match is None:
        return None
    away_home = [part.strip() for part in match["teams"].split(" vs ")]
    if len(away_home) != 2:  # noqa: PLR2004 - away and home
        return None
    teams = tuple(sorted(_canonical_team(team) for team in away_home))
    return PlayIdentity(
        date=match["date"].strip(),
        teams=(teams[0], teams[1]),
        inning=match["inning"].strip(),
        batter=match["batter"].strip(),
        event=match["event"].strip()[:EVENT_PREFIX],
    )
