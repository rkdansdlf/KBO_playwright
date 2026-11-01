r"""Pair legacy ``id=N`` RAG chunks with their source rows by reading the chunk text.

Why this exists: the ``id=N`` keys in ``rag_chunks`` are **not** source primary
keys. Measured 2026-11-02, ``player_movements:id=3901`` describes 구자욱
2022-05-04 while that table's ``id=3901`` is 권혁경 2022-07-21, and
``awards:id=130`` describes the 2021 Golden Glove catcher while
``awards.id=130`` is the 1987 first baseman. The identity census matched them
anyway, because it looked the number up in a map keyed by the source's ``id`` —
so its ``SAFE_REKEY`` verdict cannot be applied and its ``existing natural
target`` count is not evidence of a duplicate.

The chunk text, however, carries every field the source row keys on. Parsed over
the whole population (4,503 movements / 486 awards / 385 team histories) the
parse failures were **zero**; the only misses were naming differences, which this
module resolves:

* ``player_movements`` — a franchise appears under both its era code (``HT``
  해태) and its current one (``KIA`` 기아), so 643 rows matched twice. The chunk
  names the team (``팀: 해태 타이거즈``), which splits every pair.
* ``team_history`` — the chunk names the short form (``삼미``) while the source
  column holds ``삼미\\n슈퍼스타즈``.
* ``awards`` — the legacy text says ``Golden Glove`` where the column says
  ``골든글러브``. Three aliases cover every legacy award chunk; the table's other
  three kinds (``수비상``, ``올스타전MVP``, ``한국시리즈MVP``) have no legacy
  chunks at all.

``player_season_batting``/``pitching`` are deliberately **not** here. Their key
ends in ``_{league}`` and the legacy text does not carry the league, so the row
is narrowed rather than identified — a different question with a different
answer.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

#: ``matched`` — exactly one source row. ``ambiguous`` — two or more, and the
#: team name did not split them. ``unmatched`` — none. ``parse_failed`` — the
#: text did not carry the fields, which is a different claim from "the row is
#: gone" and must not be reported as one.
MATCHED = "matched"
AMBIGUOUS = "ambiguous"
UNMATCHED = "unmatched"
PARSE_FAILED = "parse_failed"

#: What a matched chunk means for the store, once its natural key is known.
#: ``REKEY`` — the natural key is free, so the identity can move onto it.
#: ``DUPLICATE`` — the natural key already exists, so this chunk is a second
#: copy of a row that is indexed; re-keying it would merge two identities into
#: one and is refused rather than resolved.
REKEY = "REKEY"
DUPLICATE = "DUPLICATE"


def disposition(natural_source_row_id: str | None, existing_keys: frozenset[str]) -> str:
    """Say what a matched chunk means for the store.

    A missing natural key is not ``UNMATCHED``: the chunk was paired with its
    source row, and the row simply has no index entry yet. Keeping the two
    apart matters because only one of them is actionable.
    """
    if natural_source_row_id is None:
        return UNMATCHED
    return DUPLICATE if natural_source_row_id in existing_keys else REKEY


#: ``팀`` is **not** required: one live chunk (``id=4549``, 서준원 2023-03-30) was
#: written without it. Requiring it turned that chunk into a parse failure, which
#: reads as "the text is unusable" rather than "the team is unknown" — and the
#: team is only needed to split era/current twins, so a chunk without it still
#: matches whenever the rest is unique.
MOVEMENT_FIELDS = {
    "date": re.compile(r"날짜:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})"),
    "section": re.compile(r"이동 유형:\s*([^\n]+)"),
    "player": re.compile(r"선수:\s*([^\n]+)"),
}
MOVEMENT_TEAM_FIELD = re.compile(r"팀:\s*([^\n]+)")
AWARD_FIELDS = {
    "year": re.compile(r"시즌:\s*([0-9]{4})"),
    "award_type": re.compile(r"수상 종류:\s*([^\n]+)"),
    "player": re.compile(r"선수:\s*([^\n]+)"),
}
TEAM_HISTORY_FIELDS = {
    "team": re.compile(r"구단:\s*([^\n]+)"),
    "season": re.compile(r"시즌:\s*([0-9]{4})"),
}

#: The legacy award generator wrote English names; the column holds Korean ones.
AWARD_TYPE_ALIASES: Mapping[str, str] = {
    "Golden Glove": "골든글러브",
    "Rookie of the Year": "신인상",
    "MVP": "MVP",
}


@dataclass(frozen=True, slots=True)
class LegacySourceRow:
    """One candidate source row, as the matcher needs to see it."""

    source_row_id: int
    identity: str
    #: Only ``player_movements`` needs this: it is what splits the era/current
    #: code twins (``HT``/``KIA``) apart.
    team_code: str | None = None
    #: Only ``awards`` needs this: the legacy text shortens foreign names
    #: (``로하스`` for ``멜 로하스 주니어``, ``린드블럼`` for ``조쉬 린드블럼``), and the
    #: full name is what a substring fallback searches.
    player_name: str | None = None


@dataclass(frozen=True, slots=True)
class LegacyMatchOutcome:
    """What one legacy chunk's text says about where it came from."""

    reason: str
    natural_source_row_id: str | None = None
    source_row_id: int | None = None
    candidates: tuple[int, ...] = ()


def parse_fields(content: str, patterns: Mapping[str, re.Pattern[str]]) -> dict[str, str] | None:
    """Return every named field, or None when any of them is missing.

    None rather than a partial dict: a chunk missing one field cannot be paired,
    and returning the rest would invite a caller to match on a subset.
    """
    parsed: dict[str, str] = {}
    for name, pattern in patterns.items():
        match = pattern.search(content)
        if match is None:
            return None
        parsed[name] = match.group(1).strip()
    return parsed


def normalize_source_team_name(name: str | None) -> str:
    r"""Collapse a source team name to the short form the chunks use.

    ``team_history.team_name`` stores names with an embedded newline —
    ``삼미\\n슈퍼스타즈``, ``해태\\n타이거즈`` — while the legacy text says ``삼미``.
    The first whitespace-delimited token is the common form.
    """
    parts = (name or "").split()
    return parts[0] if parts else ""


def normalize_award_type(raw: str) -> str:
    """Map a legacy English award name onto the column's Korean one."""
    return AWARD_TYPE_ALIASES.get(raw.strip(), raw.strip())


def _unique_or_ambiguous(candidates: Sequence[LegacySourceRow]) -> LegacyMatchOutcome:
    """Return the single candidate, or an ambiguity that names all of them."""
    if len(candidates) == 1:
        row = candidates[0]
        return LegacyMatchOutcome(MATCHED, row.identity, row.source_row_id)
    return LegacyMatchOutcome(AMBIGUOUS, candidates=tuple(row.source_row_id for row in candidates))


def match_movement(
    content: str,
    by_key: Mapping[tuple[str, str, str], Sequence[LegacySourceRow]],
    team_name_to_code: Mapping[str, str],
) -> LegacyMatchOutcome:
    """Pair a legacy movement chunk with its row, using the team name to split twins."""
    fields = parse_fields(content, MOVEMENT_FIELDS)
    if fields is None:
        return LegacyMatchOutcome(PARSE_FAILED)
    candidates = by_key.get((fields["date"], fields["player"], fields["section"]), ())
    if not candidates:
        return LegacyMatchOutcome(UNMATCHED)
    if len(candidates) == 1:
        return _unique_or_ambiguous(candidates)
    team_match = MOVEMENT_TEAM_FIELD.search(content)
    code = team_name_to_code.get(team_match.group(1).strip()) if team_match else None
    narrowed = [row for row in candidates if row.team_code == code]
    if len(narrowed) == 1:
        return _unique_or_ambiguous(narrowed)
    return LegacyMatchOutcome(AMBIGUOUS, candidates=tuple(row.source_row_id for row in candidates))


def match_award(
    content: str,
    by_key: Mapping[tuple[int, str, str], Sequence[LegacySourceRow]],
    by_year_type: Mapping[tuple[int, str], Sequence[LegacySourceRow]] | None = None,
) -> LegacyMatchOutcome:
    """Pair a legacy award chunk with its row, translating the award name first.

    When the exact name finds nothing and ``by_year_type`` is supplied, the search
    falls back to rows of the same year and award whose full name *contains* the
    legacy one. The legacy generator shortened foreign names — ``로하스`` for
    ``멜 로하스 주니어``, ``린드블럼`` for ``조쉬 린드블럼`` — and those are the bulk of
    the 83 chunks the exact match missed. The fallback is only accepted when
    exactly one row contains the name, so a short name that fits two players
    stays ambiguous rather than picking one.
    """
    fields = parse_fields(content, AWARD_FIELDS)
    if fields is None:
        return LegacyMatchOutcome(PARSE_FAILED)
    year = int(fields["year"])
    award_type = normalize_award_type(fields["award_type"])
    player = fields["player"]
    candidates = by_key.get((year, award_type, player), ())
    if candidates:
        return _unique_or_ambiguous(candidates)
    if by_year_type is None:
        return LegacyMatchOutcome(UNMATCHED)
    same_award = by_year_type.get((year, award_type), ())
    contained = [row for row in same_award if row.player_name and player in row.player_name]
    if not contained:
        return LegacyMatchOutcome(UNMATCHED)
    return _unique_or_ambiguous(contained)


def match_team_history(
    content: str,
    by_key: Mapping[tuple[int, str], Sequence[LegacySourceRow]],
) -> LegacyMatchOutcome:
    """Pair a legacy team-history chunk with its row by ``(season, short name)``."""
    fields = parse_fields(content, TEAM_HISTORY_FIELDS)
    if fields is None:
        return LegacyMatchOutcome(PARSE_FAILED)
    key = (int(fields["season"]), normalize_source_team_name(fields["team"]))
    candidates = by_key.get(key, ())
    if not candidates:
        return LegacyMatchOutcome(UNMATCHED)
    if len(candidates) == 1:
        row = candidates[0]
        return LegacyMatchOutcome(MATCHED, row.identity, row.source_row_id)
    return LegacyMatchOutcome(AMBIGUOUS, candidates=tuple(row.source_row_id for row in candidates))
