"""Read-only check whether PBP candidates are already embedded under another team code.

Reads candidate JSON Lines from stdin (the output of ``--list-candidates``) and compares
each candidate with the ACTIVE PBP rows of the vector store. The team field is ignored,
so ``OB vs HT`` and ``DB vs KIA`` for the same play meet on ``(date, inning, batter, event)``.

Writes nothing and calls no embedding provider. Prints one JSON summary to stdout.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from typing import TYPE_CHECKING

from sqlalchemy import bindparam, text

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlalchemy.orm import Session

TEAMS_PATTERN = re.compile(r"경기: (?P<teams>.*?), 이닝: ")
PLAY_PATTERN = re.compile(
    r"경기일: (?P<date>[^,]+), 경기: .*?, 이닝: (?P<inning>[^,]+), "
    r"투수: .*?, 타자: (?P<batter>[^,]+), 내용: (?P<event>.*)$"
)
#: Candidate content is cut at 300 characters, so compare only the leading part of the event.
EVENT_PREFIX = 120
SAMPLE_CAP = 10

_LEGACY_SQL = text(
    "SELECT id, game_id, inning, inning_half, batter_name FROM game_play_by_play WHERE id IN :ids"
).bindparams(bindparam("ids", expanding=True))
_DENSE_SQL = text(
    "SELECT source_row_id, content FROM rag_chunks "
    "WHERE source_table = 'game_play_by_play' AND index_status = 'ACTIVE' "
    "AND content LIKE '경기일: %'"
)


def play_key(content: str) -> tuple[str, str, str, str] | None:
    """Return ``(date, inning, batter, event prefix)`` or ``None`` when the text does not parse."""
    match = PLAY_PATTERN.search(content or "")
    if match is None:
        return None
    return (
        match["date"].strip(),
        match["inning"].strip(),
        match["batter"].strip(),
        match["event"].strip()[:EVENT_PREFIX],
    )


#: Team codes that appear in game ids, and the current code each old one became.
KNOWN_CODES = frozenset({"DB", "OB", "KIA", "HT", "SSG", "SK", "LG", "LT", "NC", "HH", "SS", "KT", "KH", "WO"})
CODE_ALIAS = {"OB": "DB", "HT": "KIA", "SK": "SSG", "WO": "KH"}
GAME_ID_PATTERN = re.compile(r"(\d{8})([A-Z]+)(\d)")


def game_key(game_id: str) -> tuple[str, tuple[str, str], str] | None:
    """Return ``(date, sorted canonical teams, doubleheader index)`` for a new-style game id.

    The team codes are split by trying known codes, then mapped through ``CODE_ALIAS`` so the
    old and new codes of one team meet. Legacy numeric ids return ``None``.
    """
    match = GAME_ID_PATTERN.fullmatch(game_id)
    if match is None:
        return None
    date, codes, index = match.groups()
    for cut in range(2, len(codes) - 1):
        away, home = codes[:cut], codes[cut:]
        if away in KNOWN_CODES and home in KNOWN_CODES:
            teams = tuple(sorted({CODE_ALIAS.get(away, away), CODE_ALIAS.get(home, home)}))
            return (date, (teams[0], teams[-1]), index)
    return None


def game_key_of_row(source_row_id: str) -> tuple[str, tuple[str, str], str] | None:
    """Return the game key carried by a row's identity, or ``None`` for legacy numeric keys."""
    return game_key(source_row_id.split("_", 1)[0])


def verdict_from_keys(
    candidate_key: tuple[str, tuple[str, str], str] | None, stored_keys: list[tuple[str, tuple[str, str], str] | None]
) -> str:
    """Judge a match from game keys: confirmed, conflicting, or unverified."""
    if candidate_key is None:
        return "unverified"
    if candidate_key in stored_keys:
        return "confirmed"
    if any(key is not None for key in stored_keys):
        return "conflicting"
    return "unverified"


def game_verdict(candidate_row_id: str, dense_row_ids: list[str]) -> str:
    """Judge whether a matched play belongs to the same game as the stored one.

    ``confirmed``: a stored row carries the candidate's game key. ``conflicting``: stored rows
    carry game keys, and none of them is the candidate's game. ``unverified``: nothing comparable
    (for example a legacy numeric key, which has no game id), so a doubleheader cannot be ruled out.
    """
    return verdict_from_keys(
        game_key_of_row(candidate_row_id),
        [game_key_of_row(row_id) for row_id in dense_row_ids],
    )


def teams_of(content: str) -> str:
    """Return the ``away vs home`` field, or an empty string when absent."""
    match = TEAMS_PATTERN.search(content or "")
    return match["teams"].strip() if match else ""


#: One source PBP row for a legacy numeric key: (game_id, inning, inning_half, batter_name).
SourcePlay = tuple[str | None, int | None, str | None, str | None]


def legacy_play_ids(dense_rows: Iterable[tuple[str, str]]) -> list[int]:
    """Return the integer keys of legacy rows, which may be source ``game_play_by_play.id`` values."""
    return sorted({int(row_id) for row_id, _ in dense_rows if row_id.isdigit()})


#: Source ``inning_half`` spellings. The stored text appends the raw value (``1회top``), so the
#: source row's value and the text suffix must agree; both Korean and English spellings are accepted.
HALF_NAMES = {"초": "top", "top": "top", "말": "bottom", "bottom": "bottom"}


def mapping_agrees(source: SourcePlay, key: tuple[str, str, str, str]) -> bool:
    """Check that a source row describes the same inning and batter as the stored text.

    This tests the hypothesis that a legacy numeric key is the source row id. Agreement on the
    inning, half, and batter across the stored rows is the evidence for that mapping.
    """
    _, inning, half, batter = source
    normalized_half = HALF_NAMES.get(str(half or "").strip().lower())
    if inning is None or normalized_half is None:
        return False
    return f"{inning}회{normalized_half}" == key[1] and (batter or "").strip() == key[2]


def _stored_game_key(row_id: str, lookup: dict[str, SourcePlay]) -> tuple[str, tuple[str, str], str] | None:
    """Return the game key of a stored row: from its source row for a legacy key, else its own id."""
    if row_id in lookup and row_id.isdigit():
        return game_key(lookup[row_id][0] or "")
    return game_key_of_row(row_id)


def _index_dense(
    dense_rows: Iterable[tuple[str, str]],
    lookup: dict[str, SourcePlay],
) -> tuple[
    dict[tuple[str, str, str, str], list[tuple[str, str]]], dict[str, tuple[str, str, str, str]], dict[str, int]
]:
    """Index stored PBP by play key and check the legacy-key mapping against the source rows."""
    index: dict[tuple[str, str, str, str], list[tuple[str, str]]] = defaultdict(list)
    dense_keys: dict[str, tuple[str, str, str, str]] = {}
    for source_row_id, content in dense_rows:
        key = play_key(content)
        if key is not None:
            index[key].append((source_row_id, teams_of(content)))
            dense_keys[source_row_id] = key
    diagnostics = {"legacy_found_in_source": 0, "legacy_mapping_agrees": 0, "legacy_mapping_checked": 0}
    for row_id, key in dense_keys.items():
        if row_id in lookup and row_id.isdigit():
            diagnostics["legacy_found_in_source"] += 1
            diagnostics["legacy_mapping_checked"] += 1
            diagnostics["legacy_mapping_agrees"] += int(mapping_agrees(lookup[row_id], key))
    return index, dense_keys, diagnostics


def classify(
    candidates: Iterable[dict[str, str]],
    dense_rows: Iterable[tuple[str, str]],
    legacy_lookup: dict[str, SourcePlay] | None = None,
) -> dict[str, object]:
    """Split candidates into same-play-under-another-key, new, and unparsed.

    ``dense_rows`` yields ``(source_row_id, content)``. A candidate is ``matched`` when its
    play key appears in the dense store under any identity, which is the team-code case.
    """
    lookup = legacy_lookup or {}
    index, _, diagnostics = _index_dense(dense_rows, lookup)
    candidate_list = list(candidates)
    candidate_keys = Counter(play_key(str(c.get("content") or "")) for c in candidate_list)
    internal_duplicates = sum(
        1 for c in candidate_list if (k := play_key(str(c.get("content") or ""))) is not None and candidate_keys[k] > 1
    )

    counts = {"candidates": 0, "unparsed": 0, "matched": 0, "new": 0, "ambiguous": 0}
    verdicts: dict[str, list[str]] = {"confirmed": [], "conflicting": [], "unverified": []}
    samples: list[dict[str, object]] = []
    team_pairs: Counter[tuple[str, str]] = Counter()
    matched_dates: set[str] = set()
    for candidate in candidate_list:
        counts["candidates"] += 1
        key = play_key(str(candidate.get("content") or ""))
        if key is None:
            counts["unparsed"] += 1
            continue
        hits = index.get(key)
        if not hits:
            counts["new"] += 1
            continue
        counts["matched"] += 1
        matched_dates.add(key[0])
        if len(hits) > 1:
            counts["ambiguous"] += 1
        candidate_teams = teams_of(str(candidate.get("content") or ""))
        team_pairs[(candidate_teams, hits[0][1])] += 1
        row_id = str(candidate.get("source_row_id") or "")
        verdict = verdict_from_keys(
            game_key_of_row(row_id),
            [_stored_game_key(hit_id, lookup) for hit_id, _ in hits],
        )
        verdicts[verdict].append(row_id)
        if len(samples) < SAMPLE_CAP:
            samples.append(
                {
                    "candidate_source_row_id": candidate.get("source_row_id"),
                    "dense_source_row_ids": [row_id for row_id, _ in hits[:3]],
                    "play": list(key),
                }
            )
    pairs = [
        {"candidate_teams": cand, "dense_teams": dense, "count": count}
        for (cand, dense), count in team_pairs.most_common()
    ]
    game_identity = {name: len(ids) for name, ids in verdicts.items()}
    return {
        **counts,
        "game_identity": game_identity,
        "legacy_mapping": diagnostics,
        "team_pairs": pairs,
        "samples": samples,
        "source_row_ids": verdicts,
        "matched_dates": sorted(matched_dates),
        "candidate_internal_duplicates": internal_duplicates,
    }


def main(argv: list[str] | None = None) -> int:
    """Read candidates from stdin, query the vector store read-only, print the summary."""
    argparse.ArgumentParser(description=__doc__).parse_args(argv)

    from src.db.engine import get_rag_source_session
    from src.db.vector_engine import get_vector_session

    candidates = [json.loads(line) for line in sys.stdin if line.strip()]
    with get_vector_session() as session:
        dense_rows = [(row[0], row[1]) for row in session.execute(_DENSE_SQL).all()]
    with get_rag_source_session() as source_session:
        legacy_lookup = load_legacy_lookup(source_session, legacy_play_ids(dense_rows))
    result = classify(candidates, dense_rows, legacy_lookup)
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    return 0


def load_legacy_lookup(source_session: Session, play_ids: list[int]) -> dict[str, SourcePlay]:
    """Read ``game_play_by_play`` rows for the legacy keys, in batches of 1,000 (read-only)."""
    lookup: dict[str, SourcePlay] = {}
    for start in range(0, len(play_ids), 1000):
        batch = play_ids[start : start + 1000]
        rows = source_session.execute(_LEGACY_SQL, {"ids": batch}).all()
        for row_id, game_id, inning, half, batter in rows:
            lookup[str(row_id)] = (game_id, inning, half, batter)
    return lookup


if __name__ == "__main__":
    sys.exit(main())
