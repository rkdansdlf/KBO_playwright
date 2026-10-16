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

from sqlalchemy import text

if TYPE_CHECKING:
    from collections.abc import Iterable

TEAMS_PATTERN = re.compile(r"경기: (?P<teams>.*?), 이닝: ")
PLAY_PATTERN = re.compile(
    r"경기일: (?P<date>[^,]+), 경기: .*?, 이닝: (?P<inning>[^,]+), "
    r"투수: .*?, 타자: (?P<batter>[^,]+), 내용: (?P<event>.*)$"
)
#: Candidate content is cut at 300 characters, so compare only the leading part of the event.
EVENT_PREFIX = 120
SAMPLE_CAP = 10

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


def teams_of(content: str) -> str:
    """Return the ``away vs home`` field, or an empty string when absent."""
    match = TEAMS_PATTERN.search(content or "")
    return match["teams"].strip() if match else ""


def classify(candidates: Iterable[dict[str, str]], dense_rows: Iterable[tuple[str, str]]) -> dict[str, object]:
    """Split candidates into same-play-under-another-key, new, and unparsed.

    ``dense_rows`` yields ``(source_row_id, content)``. A candidate is ``matched`` when its
    play key appears in the dense store under any identity, which is the team-code case.
    """
    index: dict[tuple[str, str, str, str], list[tuple[str, str]]] = defaultdict(list)
    for source_row_id, content in dense_rows:
        key = play_key(content)
        if key is not None:
            index[key].append((source_row_id, teams_of(content)))

    counts = {"candidates": 0, "unparsed": 0, "matched": 0, "new": 0, "ambiguous": 0}
    samples: list[dict[str, object]] = []
    team_pairs: Counter[tuple[str, str]] = Counter()
    for candidate in candidates:
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
        if len(hits) > 1:
            counts["ambiguous"] += 1
        candidate_teams = teams_of(str(candidate.get("content") or ""))
        team_pairs[(candidate_teams, hits[0][1])] += 1
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
    return {**counts, "team_pairs": pairs, "samples": samples}


def main(argv: list[str] | None = None) -> int:
    """Read candidates from stdin, query the vector store read-only, print the summary."""
    argparse.ArgumentParser(description=__doc__).parse_args(argv)

    from src.db.vector_engine import get_vector_session

    candidates = [json.loads(line) for line in sys.stdin if line.strip()]
    with get_vector_session() as session:
        dense_rows = [(row[0], row[1]) for row in session.execute(_DENSE_SQL).all()]
    sys.stdout.write(json.dumps(classify(candidates, dense_rows), ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
