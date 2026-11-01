"""Report how legacy ``id=N`` chunks pair with their source rows (read-only).

The identity census pairs a legacy key with a source row by looking the number up
in a map keyed by the source's ``id``. Measured 2026-11-02, that pairing is wrong
— ``awards:id=130`` describes the 2021 Golden Glove catcher while
``awards.id=130`` is the 1987 first baseman — so its ``SAFE_REKEY`` verdict
cannot be applied.

This tool pairs by the chunk *text* instead, which carries every field the source
row keys on, and reports what each pairing means:

``REKEY``
    Matched, and the natural key is free — the identity can move onto it.
``DUPLICATE``
    Matched, and the natural key already exists. Re-keying would merge two
    identities, so it is refused and reported instead.
``UNMATCHED`` / ``AMBIGUOUS`` / ``PARSE_FAILED``
    No usable pairing. ``PARSE_FAILED`` means the text did not carry the fields,
    which is a different claim from "the row is gone".

Nothing is written. ``apply_rag_rekey`` is the writer, and it must not be pointed
at this population until the counts here have been reproduced and reviewed.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from typing import TYPE_CHECKING

from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import get_rag_index_session
from src.services.rag_index_identity import (
    stable_award_source_row_id,
    stable_player_movement_source_row_id,
    stable_team_history_source_row_id,
)
from src.services.rag_legacy_content_match import (
    DUPLICATE,
    MATCHED,
    LegacySourceRow,
    disposition,
    match_award,
    match_movement,
    match_team_history,
    normalize_source_team_name,
)
from src.utils.team_codes import TEAM_NAME_TO_CODE

EXIT_OK = 0
EXIT_STORE_UNREACHABLE = 2

SOURCES = ("player_movements", "awards", "team_history")
#: How many per-source examples the text output shows. The JSON report carries
#: the counts; the samples exist to make a number falsifiable at a glance.
SAMPLE_LIMIT = 5

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from src.services.rag_legacy_content_match import LegacyMatchOutcome


def _movement_index(session: Session) -> dict:
    """Index movement rows by ``(date, player, section)``, with their team code."""
    from sqlalchemy import select

    from src.models.player import PlayerMovement

    index: dict[tuple[str, str, str], list[LegacySourceRow]] = {}
    statement = select(
        PlayerMovement.id,
        PlayerMovement.movement_date,
        PlayerMovement.team_code,
        PlayerMovement.player_name,
        PlayerMovement.section,
    )
    for row in session.execute(statement).yield_per(1_000):
        identity = stable_player_movement_source_row_id(row[1], row[2], row[3], row[4])
        index.setdefault((str(row[1]), row[3], row[4]), []).append(
            LegacySourceRow(int(row[0]), identity, row[2]),
        )
    return index


def _award_index(session: Session) -> dict:
    """Index award rows by ``(year, award_type, player_name)``."""
    from sqlalchemy import select

    from src.models.award import Award

    index: dict[tuple[int, str, str], list[LegacySourceRow]] = {}
    statement = select(Award.id, Award.year, Award.award_type, Award.category, Award.player_name)
    for row in session.execute(statement).yield_per(1_000):
        identity = stable_award_source_row_id(row[1], row[2], row[3], row[4])
        index.setdefault((int(row[1]), row[2], row[4]), []).append(LegacySourceRow(int(row[0]), identity))
    return index


def _team_history_index(session: Session) -> dict:
    r"""Index team-history rows by ``(season, short team name)``.

    The source column stores names with an embedded newline
    (``삼미\\n슈퍼스타즈``) while the chunk says ``삼미``, so both sides are
    normalised through the same helper.
    """
    from sqlalchemy import select

    from src.models.team_history import TeamHistory

    index: dict[tuple[int, str], list[LegacySourceRow]] = {}
    statement = select(TeamHistory.id, TeamHistory.season, TeamHistory.team_code, TeamHistory.team_name)
    for row in session.execute(statement).yield_per(1_000):
        identity = stable_team_history_source_row_id(row[1], row[2])
        key = (int(row[1]), normalize_source_team_name(row[3]))
        index.setdefault(key, []).append(LegacySourceRow(int(row[0]), identity))
    return index


def _legacy_chunks(session: Session, source_table: str) -> list[tuple[int, str, str]]:
    """Return ``(chunk_id, source_row_id, content)`` for one source's legacy keys."""
    from sqlalchemy import select

    from src.models.rag_chunk import RagChunk

    statement = select(RagChunk.id, RagChunk.source_row_id, RagChunk.content).where(
        RagChunk.source_table == source_table,
        RagChunk.index_status == "ACTIVE",
        RagChunk.source_row_id.regexp_match(r"^id=[0-9]+$"),
    )
    return [(int(row[0]), row[1], row[2]) for row in session.execute(statement).yield_per(1_000)]


def _existing_natural_keys(session: Session, source_table: str) -> frozenset[str]:
    """Return the natural (non-legacy) keys already present for one source."""
    from sqlalchemy import select

    from src.models.rag_chunk import RagChunk

    statement = select(RagChunk.source_row_id).where(
        RagChunk.source_table == source_table,
        ~RagChunk.source_row_id.regexp_match(r"^id=[0-9]+$"),
    )
    return frozenset(str(row[0]) for row in session.execute(statement).yield_per(1_000))


def _match_one(source_table: str, content: str, indexes: dict) -> LegacyMatchOutcome:
    if source_table == "player_movements":
        return match_movement(content, indexes["player_movements"], TEAM_NAME_TO_CODE)
    if source_table == "awards":
        return match_award(content, indexes["awards"])
    return match_team_history(content, indexes["team_history"])


def build_report(session: Session, sources: tuple[str, ...]) -> dict:
    """Match every legacy chunk and summarise what each pairing means."""
    indexes = {
        "player_movements": _movement_index(session),
        "awards": _award_index(session),
        "team_history": _team_history_index(session),
    }
    per_source: list[dict] = []
    total = Counter()
    for source_table in sources:
        existing = _existing_natural_keys(session, source_table)
        chunks = _legacy_chunks(session, source_table)
        by_reason: Counter[str] = Counter()
        actionable: Counter[str] = Counter()
        samples: list[str] = []
        for _chunk_id, legacy_key, content in chunks:
            outcome = _match_one(source_table, content, indexes)
            by_reason[outcome.reason] += 1
            if outcome.reason == MATCHED:
                verdict = disposition(outcome.natural_source_row_id, existing)
                actionable[verdict] += 1
                if verdict == DUPLICATE and len(samples) < SAMPLE_LIMIT:
                    samples.append(f"{legacy_key} -> {outcome.natural_source_row_id} ({DUPLICATE})")
            elif len(samples) < SAMPLE_LIMIT:
                samples.append(f"{legacy_key}: {outcome.reason} {list(outcome.candidates)[:3]}")
        entry = {
            "source_table": source_table,
            "legacy_chunks": len(chunks),
            "by_reason": dict(by_reason),
            "by_disposition": dict(actionable),
            "existing_natural_keys": len(existing),
            "samples": samples,
        }
        per_source.append(entry)
        total.update(by_reason)
    return {"sources": per_source, "totals": dict(total), "read_only": True}


def main(argv: list[str] | None = None) -> int:
    """Report legacy-to-source pairings without modifying either side."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--source", action="append", choices=SOURCES, help="Limit to one source table")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    args = parser.parse_args(argv)

    try:
        with get_rag_index_session() as session:
            report = build_report(session, tuple(args.source or SOURCES))
    except SQLAlchemyError as exc:
        # Named so the exit code means one thing: the store could not be read, so
        # nothing was compared. A parse or matching bug must not land here and be
        # reported as an outage.
        sys.stdout.write(f"legacy content match unavailable: {type(exc).__name__}: {exc}\n")
        return EXIT_STORE_UNREACHABLE

    if args.as_json:
        sys.stdout.write(json.dumps(report, ensure_ascii=False) + "\n")
        return EXIT_OK

    for entry in report["sources"]:
        reasons = " ".join(f"{k}={v}" for k, v in sorted(entry["by_reason"].items()))
        verdicts = " ".join(f"{k}={v}" for k, v in sorted(entry["by_disposition"].items()))
        sys.stdout.write(f"{entry['source_table']}: legacy={entry['legacy_chunks']} {reasons}\n")
        sys.stdout.write(f"   actionable: {verdicts or '(none)'}\n")
        for sample in entry["samples"]:
            sys.stdout.write(f"   {sample}\n")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
