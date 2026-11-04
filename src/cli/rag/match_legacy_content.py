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
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path
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
    REKEY,
    LegacyMatchOutcome,
    LegacySourceRow,
    disposition,
    match_award,
    match_movement,
    match_team_history,
    normalize_source_team_name,
)
from src.services.rag_rekey_manifest import build_manifest
from src.utils.team_codes import TEAM_NAME_TO_CODE

EXIT_OK = 0
EXIT_STORE_UNREACHABLE = 2

SOURCES = ("player_movements", "awards", "team_history")
#: The applier's vocabulary. ``REKEY`` becomes ``SAFE_REKEY`` (the natural key is
#: free) and ``DUPLICATE`` becomes ``TARGET_EXISTS_CONTENT_MISMATCH`` (the target
#: exists and its text differs from this chunk's, so merging would lose one of
#: the two). Applying a manifest whose entries do not match one of these five
#: fails, so the translation is explicit here rather than in the applier.
APPLIER_DISPOSITIONS = {
    REKEY: "SAFE_REKEY",
    DUPLICATE: "TARGET_EXISTS_CONTENT_MISMATCH",
}

#: How many per-source examples the text output shows. The JSON report carries
#: the counts; the samples exist to make a number falsifiable at a glance.
SAMPLE_LIMIT = 5

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from src.services.rag_legacy_content_match import LegacyMatchOutcome


def _movement_index(session: Session) -> dict:
    """Index movement rows by ``(date, player, section)``, with their team code and snapshot."""
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
        snap = hashlib.sha256(repr((row[0], str(row[1]), row[2], row[3], row[4])).encode("utf-8")).hexdigest()
        index.setdefault((str(row[1]), row[3], row[4]), []).append(
            LegacySourceRow(
                int(row[0]),
                identity,
                team_code=row[2],
                player_name=row[3],
                source_snapshot_hash=snap,
            ),
        )
    return index


def _award_index(session: Session) -> tuple[dict, dict]:
    """Return ``(exact, by_year_type)`` with full metadata and snapshot."""
    from sqlalchemy import select

    from src.models.award import Award

    exact: dict[tuple[int, str, str], list[LegacySourceRow]] = {}
    by_year_type: dict[tuple[int, str], list[LegacySourceRow]] = {}
    statement = select(
        Award.id,
        Award.year,
        Award.award_type,
        Award.category,
        Award.player_name,
        Award.team_name,
        Award.team_code,
    )
    for row in session.execute(statement).yield_per(1_000):
        identity = stable_award_source_row_id(row[1], row[2], row[3], row[4])
        snap = hashlib.sha256(
            repr((row[0], row[1], row[2], row[3], row[4], row[5], row[6])).encode("utf-8")
        ).hexdigest()
        item = LegacySourceRow(
            int(row[0]),
            identity,
            player_name=row[4],
            category=row[3],
            team_name=row[5],
            team_code=row[6],
            season=int(row[1]),
            source_snapshot_hash=snap,
        )
        exact.setdefault((int(row[1]), row[2], row[4]), []).append(item)
        by_year_type.setdefault((int(row[1]), row[2]), []).append(item)
    return exact, by_year_type


def _team_history_index(session: Session) -> dict:
    r"""Index team-history rows by ``(season, short team name)`` with ranking and city."""
    from sqlalchemy import select

    from src.models.team_history import TeamHistory

    index: dict[tuple[int, str], list[LegacySourceRow]] = {}
    statement = select(
        TeamHistory.id,
        TeamHistory.season,
        TeamHistory.team_code,
        TeamHistory.team_name,
        TeamHistory.ranking,
        TeamHistory.city,
    )
    for row in session.execute(statement).yield_per(1_000):
        identity = stable_team_history_source_row_id(row[1], row[2])
        snap = hashlib.sha256(repr((row[0], row[1], row[2], row[3], row[4], row[5])).encode("utf-8")).hexdigest()
        key = (int(row[1]), normalize_source_team_name(row[3]))
        index.setdefault(key, []).append(
            LegacySourceRow(
                int(row[0]),
                identity,
                team_code=row[2],
                team_name=row[3],
                season=int(row[1]),
                ranking=int(row[4]) if row[4] is not None else None,
                city=str(row[5]) if row[5] is not None else None,
                source_snapshot_hash=snap,
            )
        )
    return index


def _legacy_chunks(session: Session, source_table: str) -> list[dict]:
    """Return legacy chunk rows with the CAS fields the applier verifies.

    The applier compares ``legacy_content_hash``, ``index_status`` and
    ``index_version`` against the live row before applying, so all three must be
    read from the DB rather than omitted — an omitted field makes the CAS check
    a no-op, and a skipped check is how a stale apply slips through.
    """
    from sqlalchemy import select

    from src.models.rag_chunk import RagChunk

    statement = select(
        RagChunk.id,
        RagChunk.source_row_id,
        RagChunk.content,
        RagChunk.content_hash,
        RagChunk.index_status,
        RagChunk.index_version,
    ).where(
        RagChunk.source_table == source_table,
        RagChunk.index_status == "ACTIVE",
        RagChunk.source_row_id.regexp_match(r"^id=[0-9]+$"),
    )
    return [
        {
            "chunk_id": int(row[0]),
            "legacy_key": row[1],
            "content": row[2],
            "content_hash": row[3],
            "index_status": row[4],
            "index_version": row[5],
        }
        for row in session.execute(statement).yield_per(1_000)
    ]


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
        exact, by_year_type = indexes["awards"]
        return match_award(content, exact, by_year_type)
    return match_team_history(content, indexes["team_history"])


def build_report(
    session: Session,
    sources: tuple[str, ...] = SOURCES,
    *,
    index_session: Session | None = None,
) -> dict:
    """Match every legacy chunk and summarise what each pairing means."""
    source_session = session
    idx_session = index_session or session
    indexes = {
        "player_movements": _movement_index(source_session),
        "awards": _award_index(source_session),
        "team_history": _team_history_index(source_session),
    }
    per_source: list[dict] = []
    entries: list[dict] = []
    total = Counter()
    for source_table in sources:
        existing = _existing_natural_keys(idx_session, source_table)
        chunks = _legacy_chunks(idx_session, source_table)
        by_reason: Counter[str] = Counter()
        actionable: Counter[str] = Counter()
        samples: list[str] = []
        pairings: list[tuple[int, str, LegacyMatchOutcome, str, dict]] = []
        for chunk in chunks:
            outcome = _match_one(source_table, chunk["content"], indexes)
            by_reason[outcome.reason] += 1
            if outcome.reason == MATCHED:
                verdict = disposition(outcome.natural_source_row_id, existing)
                actionable[verdict] += 1
                pairings.append((chunk["chunk_id"], chunk["legacy_key"], outcome, verdict, chunk))
                if verdict == DUPLICATE and len(samples) < SAMPLE_LIMIT:
                    samples.append(f"{chunk['legacy_key']} -> {outcome.natural_source_row_id} ({DUPLICATE})")
            elif len(samples) < SAMPLE_LIMIT:
                samples.append(f"{chunk['legacy_key']}: {outcome.reason} {list(outcome.candidates)[:3]}")
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
        entries.extend(
            {
                "chunk_id": chunk_id,
                "source_table": source_table,
                "legacy_source_row_id": legacy_key,
                "legacy_content_hash": cas_chunk["content_hash"],
                "index_status": cas_chunk["index_status"],
                "index_version": cas_chunk["index_version"],
                "matched_source_pk": outcome.source_row_id,
                "natural_source_row_id": outcome.natural_source_row_id,
                "match_reason": "IDENTITY_VERIFIED" if verdict == REKEY else verdict,
                "disposition": APPLIER_DISPOSITIONS[verdict],
                "match_schema_version": "content-proof-v1",
                "proof": {
                    "matched_fields": list(outcome.matched_fields),
                    "candidate_count": 1,
                    "source_snapshot_hash": outcome.source_snapshot_hash,
                    "chunk_content_hash": outcome.chunk_content_hash,
                },
            }
            for chunk_id, legacy_key, outcome, verdict, cas_chunk in pairings
        )
    return {
        "sources": per_source,
        "totals": dict(total),
        "entries": entries,
        "read_only": True,
    }


def main(argv: list[str] | None = None) -> int:
    """Report legacy-to-source pairings without modifying either side."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--source", action="append", choices=SOURCES, help="Limit to one source table")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the apply-gated rekey manifest to this path (entries the content pairing produced)",
    )
    args = parser.parse_args(argv)

    try:
        from src.db.engine import get_rag_source_session

        with get_rag_source_session() as source_session, get_rag_index_session() as index_session:
            report = build_report(source_session, tuple(args.source or SOURCES), index_session=index_session)
    except SQLAlchemyError as exc:
        # Named so the exit code means one thing: the store could not be read, so
        # nothing was compared. A parse or matching bug must not land here and be
        # reported as an outage.
        sys.stdout.write(f"legacy content match unavailable: {type(exc).__name__}: {exc}\n")
        return EXIT_STORE_UNREACHABLE

    if args.output is not None:
        entries = report["entries"]
        unsafe = sum(1 for entry in entries if entry["disposition"] != "SAFE_REKEY")
        manifest = build_manifest(
            manifest_version="legacy-content-match-v1",
            target_index_version="rag-v1",
            source_tables=tuple(args.source or SOURCES),
            totals=report["totals"],
            unsafe_entry_count=unsafe,
            sources=report["sources"],
            entries=entries,
        )
        args.output.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        sys.stdout.write(f"manifest written: {args.output} entries={len(entries)} unsafe={unsafe}\n")

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
