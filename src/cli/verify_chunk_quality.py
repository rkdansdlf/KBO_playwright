"""verify_chunk_quality.py.

CLI tool to measure and report the quality of RAG chunks stored in the local SQLite database.

Metrics reported:
  - Total chunk count
  - Average / min / max / p50 / p95 chunk length (in chars)
  - Empty chunk count
  - Duplicate chunk count (by source_row_id hash)
  - Keyword coverage rate (% of chunks that have keywords metadata)
  - Category distribution
  - Documents with < 3 chunks (under-chunked documents)

Usage:
    python -m src.cli.verify_chunk_quality
    python -m src.cli.verify_chunk_quality --source rulebook
    python -m src.cli.verify_chunk_quality --fix-duplicates

"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import contextlib

from src.db.engine import get_db_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

load_dotenv()

logger = logging.getLogger(__name__)

# Quality thresholds
MIN_AVG_LENGTH = 100  # chars
MIN_CHUNK_LENGTH = 50  # chars — below this is considered a stub
MIN_CHUNKS_PER_DOC = 3  # fewer chunks per source document signals bad splitting
KEYWORD_COVERAGE_TARGET = 0.80  # 80%


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_chunks(session: Session, source_filter: str | None = None) -> list[dict[str, Any]]:
    """Load rag_chunks rows from the local SQLite database.

    Return a list of dicts with keys: id, source_table, source_row_id, content, metadata.

    Args:
        session: Session.
        source_filter: Source Filter.

    """
    from sqlalchemy import text

    query = "SELECT id, source_table, source_row_id, content, metadata FROM rag_chunks"
    params: dict[str, Any] = {}

    if source_filter:
        query += " WHERE source_table = :src OR json_extract(metadata, '$.category') = :src"
        params["src"] = source_filter

    rows = session.execute(text(query), params).fetchall()
    chunks = []
    for row in rows:
        meta: dict[str, Any] = {}
        if row.metadata:
            with contextlib.suppress(ValueError, TypeError):
                meta = json.loads(row.metadata) if isinstance(row.metadata, str) else row.metadata
        chunks.append(
            {
                "id": row.id,
                "source_table": row.source_table or "",
                "source_row_id": row.source_row_id or "",
                "content": row.content or "",
                "meta": meta,
            },
        )
    return chunks


# ---------------------------------------------------------------------------
# Metric calculations
# ---------------------------------------------------------------------------


def _percentile(sorted_vals: list[int], pct: float) -> int:
    if not sorted_vals:
        return 0
    idx = int(len(sorted_vals) * pct / 100)
    idx = min(idx, len(sorted_vals) - 1)
    return sorted_vals[idx]


def compute_length_stats(chunks: list[dict]) -> dict[str, Any]:
    """Compute length stats.

    Args:
        chunks: Chunks.
        chunks: Chunks.

    Returns:
        Dictionary result.

    """
    lengths = sorted(len(c["content"]) for c in chunks)

    if not lengths:
        return {"count": 0, "avg": 0, "min": 0, "max": 0, "p50": 0, "p95": 0}
    total = sum(lengths)
    return {
        "count": len(lengths),
        "avg": total // len(lengths),
        "min": lengths[0],
        "max": lengths[-1],
        "p50": _percentile(lengths, 50),
        "p95": _percentile(lengths, 95),
    }


def count_empty_chunks(chunks: list[dict]) -> int:
    """Handle the count empty chunks operation.

    Args:
        chunks: Chunks.
        chunks: Chunks.

    Returns:
        Integer result.

    """
    return sum(1 for c in chunks if not c["content"].strip())


def count_stub_chunks(chunks: list[dict]) -> int:
    """Handle the count stub chunks operation.

    Args:
        chunks: Chunks.
        chunks: Chunks.

    Returns:
        Integer result.

    """
    return sum(1 for c in chunks if 0 < len(c["content"].strip()) < MIN_CHUNK_LENGTH)


def find_duplicates(chunks: list[dict]) -> tuple[int, list[str]]:
    """Return (duplicate_count, list of duplicate source_row_ids).

    Args:
        chunks: Chunks.

    """
    seen: dict[str, int] = {}

    dupes = []
    for c in chunks:
        rid = c["source_row_id"]
        if not rid:
            # Fall back to content hash
            rid = hashlib.sha256(c["content"].encode()).hexdigest()
        if rid in seen:
            seen[rid] += 1
            if rid not in dupes:
                dupes.append(rid)
        else:
            seen[rid] = 1
    return len(dupes), dupes


def keyword_coverage(chunks: list[dict]) -> float:
    """Handle the keyword coverage operation.

    Args:
        chunks: Chunks.
        chunks: Chunks.

    Returns:
        float instance.

    """
    if not chunks:
        return 0.0
    with_kw = sum(1 for c in chunks if c["meta"].get("keywords"))
    return with_kw / len(chunks)


def category_distribution(chunks: list[dict]) -> Counter:
    """Handle the category distribution operation.

    Args:
        chunks: Chunks.
        chunks: Chunks.

    Returns:
        Counter instance.

    """
    return Counter(c["meta"].get("category", "unknown") for c in chunks)


def chunks_per_source(chunks: list[dict]) -> dict[str, int]:
    """Handle the chunks per source operation.

    Args:
        chunks: Chunks.
        chunks: Chunks.

    Returns:
        Dictionary result.

    """
    dist: dict[str, int] = defaultdict(int)

    for c in chunks:
        src = c["meta"].get("source", c["source_table"] or "unknown")
        dist[src] += 1
    return dict(dist)


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------


def _status(*, ok: bool) -> str:
    return "✅" if ok else "❌"


def print_report(chunks: list[dict], source_filter: str | None) -> bool:
    """Print the quality report.

        Returns True if all checks pass.

    Args:
        chunks: Chunks.
        source_filter: Source Filter.

    """
    total = len(chunks)

    if total == 0:
        logger.warning("\n⚠️  No chunks found%s.", " for source=" + source_filter if source_filter else "")
        return False

    logger.info("\n%s", "=" * 70)
    logger.info("  📊  RAG Chunk Quality Report%s", f"  [filter: {source_filter}]" if source_filter else "")
    logger.info("%s\n", "=" * 70)

    # --- Length stats ---
    stats = compute_length_stats(chunks)
    avg_ok = stats["avg"] >= MIN_AVG_LENGTH
    min_ok = stats["min"] >= MIN_CHUNK_LENGTH

    # --- Empty / Stub / Duplicate ---
    empty = count_empty_chunks(chunks)
    stubs = count_stub_chunks(chunks)
    dup_cnt, dup_ids = find_duplicates(chunks)
    kw_cov = keyword_coverage(chunks)
    kw_ok = kw_cov >= KEYWORD_COVERAGE_TARGET

    # --- Per-document chunk counts ---
    per_src = chunks_per_source(chunks)
    under_chunked = {src: cnt for src, cnt in per_src.items() if cnt < MIN_CHUNKS_PER_DOC}

    # --- Category distribution ---
    cat_dist = category_distribution(chunks)

    all_ok = avg_ok and min_ok and empty == 0 and dup_cnt == 0

    # Print table
    rows = [
        ("Total Chunks", str(total), "-", "✅"),
        ("Avg Length (chars)", str(stats["avg"]), f"≥ {MIN_AVG_LENGTH}", _status(ok=avg_ok)),
        ("Min Length (chars)", str(stats["min"]), f"≥ {MIN_CHUNK_LENGTH}", _status(ok=min_ok)),
        ("Max Length (chars)", str(stats["max"]), "-", "✅"),
        ("p50 Length (chars)", str(stats["p50"]), "-", "✅"),
        ("p95 Length (chars)", str(stats["p95"]), "-", "✅"),
        ("Empty Chunks", str(empty), "= 0", _status(ok=empty == 0)),
        ("Stub Chunks (<50ch)", str(stubs), "= 0", _status(ok=stubs == 0)),
        ("Duplicate Chunks", str(dup_cnt), "= 0", _status(ok=dup_cnt == 0)),
        ("Keyword Coverage", f"{kw_cov * 100:.1f}%", f"≥ {KEYWORD_COVERAGE_TARGET * 100:.0f}%", _status(ok=kw_ok)),
        ("Under-chunked Docs", str(len(under_chunked)), "= 0", _status(ok=len(under_chunked) == 0)),
    ]

    header = f"│ {'Metric':<24}│ {'Value':>10} │ {'Threshold':>12} │ {'Status':>6} │"
    sep = f"├{'─' * 25}┼{'─' * 12}┼{'─' * 14}┼{'─' * 8}┤"
    top = f"┌{'─' * 25}┬{'─' * 12}┬{'─' * 14}┬{'─' * 8}┐"
    bot = f"└{'─' * 25}┴{'─' * 12}┴{'─' * 14}┴{'─' * 8}┘"

    logger.info(top)
    logger.info(header)
    logger.info(sep)
    for metric, value, threshold, status in rows:
        logger.info("│ %-24s│ %10s │ %12s │ %6s │", metric, value, threshold, status)
    logger.info(bot)

    # Category distribution
    logger.info("\n📂  Category Distribution:")
    for cat, cnt in sorted(cat_dist.items(), key=lambda x: -x[1]):
        bar = "█" * min(cnt // 5 + 1, 30)
        logger.info("   %-30s %5s  %s", cat, cnt, bar)

    # Under-chunked documents
    if under_chunked:
        logger.warning("\n⚠️  Under-chunked documents (< %s chunks):", MIN_CHUNKS_PER_DOC)
        for src, cnt in sorted(under_chunked.items(), key=lambda x: x[1]):
            logger.info("   [%s]  %s", cnt, src)

    # Duplicate IDs sample
    if dup_ids:
        logger.warning("\n⚠️  Sample duplicate source_row_ids (showing up to 5):")
        for did in dup_ids[:5]:
            logger.info("   %s", did)

    logger.error(
        "\n%s\n",
        "✅  All quality checks passed!" if all_ok else "❌  Some quality checks failed — review above.",
    )
    return all_ok


# ---------------------------------------------------------------------------
# Fix helpers
# ---------------------------------------------------------------------------


def remove_duplicate_chunks(session: Session) -> int:
    """Remove duplicate rag_chunks rows keeping the one with the lowest id.

    Return the number of rows deleted.

    Args:
        session: Session.

    """
    from sqlalchemy import text

    result = session.execute(
        text("""
        DELETE FROM rag_chunks
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM rag_chunks
            GROUP BY source_row_id
        )
    """),
    )
    session.commit()
    return result.rowcount  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    """Run the main entry point for this CLI command."""
    parser = argparse.ArgumentParser(description="KBO RAG chunk quality verifier")
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="Filter chunks by source_table or category (e.g. 'rulebook', 'game_rules').",
    )
    parser.add_argument(
        "--fix-duplicates",
        action="store_true",
        help="Remove duplicate chunks (keep lowest-id row per source_row_id).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output raw metrics as JSON instead of table.",
    )
    args = parser.parse_args()

    with get_db_session() as session:
        chunks = load_chunks(session, args.source)

        if args.fix_duplicates:
            deleted = remove_duplicate_chunks(session)
            logger.info("🗑️  Removed %s duplicate chunk row(s).", deleted)
            # Reload after fix
            chunks = load_chunks(session, args.source)

        if args.json:
            stats = compute_length_stats(chunks)
            dup_cnt, _ = find_duplicates(chunks)
            out = {
                **stats,
                "empty_chunks": count_empty_chunks(chunks),
                "stub_chunks": count_stub_chunks(chunks),
                "duplicate_chunks": dup_cnt,
                "keyword_coverage": round(keyword_coverage(chunks), 4),
                "category_distribution": dict(category_distribution(chunks)),
            }
            logger.info(json.dumps(out, ensure_ascii=False, indent=2))
            sys.exit(0)

        ok = print_report(chunks, args.source)
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
