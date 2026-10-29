"""기존 RAG 청크의 ``document_type``을 생성기가 쓸 값으로 채운다.

왜 필요한가: `document_type` 컬럼은 2026-10-28에야 추가됐고(마이그레이션 066),
그 전에 인덱싱된 348,121행은 NULL이다. 그 상태에서 라우터가 유형 필터를 걸면
`WHERE document_type = 'milestone'`이 **0건**이 되고, 검색은 "데이터가 없어서"가
아니라 "값이 없어서" 실패한다 — 실제로 `2024년 김도영 홈런` 질문이 그렇게
0건이 됐고, 폴백이 전체 스캔을 돌아 130초를 썼다.

**매핑을 복사하지 않는다**: 소스마다 1건을 실제로 생성해 그 청크의
`document_type`을 읽는다. 표를 손으로 적으면 생성기가 유형을 바꿔도 백필은
옛 값을 쓰고, 그 불일치가 다시 0건 필터를 만든다. 자기 매핑을 상수로 갖는
검사는 "필터가 맞는가"를 묻지 않으므로 그 어긋남을 못 본다.

생성기에 없는 소스(`game_summary`·`kbo_seasons`·`markdown_docs` 등)는 건드리지
않는다. 그 유형이 무엇이어야 하는지 말해 주는 코드가 없고, 지어내면 그 값으로
거르는 검색이 잘못된 결과를 낸다.

Usage::

    python3 -m scripts.maintenance.backfill_rag_document_type --dry-run
    python3 -m scripts.maintenance.backfill_rag_document_type --apply
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import text

from src.constants import KST
from src.db.engine import Engine

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BACKUP_DIR = Path("data/repair_backups")


@dataclass(frozen=True)
class SourceMapping:
    """One source table and the document type its generator writes."""

    source_table: str
    document_type: str


def discover_generator_types(session: Session) -> list[SourceMapping]:
    """Learn each source's ``document_type`` from the generator itself.

    Calls every generator with ``limit=1`` and reads the field off the produced
    chunk. That keeps this tool honest: if a generator changes its type, the next
    backfill uses the new one instead of replaying a hand-copied table.

    Args:
        session: Read session for the generators.

    Returns:
        One entry per generator that produced a chunk, sorted by source table.

    """
    from src.cli.rag import build_rag_index

    found: dict[str, str] = {}
    for source, generator in sorted(build_rag_index._SOURCE_MAP.items()):
        try:
            chunk = next(iter(generator(session, None, 1)), None)
        except Exception:
            logger.warning("generator for %s could not produce a sample; skipping", source)
            continue
        if not chunk:
            continue
        document_type = chunk.get("document_type")
        source_table = chunk.get("source_table")
        if not document_type or not source_table:
            continue
        found[source_table] = document_type
    return [SourceMapping(table, found[table]) for table in sorted(found)]


def count_pending(mappings: list[SourceMapping]) -> dict[str, int]:
    """Count NULL ``document_type`` rows per source table."""
    pending: dict[str, int] = {}
    with Engine.connect() as conn:
        for mapping in mappings:
            pending[mapping.source_table] = conn.execute(
                text("SELECT count(*) FROM rag_chunks WHERE source_table = :t AND document_type IS NULL"),
                {"t": mapping.source_table},
            ).scalar_one()
    return pending


def backfill(mappings: list[SourceMapping], *, apply: bool) -> int:
    """Fill ``document_type`` for the mapped sources.

    Args:
        mappings: Source-to-type pairs learned from the generators.
        apply: When False, only report what would change.

    Returns:
        Process exit code.

    """
    pending = count_pending(mappings)
    total = sum(pending.values())
    print(f"{'source_table':<26}{'document_type':<20}{'NULL 행':>10}")
    for mapping in mappings:
        print(f"{mapping.source_table:<26}{mapping.document_type:<20}{pending[mapping.source_table]:>10,}")
    print(f"{'합계':<26}{'':<20}{total:>10,}")

    if not apply:
        print("\n[dry-run] no writes; pass --apply to fill")
        return 0
    if total == 0:
        print("\n[skip] nothing to fill")
        return 0

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(KST).strftime("%Y%m%dT%H%M%S")
    backup_path = BACKUP_DIR / f"rag_document_type_backfill_{stamp}.csv"

    # 백업: 매핑표 자체를 남긴다(행 단위 백업은 NULL뿐이라 불필요하다).
    with backup_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["source_table", "document_type", "rows"])
        for mapping in mappings:
            writer.writerow([mapping.source_table, mapping.document_type, pending[mapping.source_table]])
    print(f"\nbackup: {backup_path}")

    updated = 0
    with Engine.begin() as conn:
        for mapping in mappings:
            result = conn.execute(
                text(
                    "UPDATE rag_chunks SET document_type = :d WHERE source_table = :t AND document_type IS NULL",
                ),
                {"d": mapping.document_type, "t": mapping.source_table},
            )
            updated += result.rowcount
            logger.info("%s -> %s (%s행)", mapping.source_table, mapping.document_type, result.rowcount)

    print(f"\nupdated: {updated:,}행")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Run the backfill."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="write the values (default is dry-run)")
    args = parser.parse_args(argv)

    from src.db.engine import SessionLocal

    with SessionLocal() as session:
        mappings = discover_generator_types(session)
    if not mappings:
        print("[refused] no generator produced a sample; refusing to guess types")
        return 1

    print(f"generators learned: {len(mappings)}\n")
    return backfill(mappings, apply=args.apply)


if __name__ == "__main__":
    sys.exit(main())
