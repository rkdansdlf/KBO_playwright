"""Compare stored normalized crawl evidence with a projection payload."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from src.db.engine import SessionLocal
from src.models.crawl_evidence import CrawlEvidence
from src.repositories.crawl_evidence_repository import (
    build_game_detail_db_projection,
    compare_evidence_to_projection,
    load_json_artifact,
)
from src.utils.data_lineage import diff_values, sha256_json

if TYPE_CHECKING:
    from collections.abc import Sequence


def _load_json_file(path: str) -> object:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _render_result(
    evidence: CrawlEvidence,
    differences: list[dict[str, object]],
    *,
    validation_status: str,
) -> dict[str, object]:
    return {
        "evidence_id": evidence.id,
        "entity_type": evidence.entity_type,
        "entity_id": evidence.entity_id,
        "dataset": evidence.dataset,
        "validation_status": validation_status,
        "raw_hash": evidence.raw_hash,
        "parsed_hash": evidence.parsed_hash,
        "normalized_hash": evidence.normalized_hash,
        "db_projection_hash": evidence.db_projection_hash,
        "difference_count": len(differences),
        "differences": differences[:50],
    }


def main(argv: Sequence[str] | None = None) -> int:
    """Compare a stored evidence payload with a JSON database projection."""
    parser = argparse.ArgumentParser(description="Compare crawl evidence with a DB projection JSON file")
    parser.add_argument("--evidence-id", type=int, required=True)
    parser.add_argument("--actual-json", help="JSON file containing the DB projection")
    parser.add_argument("--json", action="store_true", help="Render JSON output")
    parser.add_argument("--record", action="store_true", help="Persist the comparison result to the evidence row")
    args = parser.parse_args(argv)

    with SessionLocal() as session:
        evidence = session.get(CrawlEvidence, args.evidence_id)
        if evidence is None:
            msg = f"Crawl evidence not found: {args.evidence_id}"
            raise SystemExit(msg)
        expected = load_json_artifact(evidence.normalized_payload_path, evidence.normalized_hash)
        actual = (
            _load_json_file(args.actual_json)
            if args.actual_json
            else build_game_detail_db_projection(session, evidence.entity_id, expected)
        )
        differences = diff_values(expected, actual)
        validation_status = "verified" if not differences else "mismatch"
        if args.record:
            compare_evidence_to_projection(session, evidence.id, actual)
            session.commit()
        result = _render_result(evidence, differences, validation_status=validation_status)

    if args.json:
        sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    else:
        status = "PASS" if not differences else "FAIL"
        sys.stdout.write(
            f"Crawl evidence {status}: id={evidence.id} differences={len(differences)} "
            f"actual_hash={sha256_json(actual)}\n",
        )
    return 0 if not differences else 1


if __name__ == "__main__":
    raise SystemExit(main())
