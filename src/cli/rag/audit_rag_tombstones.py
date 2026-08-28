"""Audit RAG tombstones without modifying the index."""

from __future__ import annotations

import argparse
import json
import sys
from typing import TYPE_CHECKING

from src.db.engine import get_rag_index_session
from src.services.rag_tombstone_audit import audit_tombstone_session

if TYPE_CHECKING:
    from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """Classify deleted RAG identities and optionally fail on unexplained rows."""
    parser = argparse.ArgumentParser(description="Audit RAG tombstones without modifying the index")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Render JSON output")
    parser.add_argument(
        "--fail-on-unexplained",
        action="store_true",
        help="Return exit code 1 when a deleted identity has no canonical replacement",
    )
    args = parser.parse_args(argv)

    with get_rag_index_session() as session:
        report = audit_tombstone_session(session)

    payload = report.to_dict()
    if args.as_json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    else:
        sys.stdout.write(
            f"classification={report.classification} deleted={len(report.deleted_keys)} "
            f"expected_rekey={len(report.expected_rekey_keys)} unexplained={len(report.unexplained_keys)}\n"
        )
        for source_key in report.unexplained_keys:
            sys.stdout.write(f"UNEXPLAINED: {source_key}\n")
    return 1 if args.fail_on_unexplained and not report.is_consistent else 0


if __name__ == "__main__":
    raise SystemExit(main())
