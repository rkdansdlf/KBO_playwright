"""CLI to enforce RAG tombstone plans."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from src.db.engine import get_rag_index_session
from src.services.rag_primary_tombstone import inspect_primary_tombstones, tombstone_primary_rows


def _load_source_keys(keys: list[str] | None, manifest_path: Path | str | None) -> tuple[str, ...]:
    result = []
    if keys:
        result.extend(keys)
    if manifest_path:
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        for source in manifest.get("sources", []):
            result.extend(source.get("deleted_identities", []))
    seen = set()
    deduped = []
    for item in result:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return tuple(deduped)


def main(argv: list[str] | None = None) -> int:
    """Run RAG tombstone execution."""
    parser = argparse.ArgumentParser(description="Tombstone deleted RAG chunks.")
    parser.add_argument("--keys", type=str, nargs="*", default=[])
    parser.add_argument("--manifest", type=str)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    keys = _load_source_keys(args.keys, args.manifest)

    with get_rag_index_session() as session:
        if args.apply and not args.dry_run:
            results = tombstone_primary_rows(session, keys)
        else:
            results = inspect_primary_tombstones(session, keys)

        for res in results:
            sys.stdout.write(f"{res.to_dict()}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
