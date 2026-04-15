from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.sources.relay import read_manifest_entries


FIELDNAMES = ["game_id", "source_type", "locator", "format", "priority", "notes"]


def _normalize_notes(value: str | None) -> str:
    return str(value or "").strip()


def _load_existing_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _dedupe_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    deduped: dict[tuple[str, str, str, str, str], dict[str, str]] = {}
    for row in rows:
        normalized = {
            "game_id": str(row.get("game_id") or "").strip(),
            "source_type": str(row.get("source_type") or "").strip(),
            "locator": str(row.get("locator") or "").strip(),
            "format": str(row.get("format") or "").strip(),
            "priority": str(row.get("priority") or "100").strip() or "100",
            "notes": _normalize_notes(row.get("notes")),
        }
        if not all(normalized[key] for key in ("game_id", "source_type", "locator", "format")):
            continue
        key = (
            normalized["game_id"],
            normalized["source_type"],
            normalized["locator"],
            normalized["format"],
            normalized["priority"],
        )
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = normalized
            continue
        if not existing["notes"] and normalized["notes"]:
            deduped[key] = normalized
    return sorted(
        deduped.values(),
        key=lambda row: (
            row["game_id"],
            int(row["priority"]),
            row["source_type"],
            row["locator"],
        ),
    )


def promote_manifests(
    *,
    target_path: Path,
    append_paths: str | list[str],
) -> int:
    rows = _load_existing_rows(target_path)
    for entry in read_manifest_entries(append_paths):
        rows.append(
            {
                "game_id": entry.game_id,
                "source_type": entry.source_type,
                "locator": entry.locator,
                "format": entry.format,
                "priority": str(entry.priority),
                "notes": _normalize_notes(entry.notes),
            }
        )

    deduped = _dedupe_rows(rows)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with target_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(deduped)
    return len(deduped)


def main() -> None:
    parser = argparse.ArgumentParser(description="Promote generated recovery manifest rows into the active source manifest")
    parser.add_argument(
        "--target",
        default="data/recovery/source_manifest.csv",
        help="Active source manifest to update",
    )
    parser.add_argument(
        "--append",
        default="data/recovery/source_manifest_auto.csv",
        help="Comma separated manifest paths to merge into the active manifest",
    )
    args = parser.parse_args()

    written = promote_manifests(
        target_path=Path(args.target),
        append_paths=args.append,
    )
    print(f"Wrote {written} total manifest rows to {args.target}")


if __name__ == "__main__":
    main()
