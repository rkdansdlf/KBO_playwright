from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


RAW_TYPE_TO_SOURCE = {
    "json_archive": ("json_archive", "normalized_events_json"),
    "html_archive": ("html_archive", "relay_html"),
    "manual_text": ("manual_text", "pbp_text"),
}

EXTENSION_HINTS = {
    ".json": ("json_archive", "normalized_events_json"),
    ".html": ("html_archive", "relay_html"),
    ".htm": ("html_archive", "relay_html"),
    ".txt": ("manual_text", "pbp_text"),
}


def _load_candidates(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _relative_locator(base_dir: Path, file_path: Path) -> str:
    try:
        return str(file_path.relative_to(base_dir))
    except ValueError:
        return str(file_path)


def _infer_manifest_fields(file_path: Path) -> tuple[str, str] | None:
    parent_name = file_path.parent.name
    if parent_name in RAW_TYPE_TO_SOURCE:
        return RAW_TYPE_TO_SOURCE[parent_name]
    return EXTENSION_HINTS.get(file_path.suffix.lower())


def _find_matching_raw_file(raw_root: Path, game_id: str) -> Path | None:
    candidates = sorted(raw_root.rglob(f"{game_id}*"))
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def build_manifest(candidates_path: Path, raw_root: Path, output_path: Path) -> int:
    rows = _load_candidates(candidates_path)
    manifest_rows: list[dict[str, str]] = []
    for row in rows:
        game_id = str(row.get("game_id") or "").strip()
        if not game_id:
            continue
        matched = _find_matching_raw_file(raw_root, game_id)
        if matched is None:
            continue
        inferred = _infer_manifest_fields(matched)
        if inferred is None:
            continue
        source_type, manifest_format = inferred
        manifest_rows.append(
            {
                "game_id": game_id,
                "source_type": source_type,
                "locator": _relative_locator(raw_root.parent, matched),
                "format": manifest_format,
                "priority": row.get("priority") or "1",
                "notes": f"auto-built from raw drop: {matched.name}",
            }
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["game_id", "source_type", "locator", "format", "priority", "notes"],
        )
        writer.writeheader()
        writer.writerows(manifest_rows)
    return len(manifest_rows)


def main():
    parser = argparse.ArgumentParser(description="Build source_manifest.csv from dropped raw relay files")
    parser.add_argument(
        "--candidates",
        default="data/recovery/source_manifest_candidates_20260415.csv",
        help="Candidate manifest CSV",
    )
    parser.add_argument(
        "--raw-root",
        default="data/recovery/raw",
        help="Directory containing dropped raw files",
    )
    parser.add_argument(
        "--output",
        default="data/recovery/source_manifest_auto.csv",
        help="Generated source manifest output path",
    )
    args = parser.parse_args()

    written = build_manifest(
        candidates_path=Path(args.candidates),
        raw_root=Path(args.raw_root),
        output_path=Path(args.output),
    )
    print(f"Wrote {written} manifest rows to {args.output}")


if __name__ == "__main__":
    main()
