from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.sources.relay import default_source_order_for_bucket, derive_bucket_id


def _recommend_seed(source_order: list[str]) -> tuple[str, str]:
    if "import" in source_order:
        return "json_archive", "normalized_events_json"
    if "manual" in source_order:
        return "manual_text", "pbp_text"
    if "kbo" in source_order:
        return "kbo", "kbo_html"
    return "manual_text", "pbp_text"


def build_candidates(input_path: Path, output_path: Path) -> int:
    with input_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "game_id",
                "source_type",
                "locator",
                "format",
                "priority",
                "notes",
                "bucket_id",
                "recommended_source_order",
                "league_type_name",
            ],
        )
        writer.writeheader()
        for row in rows:
            game_id = str(row.get("game_id") or "").strip()
            if not game_id:
                continue
            league_type_name = str(row.get("league_type_name") or "").strip() or None
            bucket_id = derive_bucket_id(game_id, league_type_name)
            source_order = default_source_order_for_bucket(bucket_id)
            source_type, manifest_format = _recommend_seed(source_order)
            writer.writerow(
                {
                    "game_id": game_id,
                    "source_type": source_type,
                    "locator": "",
                    "format": manifest_format,
                    "priority": 1,
                    "notes": "Fill locator once archive/manual raw source is secured",
                    "bucket_id": bucket_id,
                    "recommended_source_order": " -> ".join(source_order),
                    "league_type_name": league_type_name or "",
                }
            )
            written += 1
    return written


def main():
    parser = argparse.ArgumentParser(description="Build manifest candidate rows from unresolved relay backlog")
    parser.add_argument(
        "--input",
        default="data/recovery/relay_unresolved_completed_games_oci_20260415.csv",
        help="Unresolved backlog CSV",
    )
    parser.add_argument(
        "--output",
        default="data/recovery/source_manifest_candidates_20260415.csv",
        help="Output candidate manifest CSV",
    )
    args = parser.parse_args()

    written = build_candidates(Path(args.input), Path(args.output))
    print(f"Wrote {written} candidate rows to {args.output}")


if __name__ == "__main__":
    main()
