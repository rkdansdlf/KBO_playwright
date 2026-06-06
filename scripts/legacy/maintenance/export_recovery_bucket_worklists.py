from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


def export_worklists(input_path: Path, output_dir: Path) -> dict[str, int]:
    with input_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        bucket_id = str(row.get("bucket_id") or "").strip() or "unclassified"
        grouped[bucket_id].append(row)

    output_dir.mkdir(parents=True, exist_ok=True)
    for stale_path in output_dir.glob("*.csv"):
        stale_path.unlink()
    counts: dict[str, int] = {}
    for bucket_id, bucket_rows in sorted(grouped.items()):
        counts[bucket_id] = len(bucket_rows)
        out_path = output_dir / f"{bucket_id}.csv"
        with out_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(bucket_rows[0].keys()))
            writer.writeheader()
            writer.writerows(bucket_rows)
    return counts


def write_summary(input_path: Path, output_path: Path, counts: dict[str, int]) -> None:
    with input_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    league_counts = Counter(str(row.get("league_type_name") or "").strip() or "unknown" for row in rows)
    lines = [
        "# Recovery Worklists",
        "",
        f"Source file: `{input_path}`",
        "",
        "## Bucket Counts",
    ]
    for bucket_id, count in sorted(counts.items()):
        lines.append(f"- `{bucket_id}`: {count}")
    lines.extend(["", "## League Counts"])
    for league_name, count in sorted(league_counts.items()):
        lines.append(f"- `{league_name}`: {count}")
    lines.extend(
        [
            "",
            "## Raw Drop Reminder",
            "- Drop recovered files under `data/recovery/raw/json_archive`, `html_archive`, or `manual_text`.",
            "- Prefix filenames with `game_id`.",
            "- Then run `./.venv/bin/python scripts/maintenance/build_source_manifest_from_raw_drop.py`.",
        ]
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Export unresolved recovery candidates into per-bucket worklists")
    parser.add_argument(
        "--input",
        default="data/recovery/source_manifest_candidates_20260415.csv",
        help="Candidate manifest CSV",
    )
    parser.add_argument(
        "--output-dir",
        default="data/recovery/worklists",
        help="Directory for per-bucket worklists",
    )
    parser.add_argument(
        "--summary-out",
        default="data/recovery/worklists/README.md",
        help="Summary markdown path",
    )
    args = parser.parse_args()

    counts = export_worklists(Path(args.input), Path(args.output_dir))
    write_summary(Path(args.input), Path(args.summary_out), counts)
    print(f"Exported {sum(counts.values())} rows across {len(counts)} worklists")


if __name__ == "__main__":
    main()
