from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.sources.relay import CapabilityRecord, upsert_capability_record


def apply_overrides(override_path: Path, capability_path: Path) -> int:
    with override_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        count = 0
        for row in reader:
            bucket_id = str(row.get("bucket_id") or "").strip()
            source_name = str(row.get("source_name") or "").strip()
            if not bucket_id or not source_name:
                continue
            upsert_capability_record(
                capability_path,
                CapabilityRecord(
                    bucket_id=bucket_id,
                    source_name=source_name,
                    sample_size=int(row.get("sample_size") or 0),
                    supported=str(row.get("supported") or "").strip().lower() == "true",
                    last_checked_at=datetime.now(timezone.utc).isoformat(),
                    notes=(row.get("notes") or "").strip() or None,
                ),
            )
            count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply explicit capability overrides to source_capability.csv")
    parser.add_argument(
        "--overrides",
        default="data/recovery/capability_overrides_20260415.csv",
        help="CSV file containing capability override rows",
    )
    parser.add_argument(
        "--capability-path",
        default="data/recovery/source_capability.csv",
        help="Capability cache CSV to update",
    )
    args = parser.parse_args()

    applied = apply_overrides(
        override_path=Path(args.overrides),
        capability_path=Path(args.capability_path),
    )
    print(f"Applied {applied} capability overrides to {args.capability_path}")


if __name__ == "__main__":
    main()
