"""Review and selectively append Exact OCI identity candidates.

The command never changes ``data/player_id_overrides.csv`` unless ``--apply``
is passed with a review CSV whose rows explicitly contain ``decision=approve``.
Candidates remain review-only by default, even when the audit classified them as
``exact``.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from src.constants import KST

if TYPE_CHECKING:
    from collections.abc import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[2]
OVERRIDE_FIELDS = [
    "source_table",
    "year",
    "team_code",
    "player_name",
    "resolved_player_id",
    "reason",
    "evidence_source",
]
REVIEW_FIELDS = [*OVERRIDE_FIELDS, "review_status", "validation_reason", "decision"]


def _candidate_evidence_rows(group: dict[str, object], evidence_key: str) -> list[dict[str, object]]:
    """Return candidates with positive evidence in one local source."""
    return [
        candidate
        for candidate in group.get("candidates", [])
        if int(candidate.get(evidence_key, {}).get("rows", 0)) > 0
    ]


def _existing_keys(existing_rows: Iterable[dict[str, str]]) -> dict[tuple[str, str, str, str], str]:
    """Index existing overrides by their logical group key."""
    return {
        (
            row.get("source_table", ""),
            row.get("year", ""),
            row.get("team_code", ""),
            row.get("player_name", ""),
        ): row.get("resolved_player_id", "")
        for row in existing_rows
    }


def build_review_rows(
    report: dict[str, object],
    *,
    existing_rows: Iterable[dict[str, str]],
) -> list[dict[str, str]]:
    """Convert audit groups into reviewable rows without approving them."""
    existing = _existing_keys(existing_rows)
    year = str(report.get("year", 2021))
    review_rows: list[dict[str, str]] = []
    for group in report.get("groups", []):
        team_code = str(group.get("team_code", ""))
        player_name = str(group.get("player_name", ""))
        resolved_id = group.get("resolved_player_id")
        key = ("player_season_pitching", year, team_code, player_name)
        existing_id = existing.get(key)
        game_candidates = _candidate_evidence_rows(group, "local_game")
        season_candidates = _candidate_evidence_rows(group, "local_season")
        candidate_ids = {str(candidate.get("player_id")) for candidate in [*game_candidates, *season_candidates]}
        is_exact = group.get("classification") == "exact" and resolved_id is not None
        if existing_id and existing_id != str(resolved_id):
            status, reason = "conflict", f"existing override points to {existing_id}"
        elif existing_id:
            status, reason = "already_present", "matching override already exists"
        elif is_exact and len(game_candidates) == 1 and str(resolved_id) in candidate_ids:
            status, reason = "eligible", "single candidate with positive local game evidence"
        elif (
            is_exact and len(game_candidates) == 0 and len(season_candidates) == 1 and str(resolved_id) in candidate_ids
        ):
            status, reason = "eligible", "single candidate with positive local season evidence"
        elif is_exact and group.get("evidence_source") == "curated_override":
            status, reason = "eligible", "curated override evidence"
        else:
            status, reason = "manual_review", str(group.get("reason", "insufficient evidence"))
        review_rows.append(
            {
                "source_table": "player_season_pitching",
                "year": year,
                "team_code": team_code,
                "player_name": player_name,
                "resolved_player_id": str(resolved_id or ""),
                "reason": str(group.get("reason", "")),
                "evidence_source": str(group.get("evidence_source", "")),
                "review_status": status,
                "validation_reason": reason,
                "decision": "review",
            },
        )
    return review_rows


def write_review_csv(rows: list[dict[str, str]], output: Path) -> None:
    """Write review rows with an explicit decision column."""
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REVIEW_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _load_csv(path: Path) -> list[dict[str, str]]:
    """Load a UTF-8 CSV as dictionaries."""
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def apply_approved_candidates(review_csv: Path, override_csv: Path) -> int:
    """Append explicitly approved eligible candidates after creating a backup."""
    review_rows = _load_csv(review_csv)
    existing_rows = _load_csv(override_csv) if override_csv.exists() else []
    existing = _existing_keys(existing_rows)
    approved: list[dict[str, str]] = []
    for row in review_rows:
        if row.get("decision", "").strip().lower() != "approve":
            continue
        if row.get("review_status") != "eligible":
            message = f"cannot approve non-eligible candidate: {row.get('player_name', '')}"
            raise ValueError(message)
        key = tuple(row.get(field, "") for field in OVERRIDE_FIELDS[:4])
        current = existing.get(key)
        resolved_id = row.get("resolved_player_id", "")
        if current and current != resolved_id:
            message = f"override conflict for {key}: existing={current}, approved={resolved_id}"
            raise ValueError(message)
        if current == resolved_id:
            continue
        approved.append({field: row.get(field, "") for field in OVERRIDE_FIELDS})
        existing[key] = resolved_id
    if not approved:
        return 0
    stamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    backup = override_csv.with_name(f"{override_csv.name}.backup_{stamp}")
    if override_csv.exists():
        shutil.copy2(override_csv, backup)
    override_csv.parent.mkdir(parents=True, exist_ok=True)
    temp_path = override_csv.with_name(f".{override_csv.name}.{stamp}.tmp")
    with temp_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OVERRIDE_FIELDS)
        writer.writeheader()
        writer.writerows([*existing_rows, *approved])
    temp_path.replace(override_csv)
    return len(approved)


def _default_review_path(audit_json: Path) -> Path:
    """Return the review CSV path beside an audit JSON artifact."""
    return audit_json.with_name(f"{audit_json.stem}_review.csv")


def main(argv: list[str] | None = None) -> int:
    """Run the Exact-candidate review CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-json", type=Path, required=True)
    parser.add_argument("--override-csv", type=Path, default=PROJECT_ROOT / "data/player_id_overrides.csv")
    parser.add_argument("--output-csv", type=Path, default=None)
    parser.add_argument("--apply", action="store_true", help="Append only rows marked decision=approve")
    parser.add_argument("--approved-csv", type=Path, default=None, help="Reviewed CSV used with --apply")
    args = parser.parse_args(argv)

    if args.apply:
        if args.approved_csv is None:
            parser.error("--approved-csv is required with --apply")
        count = apply_approved_candidates(args.approved_csv, args.override_csv)
        print(json.dumps({"applied": count, "override_csv": str(args.override_csv)}, ensure_ascii=False))
        return 0

    report = json.loads(args.audit_json.read_text(encoding="utf-8"))
    existing_rows = _load_csv(args.override_csv) if args.override_csv.exists() else []
    rows = build_review_rows(report, existing_rows=existing_rows)
    output = args.output_csv or _default_review_path(args.audit_json)
    write_review_csv(rows, output)
    summary = {
        status: sum(row["review_status"] == status for row in rows)
        for status in ("eligible", "manual_review", "already_present", "conflict")
    }
    print(json.dumps({"output_csv": str(output), **summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
