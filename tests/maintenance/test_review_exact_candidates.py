"""Tests for the reviewed Exact-candidate override workflow."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.maintenance.review_exact_candidates import (
    apply_approved_candidates,
    approve_eligible_rows,
    build_review_rows,
    write_review_csv,
)


def _report() -> dict[str, object]:
    return {
        "year": 2021,
        "groups": [
            {
                "team_code": "LG",
                "player_name": "김A",
                "candidate_ids": [1, 2],
                "classification": "exact",
                "resolved_player_id": 1,
                "reason": "unique local game pitching evidence",
                "evidence_source": "local_game_pitching_stats",
                "candidates": [
                    {"player_id": 1, "local_game": {"available": True, "rows": 3, "games": 2}},
                    {"player_id": 2, "local_game": {"available": True, "rows": 0, "games": 0}},
                ],
            },
            {
                "team_code": "KIA",
                "player_name": "김B",
                "candidate_ids": [3, 4],
                "classification": "unresolved",
                "resolved_player_id": None,
                "reason": "no exact local game evidence",
                "evidence_source": "none",
                "candidates": [],
            },
        ],
    }


def test_build_review_rows_keeps_unresolved_out_of_eligible_candidates() -> None:
    rows = build_review_rows(_report(), existing_rows=[])

    assert rows[0]["review_status"] == "eligible"
    assert rows[0]["decision"] == "review"
    assert rows[1]["review_status"] == "manual_review"
    assert all(row["resolved_player_id"] for row in rows[:1])


def test_write_and_apply_requires_explicit_approval(tmp_path: Path) -> None:
    review_csv = tmp_path / "review.csv"
    override_csv = tmp_path / "overrides.csv"
    override_csv.write_text(
        "source_table,year,team_code,player_name,resolved_player_id,reason,evidence_source\n",
        encoding="utf-8",
    )
    rows = build_review_rows(_report(), existing_rows=[])
    write_review_csv(rows, review_csv)

    assert apply_approved_candidates(review_csv, override_csv) == 0
    with review_csv.open(newline="", encoding="utf-8") as handle:
        editable_rows = list(csv.DictReader(handle))
    editable_rows[0]["decision"] = "approve"
    with review_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=editable_rows[0].keys())
        writer.writeheader()
        writer.writerows(editable_rows)

    assert apply_approved_candidates(review_csv, override_csv) == 1
    content = override_csv.read_text(encoding="utf-8")
    assert "player_season_pitching,2021,LG,김A,1" in content
    assert list(override_csv.parent.glob("overrides.csv.backup_*"))


def test_season_evidence_is_eligible_when_game_evidence_is_absent() -> None:
    report = _report()
    group = report["groups"][0]
    group["candidates"][0]["local_game"] = {"available": True, "rows": 0, "games": 0}
    group["candidates"][0]["local_season"] = {"available": True, "rows": 1, "innings_outs": 2}

    rows = build_review_rows(report, existing_rows=[])

    assert rows[0]["review_status"] == "eligible"
    assert rows[0]["validation_reason"] == "single candidate with positive local season evidence"


def test_approve_eligible_rows_does_not_approve_manual_review() -> None:
    rows = build_review_rows(_report(), existing_rows=[])

    approved = approve_eligible_rows(rows)

    assert approved[0]["decision"] == "approve"
    assert approved[1]["decision"] == "review"


def test_review_rows_are_json_serializable() -> None:
    rows = build_review_rows(_report(), existing_rows=[])
    assert json.loads(json.dumps(rows, ensure_ascii=False))[0]["review_status"] == "eligible"
