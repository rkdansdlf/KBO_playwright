"""Contracts for the movement-duplicate purger.

The dangerous half is the plan: it decides which legacy chunks are duplicates of a
natural-key chunk. If it ever confirms a row whose note is missing from the
counterpart, that information is gone when the row is deleted.
"""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from scripts.maintenance.purge_duplicate_movement_chunks import (
    ALLOW_ENV,
    DuplicateRow,
    legacy_fields,
    load_backup,
    main,
    plan_duplicates,
)
from src.models.player import PlayerMovement


def test_legacy_fields_reads_the_labelled_lines() -> None:
    content = "선수 이동 기록 (FA, 트레이드, 드래프트)\n날짜: 2024-07-18\n이동 유형: 부상자 명단\n선수: 이우성(외야수)\n팀: 해태 타이거즈\n비고: 10일"
    fields = legacy_fields(content)

    assert fields["날짜"] == "2024-07-18"
    assert fields["선수"] == "이우성(외야수)"
    assert fields["이동 유형"] == "부상자 명단"
    assert fields["비고"] == "10일"


def test_legacy_fields_ignores_a_body_that_is_not_in_the_legacy_shape() -> None:
    assert legacy_fields("2024-07-18: 이우성(외야수) 선수 부상자 명단 (팀: KIA)") == {}


def _session() -> Session:
    engine = create_engine("sqlite://")
    PlayerMovement.__table__.create(engine)
    from src.models.rag_chunk import RagChunk

    RagChunk.__table__.create(engine, checkfirst=False)
    return Session(engine)


def test_the_plan_confirms_a_legacy_row_whose_note_survives_under_the_natural_key() -> None:
    """The counterpart carries the note, so deleting the legacy row loses nothing."""
    from src.models.rag_chunk import RagChunk

    session = _session()
    session.add(
        PlayerMovement(
            movement_date=date(2024, 7, 18),
            section="부상자 명단",
            team_code="해태",
            canonical_team_id="KIA",
            player_name="이우성(외야수)",
            remarks="10일",
        ),
    )
    session.commit()

    natural = RagChunk(
        source_table="player_movements",
        source_row_id="2024-07-18_KIA_이우성(외야수)_부상자 명단_060cceb5e74f7200",
        content="2024-07-18: 이우성(외야수) 선수 부상자 명단 (팀: KIA) — 10일",
        index_status="ACTIVE",
    )
    legacy = RagChunk(
        source_table="player_movements",
        source_row_id="id=970",
        content="선수 이동 기록\n날짜: 2024-07-18\n이동 유형: 부상자 명단\n선수: 이우성(외야수)\n팀: 해태 타이거즈\n비고: 10일",
        index_status="ACTIVE",
    )
    session.add_all([natural, legacy])
    session.commit()

    confirmed, rejected = plan_duplicates(session)

    assert rejected == []
    assert [row.source_row_id for row in confirmed] == ["id=970"]
    # The source's team_code is 해태 here; the key must still be canonical.
    assert confirmed[0].natural_key.startswith("2024-07-18_KIA_")
    assert "해태" not in confirmed[0].natural_key


def test_the_plan_refuses_a_row_whose_note_is_missing_from_the_counterpart() -> None:
    """A note that exists only in the legacy chunk is information, not duplication."""
    from src.models.rag_chunk import RagChunk

    session = _session()
    session.add(
        PlayerMovement(
            movement_date=date(2024, 7, 18),
            section="부상자 명단",
            team_code="해태",
            canonical_team_id="KIA",
            player_name="이우성(외야수)",
            remarks="10일",
        ),
    )
    session.commit()

    session.add_all(
        [
            RagChunk(
                source_table="player_movements",
                source_row_id="2024-07-18_KIA_이우성(외야수)_부상자 명단_060cceb5e74f7200",
                content="2024-07-18: 이우성(외야수) 선수 부상자 명단 (팀: KIA)",
                index_status="ACTIVE",
            ),
            RagChunk(
                source_table="player_movements",
                source_row_id="id=970",
                content="선수 이동 기록\n날짜: 2024-07-18\n이동 유형: 부상자 명단\n선수: 이우성(외야수)\n팀: KIA\n비고: 10일",
                index_status="ACTIVE",
            ),
        ],
    )
    session.commit()

    confirmed, rejected = plan_duplicates(session)

    assert confirmed == []
    assert len(rejected) == 1
    assert "is missing from" in rejected[0]


def test_the_plan_refuses_a_row_with_no_source_row() -> None:
    from src.models.rag_chunk import RagChunk

    session = _session()
    session.add(
        RagChunk(
            source_table="player_movements",
            source_row_id="id=999",
            content="선수 이동 기록\n날짜: 1999-01-01\n이동 유형: 트레이드\n선수: 없음\n팀: 롯데",
            index_status="ACTIVE",
        ),
    )
    session.commit()

    confirmed, rejected = plan_duplicates(session)

    assert confirmed == []
    assert "no source row" in rejected[0]


def test_load_backup_rejects_a_file_that_is_not_a_plan(tmp_path: Path) -> None:
    path = tmp_path / "bad.csv"
    path.write_text("id,other\n1,2\n", encoding="utf-8")

    with pytest.raises(ValueError, match="missing columns"):
        load_backup(path)


def test_load_backup_round_trips_a_plan(tmp_path: Path) -> None:
    path = tmp_path / "plan.csv"
    row = DuplicateRow(
        chunk_id=1,
        source_row_id="id=970",
        index_status="ACTIVE",
        natural_key="2024-07-18_KIA_이우성(외야수)_부상자 명단_060cceb5e74f7200",
        legacy_note="10일",
    )
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("chunk_id", "source_row_id", "index_status", "natural_key", "legacy_note", "reason"),
        )
        writer.writeheader()
        writer.writerow(
            {
                "chunk_id": row.chunk_id,
                "source_row_id": row.source_row_id,
                "index_status": row.index_status,
                "natural_key": row.natural_key,
                "legacy_note": row.legacy_note,
                "reason": row.reason,
            },
        )

    loaded = load_backup(path)

    assert loaded == [row]


def test_apply_without_the_env_guard_writes_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "plan.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("chunk_id", "source_row_id", "index_status", "natural_key", "legacy_note", "reason"),
        )
        writer.writeheader()
        writer.writerow(
            {
                "chunk_id": 1,
                "source_row_id": "id=970",
                "index_status": "ACTIVE",
                "natural_key": "2024-07-18_KIA_x_부상자 명단_060cceb5e74f7200",
                "legacy_note": "",
                "reason": "duplicate_of_natural_key",
            },
        )
    monkeypatch.delenv(ALLOW_ENV, raising=False)

    exit_code = main(["--backup-file", str(path), "--apply"])

    assert exit_code == 3
