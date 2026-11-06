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
    _apply_deletions,
    DuplicateRow,
    legacy_fields,
    load_backup,
    main,
    plan_duplicates,
)
from src.models.player import PlayerMovement
from src.services.rag_index_identity import stable_player_movement_source_row_id


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
    from src.models.audit_trail import CorrectionAuditTrail
    from src.models.rag_chunk import RagChunk
    from src.models.team_history import TeamHistory

    RagChunk.__table__.create(engine, checkfirst=False)
    CorrectionAuditTrail.__table__.create(engine, checkfirst=False)
    TeamHistory.__table__.create(engine, checkfirst=False)
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


def test_a_stale_natural_key_is_confirmed_when_the_content_matches() -> None:
    """The Korean-name key and the canonical key must hold the same text."""
    from src.models.rag_chunk import RagChunk

    session = _session()
    session.add(
        PlayerMovement(
            movement_date=date(2025, 6, 12),
            section="부상자 명단",
            team_code="기아",
            canonical_team_id="KIA",
            player_name="윤도현(내야수)",
            remarks="30일",
        ),
    )
    session.commit()

    content = "2025-06-12: 윤도현(내야수) 선수 부상자 명단 (팀: KIA) — 30일"
    canonical_key = stable_player_movement_source_row_id(date(2025, 6, 12), "KIA", "윤도현(내야수)", "부상자 명단")
    raw_key = stable_player_movement_source_row_id(date(2025, 6, 12), "기아", "윤도현(내야수)", "부상자 명단")
    session.add_all(
        [
            RagChunk(
                source_table="player_movements", source_row_id=canonical_key, content=content, index_status="ACTIVE"
            ),
            RagChunk(source_table="player_movements", source_row_id=raw_key, content=content, index_status="ACTIVE"),
        ],
    )
    session.commit()

    confirmed, rejected = plan_duplicates(session)

    assert rejected == []
    stale = [row for row in confirmed if row.reason == "superseded_by_canonical_key"]
    assert len(stale) == 1
    assert stale[0].source_row_id == raw_key
    assert stale[0].natural_key == canonical_key


def test_a_stale_natural_key_is_left_alone_when_the_content_differs() -> None:
    """Two movements can share (date, player, section) and still be different facts."""
    from src.models.rag_chunk import RagChunk

    session = _session()
    session.add(
        PlayerMovement(
            movement_date=date(2018, 12, 7),
            section="트레이드",
            team_code="넥센",
            canonical_team_id="NX",
            player_name="김동엽(외야수)",
            remarks="SK→넥센",
        ),
    )
    session.commit()

    session.add_all(
        [
            RagChunk(
                source_table="player_movements",
                source_row_id="2018-12-07_NX_김동엽(외야수)_트레이드_f48692b0ac298556",
                content="2018-12-07: 김동엽(외야수) 선수 트레이드 (팀: 넥센) — SK→넥센",
                index_status="ACTIVE",
            ),
            RagChunk(
                source_table="player_movements",
                source_row_id="2018-12-07_SS_김동엽(외야수)_트레이드_42f3d3a6605c5578",
                content="2018-12-07: 김동엽(외야수) 선수 트레이드 (팀: 삼성) — 넥센→삼성",
                index_status="ACTIVE",
            ),
        ],
    )
    session.commit()

    confirmed, rejected = plan_duplicates(session)

    assert [row for row in confirmed if row.reason == "superseded_by_canonical_key"] == []


def test_the_deletions_are_split_into_bounded_statements(monkeypatch: pytest.MonkeyPatch) -> None:
    """One ``IN (...)`` covering every id timed out waiting on the server.

    Measured 2026-11-05: the first attempt at 7,227 ids died with ``could not
    receive data from server: Operation timed out``. The delete has to walk the set
    in statements small enough for the connection to survive.
    """
    import scripts.maintenance.purge_duplicate_movement_chunks as module
    from src.models.rag_chunk import RagChunk

    session = _session()
    rows = [
        DuplicateRow(
            chunk_id=index + 1,
            source_row_id=f"id={index + 1}",
            index_status="ACTIVE",
            natural_key=f"2024-07-18_KIA_x_부상자 명단_{index:016x}",
            legacy_note="",
        )
        for index in range(module._RETIRE_CHUNK_SIZE * 2 + 100)
    ]
    session.add_all(
        [
            RagChunk(id=row.chunk_id, source_table="player_movements", source_row_id=row.source_row_id, content="x")
            for row in rows
        ],
    )
    session.commit()

    calls: list[dict[str, list[int]]] = []
    monkeypatch.setattr(
        module,
        "_retire_rows",
        lambda session, ids_by_status: (calls.append(ids_by_status), sum(len(v) for v in ids_by_status.values()))[1],
    )

    retired, already_done = module._apply_deletions(session, rows, Path("plan.csv"))

    assert len(calls) == 3
    assert sorted(len(next(iter(call.values()))) for call in calls) == [100, 200, 200]
    assert retired == len(rows)
    assert already_done == 0


def test_every_deleted_row_leaves_an_audit_trail() -> None:
    """The trail is the only record of what a delete removed and why."""
    from sqlalchemy import select

    from src.models.audit_trail import CorrectionAuditTrail
    from src.models.rag_chunk import RagChunk

    session = _session()
    rows = [
        DuplicateRow(
            chunk_id=index + 1,
            source_row_id=f"id={index + 1}",
            index_status="ACTIVE",
            natural_key=f"2024-07-18_KIA_x_부상자 명단_{index:016x}",
            legacy_note="",
        )
        for index in range(3)
    ]
    session.add_all(
        [
            RagChunk(id=row.chunk_id, source_table="player_movements", source_row_id=row.source_row_id, content="x")
            for row in rows
        ],
    )
    session.commit()

    retired, _already = _apply_deletions(session, rows, Path("data/repair_backups/plan.csv"))

    audit = list(session.execute(select(CorrectionAuditTrail)).scalars())
    assert retired == 3
    assert len(audit) == 3
    assert all(entry.entity_type == "rag_chunk" for entry in audit)
    assert all(entry.field_name == "row_deleted" for entry in audit)
    assert all(entry.corrected_value.startswith("2024-07-18_KIA_") for entry in audit)
    assert all("plan.csv" in (entry.extra_metadata or {})["backup_file"] for entry in audit)


def test_a_rerun_skips_rows_already_off_the_search_surface() -> None:
    """A dropped connection costs the batch in flight, not the run.

    Every batch commits, so a second attempt sees the first attempt's work as
    ``DELETED``. It must count those and move on -- ``main``'s guard only checks that
    the id still carries the planned ``source_row_id``, so refusing here would make
    a dropped connection unrecoverable.
    """
    from src.models.rag_chunk import RagChunk

    session = _session()
    rows = [
        DuplicateRow(
            chunk_id=index + 1,
            source_row_id=f"id={index + 1}",
            index_status="ACTIVE",
            natural_key=f"2024-07-18_KIA_x_부상자 명단_{index:016x}",
            legacy_note="",
        )
        for index in range(3)
    ]
    session.add_all(
        [
            RagChunk(id=row.chunk_id, source_table="player_movements", source_row_id=row.source_row_id, content="x")
            for row in rows
        ],
    )
    session.commit()

    retired, already_done = _apply_deletions(session, rows, Path("plan.csv"))
    assert (retired, already_done) == (3, 0)

    retired2, already_done2 = _apply_deletions(session, rows, Path("plan.csv"))
    assert (retired2, already_done2) == (0, 3)


def test_each_batch_commits_so_a_later_failure_keeps_earlier_work(monkeypatch: pytest.MonkeyPatch) -> None:
    """The point of batching is that a mid-run drop is resumable."""
    from src.models.rag_chunk import RagChunk

    session = _session()
    rows = [
        DuplicateRow(
            chunk_id=index + 1,
            source_row_id=f"id={index + 1}",
            index_status="ACTIVE",
            natural_key=f"2024-07-18_KIA_x_부상자 명단_{index:016x}",
            legacy_note="",
        )
        for index in range(6)
    ]
    session.add_all(
        [
            RagChunk(id=row.chunk_id, source_table="player_movements", source_row_id=row.source_row_id, content="x")
            for row in rows
        ],
    )
    session.commit()

    commits: list[int] = []
    original_commit = session.commit

    def counting_commit() -> None:
        commits.append(1)
        original_commit()

    monkeypatch.setattr(session, "commit", counting_commit)

    _apply_deletions(session, rows, Path("plan.csv"), batch_size=2)

    assert len(commits) == 3


class TestTeamHistoryPlanning:
    """The legacy form labels five fields; the natural key writes one line.

    The first read called the legacy form richer because it lists more fields.
    Comparing values says otherwise -- ``현재 구단명`` is not a source column at all.
    """

    def _add(self, session, *, natural_content: str, legacy_content: str) -> None:
        from src.models.rag_chunk import RagChunk
        from src.models.team_history import TeamHistory

        session.add(
            TeamHistory(
                franchise_id=1,
                season=2026,
                team_name="삼성 라이온즈",
                team_code="SS",
                ranking=1,
                stadium="대구 삼성 라이온즈 파크",
                city="대구",
            ),
        )
        session.commit()
        session.add_all(
            [
                RagChunk(
                    source_table="team_history",
                    source_row_id="2026_SS",
                    content=natural_content,
                    index_status="ACTIVE",
                ),
                RagChunk(
                    source_table="team_history",
                    source_row_id="id=311",
                    content=legacy_content,
                    index_status="ACTIVE",
                ),
            ],
        )
        session.commit()

    def test_a_row_whose_values_all_survive_is_confirmed(self) -> None:
        from scripts.maintenance.purge_duplicate_movement_chunks import plan_team_history_duplicates

        session = _session()
        self._add(
            session,
            natural_content="2026년 삼성 라이온즈 (SS): 1위, 구장: 대구 삼성 라이온즈 파크, 도시: 대구",
            legacy_content=(
                "KBO 구단 변천사\n구단: 삼성 라이온즈\n시즌: 2026\n순위: 1\n도시: 대구\n"
                "주경기장: 대구 삼성 라이온즈 파크\n\n현재 구단명: 삼성 라이온즈"
            ),
        )

        confirmed, rejected = plan_team_history_duplicates(session)

        assert rejected == []
        assert [row.source_row_id for row in confirmed] == ["id=311"]
        assert confirmed[0].natural_key == "2026_SS"

    def test_a_value_missing_from_the_natural_key_is_refused(self) -> None:
        from scripts.maintenance.purge_duplicate_movement_chunks import plan_team_history_duplicates

        session = _session()
        self._add(
            session,
            natural_content="2026년 삼성 라이온즈 (SS): 1위, 구장: 불명, 도시: 대구",
            legacy_content=(
                "KBO 구단 변천사\n구단: 삼성 라이온즈\n시즌: 2026\n순위: 1\n도시: 대구\n"
                "주경기장: 대구 삼성 라이온즈 파크\n\n현재 구단명: 삼성 라이온즈"
            ),
        )

        confirmed, rejected = plan_team_history_duplicates(session)

        assert confirmed == []
        assert len(rejected) == 1
        assert "주경기장" in rejected[0]
