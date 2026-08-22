"""scripts/historical/apply_season_to_db.py 검증.

커버 범위:
  - game_id 표준화 (원정+홈, MBC→MB 세그먼트) 규칙
  - 승리팀/승리 스코어 산출 (무승부 → None)
  - plan_rows가 season_id/status/franchise_id를 올바르게 채우는지
  - dry-run은 어떤 DB도 쓰지 않는지 (apply_year do_apply=False)
  - 실제 --apply 경로는 백업→삭제→적재 순서 보장 (session mock)
"""

from __future__ import annotations

import json

import pytest

from scripts.historical import apply_season_to_db as ap

SAMPLE = [
    {
        "game_id": "19830402OBMBC0",
        "game_date": "1983-04-02",
        "stadium": "잠실",
        "home_team": "OB",
        "away_team": "MBC",
        "home_score": 7,
        "away_score": 0,
    },
    {
        "game_id": "19830403OBMBC0",
        "game_date": "1983-04-03",
        "stadium": "잠실",
        "home_team": "OB",
        "away_team": "MBC",
        "home_score": 4,
        "away_score": 5,
    },
    {
        "game_id": "19830924OBSM0",
        "game_date": "1983-09-24",
        "stadium": "대전",
        "home_team": "OB",
        "away_team": "SM",
        "home_score": 3,
        "away_score": 4,
    },
    {
        "game_id": "19830829MBC0B0",  # 무승부 (0:0)
        "game_date": "1983-08-29",
        "stadium": "잠실",
        "home_team": "MBC",
        "away_team": "OB",
        "home_score": 0,
        "away_score": 0,
    },
]


class TestGameIdStandardization:
    """answer set의 홈+원정·MBC 3자리 → DB 표준 원정+홈·MB 2자리."""

    def test_away_home_order(self) -> None:
        assert ap.build_standard_game_id("19830402", "OB", "MBC") == "19830402MBOB0"

    def test_mbc_segment_shortened(self) -> None:
        assert ap.build_standard_game_id("19830403", "MBC", "OB") == "19830403OBMB0"

    def test_same_team_other_pair(self) -> None:
        assert ap.build_standard_game_id("19830924", "OB", "SM") == "19830924SMOB0"

    def test_dh_suffix_preserved(self) -> None:
        assert ap.build_standard_game_id("19830430", "OB", "MBC", dh="1") == "19830430MBOB1"


class TestComputeWinning:
    """승리팀/승리 스코어 산출 (무승부 → None)."""

    def test_home_wins(self) -> None:
        assert ap.compute_winning(SAMPLE[0]) == ("OB", 7)

    def test_away_wins(self) -> None:
        assert ap.compute_winning(SAMPLE[1]) == ("MBC", 5)

    def test_draw(self) -> None:
        assert ap.compute_winning(SAMPLE[3]) == (None, None)


class TestPlanRows:
    """plan_rows — DB 행 플랜 (season_id/status/franchise)."""

    def _fid(self) -> dict[str, int]:
        return {"OB": 4, "MBC": 3, "SM": 6}

    def test_fields_populated(self) -> None:
        rows = ap.plan_rows(SAMPLE, 1983, self._fid())
        assert len(rows) == len(SAMPLE)
        r = rows[0]
        assert r["game_id"] == "19830402MBOB0"
        assert r["season_id"] == 198300
        assert r["game_status"] == "COMPLETED"
        assert r["is_primary"] is True
        assert r["game_date"].isoformat() == "1983-04-02"

    def test_franchise_ids(self) -> None:
        rows = ap.plan_rows(SAMPLE, 1983, self._fid())
        assert rows[0]["home_franchise_id"] == 4
        assert rows[0]["away_franchise_id"] == 3
        assert rows[2]["winning_franchise_id"] == 6  # SM 승

    def test_draw_franchise_none(self) -> None:
        rows = ap.plan_rows(SAMPLE, 1983, self._fid())
        assert rows[3]["winning_team"] is None
        assert rows[3]["winning_franchise_id"] is None

    def test_unique_game_ids(self) -> None:
        rows = ap.plan_rows(SAMPLE, 1983, self._fid())
        ids = [r["game_id"] for r in rows]
        assert len(ids) == len(set(ids))


class TestApplyYearFlow:
    """apply_year — dry-run 무기록 + do_apply=백업→삭제→적재 순서."""

    def test_dry_run_does_not_write(self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
        calls: list[str] = []
        monkeypatch.setattr(ap, "load_answer_set", lambda year: SAMPLE)
        monkeypatch.setattr(ap, "franchise_map", lambda s, y: {"OB": 4, "MBC": 3, "SM": 6})
        monkeypatch.setattr(ap, "existing_count", lambda s, y: 0)
        monkeypatch.setattr(ap, "anchor_checks", lambda rows, y: True)
        for fn in ("backup_existing", "delete_existing", "insert_rows"):
            monkeypatch.setattr(ap, fn, lambda *a, __f=fn: calls.append(__f) or 0)
        rc = ap.apply_year(object(), 1983, do_apply=False)
        assert rc == 0
        assert calls == []
        out = capsys.readouterr().out
        assert "DRY-RUN" in out

    def test_apply_sequence(self, monkeypatch: pytest.MonkeyPatch, tmp_path: pytest.TempPathFactory) -> None:
        order: list[str] = []
        monkeypatch.setattr(ap, "load_answer_set", lambda year: SAMPLE)
        monkeypatch.setattr(ap, "franchise_map", lambda s, y: {"OB": 4, "MBC": 3, "SM": 6})
        monkeypatch.setattr(ap, "existing_count", lambda s, y: 4)
        monkeypatch.setattr(ap, "anchor_checks", lambda rows, y: True)
        monkeypatch.setattr(
            ap,
            "backup_existing",
            lambda s, y, out: order.append("backup") or 4,
        )
        monkeypatch.setattr(ap, "delete_existing", lambda s, y: order.append("delete") or 4)
        monkeypatch.setattr(ap, "insert_rows", lambda s, rows: order.append("insert") or 4)
        monkeypatch.setattr(ap, "ANSWER_DIR", tmp_path)  # 백업은 mock이 대신 처리
        rc = ap.apply_year(object(), 1983, do_apply=True)
        assert rc == 0
        assert order == ["backup", "delete", "insert"]

    def test_anchor_fail_aborts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(ap, "load_answer_set", lambda year: SAMPLE)
        monkeypatch.setattr(ap, "franchise_map", lambda s, y: {})
        monkeypatch.setattr(ap, "existing_count", lambda s, y: 4)
        monkeypatch.setattr(ap, "anchor_checks", lambda rows, y: False)
        monkeypatch.setattr(ap, "backup_existing", lambda s, y, out: 0)
        monkeypatch.setattr(ap, "delete_existing", lambda s, y: 0)
        monkeypatch.setattr(ap, "insert_rows", lambda s, rows: 0)
        rc = ap.apply_year(object(), 1983, do_apply=True)
        assert rc == 2


class TestBackupSerialization:
    """기존 행 덤프 — datetime/date를 ISO 문자열로 직렬화."""

    def test_iso_serialization(self, tmp_path: pytest.TempPathFactory) -> None:
        from datetime import date, datetime

        class FakeRows:
            def mappings(self):
                return iter(
                    [
                        {
                            "id": 1,
                            "game_date": date(1983, 4, 2),
                            "created_at": datetime(2026, 8, 20, 12, 0, 0),
                            "is_primary": 1,
                        }
                    ]
                )

        class FakeSession:
            def execute(self, stmt, params=None):
                return FakeRows()

        out = tmp_path / "backup.json"
        n = ap.backup_existing(FakeSession(), 1983, out)
        assert n == 1
        payload = json.loads(out.read_text(encoding="utf-8"))
        assert payload == [{"id": 1, "game_date": "1983-04-02", "created_at": "2026-08-20T12:00:00", "is_primary": 1}]
