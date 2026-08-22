"""1983 교정에서 도출된 감지 규칙 검증 (scripts/historical/namu_season_boxscores).

커버하는 규칙:
  - R열 오기 감지: 이닝 라인 합 != R열 (1983-04-30 MBC 문서 사례)
  - 유령 경기 병합: R열 오기 박스 폐기 후 정상 박스 유지
  - 진짜 더블헤더 보존: R열 정상 + 서로 다른 스코어 2건 유지
  - verify 진단: 앵커 불일치 시 매치업/무승부/head-to-head 출력
"""

from __future__ import annotations

import json

import pytest

from scripts.historical import namu_season_boxscores as nsb

TEAMS_1983 = nsb.SEASONS[1983]


def _box_table(
    date_head: str,
    rows: list[list[str]],
) -> str:
    """parse_tables가 이해하는 HTML 표 1개 생성 (date 헤더 + 헤더행 + 데이터행)."""
    body = "".join(f"<tr>{''.join(f'<td>{c}</td>' for c in r)}</tr>" for r in rows)
    return f"<table><tr><td>{date_head}</td></tr>{body}</table>"


INNING_HEADER = ["팀"] + [f"{n}회" for n in range(1, 10)] + ["R"]


def _parse_single(table_html: str) -> list[dict]:
    return nsb.parse_boxes(nsb.parse_tables(table_html), TEAMS_1983, "4월")


class TestRColumnMismatchDetection:
    """R열 오기 감지 — 이닝 라인 합과 R열 값이 다르면 r_mismatch=True."""

    def test_r_column_typo_detected(self) -> None:
        """1983-04-30 MBC 문서 사례: 이닝 합 4인데 R열이 2로 오기."""
        html = _box_table(
            "4월 30일, 서울종합운동장 야구장",
            [
                INNING_HEADER,
                ["해태 타이거즈", "0", "0", "0", "3", "0", "0", "0", "1", "0", "2"],
                ["MBC 청룡", "0", "0", "0", "0", "0", "0", "0", "0", "1", "1"],
            ],
        )
        games = _parse_single(html)
        assert len(games) == 1
        g = games[0]
        assert g["r_mismatch"] is True
        assert g["inning_sum1"] == 4
        assert g["inning_sum2"] == 1
        assert g["score1"] == 2  # R열 값은 원본대로 보존 (파싱 단계에서 판단만)

    def test_consistent_box_not_flagged(self) -> None:
        """해태 문서 사례: R열 4 == 이닝 합 4 → 정상."""
        html = _box_table(
            "4월 30일, 서울종합운동장 야구장",
            [
                INNING_HEADER,
                ["해태 타이거즈", "0", "0", "0", "3", "0", "0", "0", "1", "0", "4"],
                ["MBC 청룡", "0", "0", "0", "0", "0", "0", "0", "0", "1", "1"],
            ],
        )
        g = _parse_single(html)[0]
        assert g["r_mismatch"] is False
        assert g["inning_sum1"] == 4

    def test_walkoff_x_runs_included_in_sum(self) -> None:
        """연장/끝내기 '5X' 셀은 X 제거 후 합산 — R열과 일치해야 정상."""
        html = _box_table(
            "4월 16일, 숭의야구장",
            [
                ["팀", "1회", "2회", "3회", "4회", "5회", "6회", "7회", "8회", "9회", "R"],
                ["롯데 자이언츠", "0", "0", "2", "0", "3", "3", "0", "0", "0", "8"],
                ["MBC 청룡", "0", "0", "0", "1", "0", "0", "0", "0", "3", "4"],
            ],
        )
        g = _parse_single(html)[0]
        assert g["r_mismatch"] is False

    def test_dash_cells_ignored(self) -> None:
        """- 셀(9회말 생략)은 합산에서 제외 — R열과 일치로 취급."""
        html = _box_table(
            "4월 5일, 무등 야구장",
            [
                INNING_HEADER,
                ["삼성 라이온즈", "0", "0", "0", "1", "0", "3", "0", "0", "-", "4"],
                ["해태 타이거즈", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"],
            ],
        )
        g = _parse_single(html)[0]
        assert g["r_mismatch"] is False


class TestGhostGameMerge:
    """유령 경기 감지 — 같은 날짜·매치업에 R열 오기 박스가 섞이면 폐기."""

    def _raw_entries(self) -> list[dict]:
        typo_box = _box_table(
            "4월 30일, 서울종합운동장 야구장",
            [
                INNING_HEADER,
                ["해태 타이거즈", "0", "0", "0", "3", "0", "0", "0", "1", "0", "2"],
                ["MBC 청룡", "0", "0", "0", "0", "0", "0", "0", "0", "1", "1"],
            ],
        )
        correct_box = _box_table(
            "4월 30일, 서울종합운동장 야구장",
            [
                INNING_HEADER,
                ["해태 타이거즈", "0", "0", "0", "3", "0", "0", "0", "1", "0", "4"],
                ["MBC 청룡", "0", "0", "0", "0", "0", "0", "0", "0", "1", "1"],
            ],
        )
        raw = []
        for html, doc in ((typo_box, "MBC"), (correct_box, "HT")):
            g = _parse_single(html)[0]
            g["team_doc"] = doc
            raw.append(g)
        return raw

    def test_typo_box_dropped_healthy_kept(self) -> None:
        raw = self._raw_entries()
        merged = nsb.merge_games(raw)
        assert len(merged) == 1
        g = merged[0]
        assert g["score1"] == 4 and g["score2"] == 1
        assert g["team1"] == "HT" and g["team2"] == "MBC"

    def test_both_healthy_different_scores_kept(self) -> None:
        """R열이 둘 다 정상인데 스코어가 다르면 진짜 더블헤더로 유지."""
        raw = []
        for score in (1, 2):
            html = _box_table(
                "4월 30일, 서울종합운동장 야구장",
                [
                    INNING_HEADER,
                    ["해태 타이거즈", "0", "0", "0", f"{score}", "0", "0", "0", "1", "0", str(score + 1)],
                    ["MBC 청룡", "0", "0", "0", "0", "0", "0", "0", "0", "1", "1"],
                ],
            )
            raw.append(_parse_single(html)[0])
        merged = nsb.merge_games(raw)
        sigs = {(g["score1"], g["score2"]) for g in merged}
        assert len(merged) == 2
        assert len(sigs) == 2

    def test_same_score_dedup(self) -> None:
        """양 팀 문서가 동일 스코어면 1경기로 병합."""
        raw = []
        for _doc in ("HT", "MBC"):
            html = _box_table(
                "4월 16일, 숭의야구장",
                [
                    INNING_HEADER,
                    ["해태 타이거즈", "2", "0", "0", "0", "3", "0", "3", "0", "2", "10"],
                    ["삼미 슈퍼스타즈", "2", "0", "1", "0", "0", "0", "0", "0", "1", "4"],
                ],
            )
            raw.append(_parse_single(html)[0])
        assert len(nsb.merge_games(raw)) == 1


class TestVerifyDiagnostics:
    """verify — 앵커 불일치 진단 출력."""

    def _sample_games(self) -> list[dict]:
        return [
            {"game_date": "1983-09-24", "home_team": "OB", "away_team": "SM", "home_score": 3, "away_score": 4},
            {"game_date": "1983-09-25", "home_team": "OB", "away_team": "SM", "home_score": 6, "away_score": 0},
            {"game_date": "1983-06-03", "home_team": "SS", "away_team": "MBC", "home_score": 7, "away_score": 7},
        ]

    def test_mismatch_prints_diagnostics(self, capsys: pytest.CaptureFixture[str]) -> None:
        anchors = {
            "OB": {"w": 44, "l": 55, "d": 1},
            "SM": {"w": 53, "l": 46, "d": 1},
            "SS": {"w": 46, "l": 49, "d": 4},
            "MBC": {"w": 55, "l": 43, "d": 2},
        }
        ok = nsb.verify(self._sample_games(), 1983, anchors)
        out = capsys.readouterr().out
        assert ok is False
        assert "MISMATCH" in out
        assert "delta W" in out
        assert "head-to-head OB-SM" in out
        assert "DRAW 1983-06-03 SS 7:7 MBC" in out

    def test_match_returns_true(self, capsys: pytest.CaptureFixture[str]) -> None:
        anchors = {
            "OB": {"w": 1, "l": 1, "d": 0},
            "SM": {"w": 1, "l": 1, "d": 0},
            "SS": {"w": 0, "l": 0, "d": 1},
            "MBC": {"w": 0, "l": 0, "d": 1},
        }
        ok = nsb.verify(self._sample_games(), 1983, anchors)
        assert ok is True
        assert "MISMATCH" not in capsys.readouterr().out


class TestScoreFixes:
    """SCORE_FIXES 교정 적용 — 재현 가능한 수동 교정."""

    def _raw_fixed_date(self) -> list[dict]:
        return [
            {
                "date": "09-24",
                "month_doc": "9월",
                "stadium": "한밭종합운동장 야구장",
                "team1": "OB",
                "team2": "SM",
                "score1": 4,
                "score2": 3,
                "team_doc": "OB",
            }
        ]

    def test_fix_applied_by_team_code(self) -> None:
        games = nsb.apply_score_fixes(self._raw_fixed_date(), 1983)
        g = games[0]
        assert (g["score1"], g["score2"]) == (3, 4)

    def test_noop_when_other_year(self) -> None:
        games = nsb.apply_score_fixes(self._raw_fixed_date(), 1984)
        assert (games[0]["score1"], games[0]["score2"]) == (4, 3)

    def test_fix_idempotent(self) -> None:
        once = nsb.apply_score_fixes(self._raw_fixed_date(), 1983)
        twice = nsb.apply_score_fixes(once, 1983)
        assert (twice[0]["score1"], twice[0]["score2"]) == (3, 4)

    def test_unrelated_game_untouched(self) -> None:
        games = [{"date": "09-25", "team1": "OB", "team2": "SM", "score1": 6, "score2": 0}]
        nsb.apply_score_fixes(games, 1983)
        assert (games[0]["score1"], games[0]["score2"]) == (6, 0)


class TestDropBoxFixes:
    """DROP_BOXES 병합 전 박스 폐기 — 같은 날짜에 스코어가 다른 유령 DH."""

    def _two_doc_boxes(self) -> list[dict]:
        return [
            {
                "date": "07-14",
                "month_doc": "7월",
                "stadium": "동대문야구장",
                "team1": "CB",
                "team2": "OB",
                "score1": 7,
                "score2": 5,
                "team_doc": "OB",
            },
            {
                "date": "07-14",
                "month_doc": "7월",
                "stadium": "동대문야구장",
                "team1": "CB",
                "team2": "OB",
                "score1": 7,
                "score2": 6,
                "team_doc": "CB",
            },
        ]

    def test_drops_registered_box_leaves_one(self) -> None:
        kept = nsb.drop_box_fixes(self._two_doc_boxes(), 1985)
        assert len(kept) == 1
        assert kept[0]["team_doc"] == "CB"
        assert kept[0]["score2"] == 6

    def test_noop_for_other_year(self) -> None:
        kept = nsb.drop_box_fixes(self._two_doc_boxes(), 1984)
        assert len(kept) == 2

    def test_warns_when_target_missing(self, capsys: pytest.CaptureFixture[str]) -> None:
        kept = nsb.drop_box_fixes([self._two_doc_boxes()[1]], 1985)
        assert len(kept) == 1
        assert "WARN drop target" in capsys.readouterr().out

    def test_1985_pipeline_merges_330(self) -> None:
        """DROP_BOXES + SCORE_FIXES 적용 후 1985 answer set이 앵커와 일치해야 한다."""
        raw = json.loads((nsb.RAW_DIR / "1985_namu_raw.json").read_text(encoding="utf-8"))
        games = nsb.merge_games(nsb.drop_box_fixes(raw, 1985))
        final = nsb.finalize_games(nsb.apply_score_fixes(games, 1985), 1985)
        anchors = json.loads(nsb.ANCHORS.read_text(encoding="utf-8"))["1985"]
        assert len(final) == 330
        assert nsb.verify(final, 1985, anchors) is True


class TestPipelineRoundTrip:
    """crawl → 병합 → finalize → verify 왕복 (순수 함수만)."""

    def test_1983_raw_merge_consistency(self, tmp_path: pytest.TempPathFactory) -> None:
        raw = json.loads((nsb.RAW_DIR / "1983_namu_raw.json").read_text(encoding="utf-8"))
        merged = nsb.merge_games(raw)
        final = nsb.finalize_games(nsb.apply_score_fixes(merged, 1983), 1983)
        assert len(final) == len(merged)
        anchors = json.loads(nsb.ANCHORS.read_text(encoding="utf-8"))["1983"]
        old_style = any("r_mismatch" not in g for g in raw)
        if old_style:
            # 규칙 도입 전 raw: 4/30 유령 박스에 플래그가 없어 301경기로
            # 병합되고 verify가 불일치를 감지한다 (재수집 시 300으로 수렴).
            assert len(final) == 301
            assert nsb.verify(final, 1983, anchors) is False
        else:
            assert len(final) == 300
            assert nsb.verify(final, 1983, anchors) is True

    def test_1983_relaunch_merges_300(self) -> None:
        """재수집된(플래그 포함) 파이프라인 원데이터로 300경기 수렴을 보인다.

        parse_boxes가 아직 도입 전인 기존 raw에 대해, 신규 파서가 남길
        r_mismatch 플래그를 4/30 유령(HT 2:1, team_doc=MBC)에만 부여해
        merge_games의 폐기 동작을 엔드투엔드로 확인한다.
        """
        raw = json.loads((nsb.RAW_DIR / "1983_namu_raw.json").read_text(encoding="utf-8"))
        for g in raw:
            if g["date"] == "04-30" and g["team1"] == "HT" and g["team2"] == "MBC":
                g["r_mismatch"] = g["score1"] == 2
        merged = nsb.merge_games(raw)
        final = nsb.finalize_games(nsb.apply_score_fixes(merged, 1983), 1983)
        assert len(final) == 300
        anchors = json.loads(nsb.ANCHORS.read_text(encoding="utf-8"))["1983"]
        assert nsb.verify(final, 1983, anchors) is True
