"""외부 박스스코어 CSV를 검증된 아카이브 payload로 바꾸는 계약.

2001~2007은 저장소가 어떤 경로로도 채울 수 없다. 데이터를 확보하는 일은
저장소 밖(도서관 OCR, 라이선스 덤프, 뉴스 텍스트 마이닝)에서 일어나므로,
저장소가 맡을 부분은 **확보된 데이터를 검증하고 기존 임포터가 받는 형태로
바꾸는 것**이다. 검증이 이 도구의 존재 이유다: 이닝 합계가 최종 점수와
다르거나 game_id가 어긋난 파일은 여기서 멈춰야 한다. 한 번 임포트된
박스스코어는 그 뒤 모든 집계의 근거가 되기 때문이다.
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "maintenance"))

from import_season_boxscores import (
    BoxscoreImportError,
    build_archive_payload,
    build_manifest,
    load_csv_tables,
    write_archive,
)


def _write(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


@pytest.fixture
def tables(tmp_path: Path) -> dict[str, Path]:
    games = tmp_path / "games.csv"
    innings = tmp_path / "innings.csv"
    batting = tmp_path / "batting.csv"
    pitching = tmp_path / "pitching.csv"

    _write(
        games,
        ["game_id", "game_date", "away_team", "home_team", "away_score", "home_score", "stadium", "game_status"],
        [
            {
                "game_id": "20050405OBLT0",
                "game_date": "2005-04-05",
                "away_team": "OB",
                "home_team": "LT",
                "away_score": 3,
                "home_score": 5,
                "stadium": "사직",
                "game_status": "COMPLETED",
            },
        ],
    )
    _write(
        innings,
        ["game_id", "team_side", "inning", "runs"],
        [
            {"game_id": "20050405OBLT0", "team_side": "away", "inning": 1, "runs": 1},
            {"game_id": "20050405OBLT0", "team_side": "away", "inning": 2, "runs": 2},
            {"game_id": "20050405OBLT0", "team_side": "home", "inning": 1, "runs": 5},
        ],
    )
    _write(
        batting,
        [
            "game_id",
            "team_side",
            "player_id",
            "player_name",
            "team_code",
            "appearance_seq",
            "pa",
            "ab",
            "hits",
            "hr",
            "rbi",
            "bb",
        ],
        [
            {
                "game_id": "20050405OBLT0",
                "team_side": "away",
                "player_id": "",
                "player_name": "홍성흔",
                "team_code": "OB",
                "appearance_seq": 4,
                "pa": 4,
                "ab": 4,
                "hits": 2,
                "hr": 0,
                "rbi": 1,
                "bb": 0,
            },
        ],
    )
    _write(
        pitching,
        ["game_id", "team_side", "player_id", "player_name", "team_code", "appearance_seq", "so"],
        [
            {
                "game_id": "20050405OBLT0",
                "team_side": "home",
                "player_id": "",
                "player_name": "박명환",
                "team_code": "LT",
                "appearance_seq": 1,
                "so": 7,
            },
        ],
    )
    return {"games": games, "innings": innings, "batting": batting, "pitching": pitching}


class TestThePayloadMatchesWhatTheIngestorReads:
    def test_the_four_tables_land_under_the_keys_the_ingestor_looks_for(self, tables: dict[str, Path]) -> None:
        payload = build_archive_payload(load_csv_tables(tables), season=2005)

        assert set(payload) == {"games", "game_inning_scores", "game_batting_stats", "game_pitching_stats"}
        assert payload["games"][0]["game_id"] == "20050405OBLT0"
        assert payload["game_inning_scores"][0]["inning"] == 1
        assert payload["game_batting_stats"][0]["hits"] == 2
        assert payload["game_pitching_stats"][0]["so"] == 7

    def test_numbers_are_parsed_not_left_as_text(self, tables: dict[str, Path]) -> None:
        payload = build_archive_payload(load_csv_tables(tables), season=2005)

        assert payload["games"][0]["away_score"] == 3
        assert payload["game_inning_scores"][1]["runs"] == 2
        assert payload["game_batting_stats"][0]["pa"] == 4


class TestTheChecksThatStopABadFile:
    def test_an_inning_total_that_disagrees_with_the_final_score_is_refused(self, tables: dict[str, Path]) -> None:
        """The one check that catches a mis-scanned table before it becomes data."""
        _write(
            tables["innings"],
            ["game_id", "team_side", "inning", "runs"],
            [
                # 2-5 against a 3-5 final score: the table was read wrong somewhere.
                {"game_id": "20050405OBLT0", "team_side": "away", "inning": 1, "runs": 2},
                {"game_id": "20050405OBLT0", "team_side": "home", "inning": 1, "runs": 5},
            ],
        )

        with pytest.raises(BoxscoreImportError, match="inning total"):
            build_archive_payload(load_csv_tables(tables), season=2005)

    def test_a_game_id_outside_the_season_is_refused(self, tables: dict[str, Path]) -> None:
        _write(
            tables["games"],
            ["game_id", "game_date", "away_team", "home_team", "away_score", "home_score", "stadium", "game_status"],
            [
                {
                    "game_id": "20040405OBLT0",
                    "game_date": "2004-04-05",
                    "away_team": "OB",
                    "home_team": "LT",
                    "away_score": 3,
                    "home_score": 5,
                    "stadium": "사직",
                    "game_status": "COMPLETED",
                },
            ],
        )

        with pytest.raises(BoxscoreImportError, match="season"):
            build_archive_payload(load_csv_tables(tables), season=2005)

    def test_the_same_game_twice_is_refused(self, tables: dict[str, Path]) -> None:
        with tables["games"].open(encoding="utf-8") as handle:
            row = next(csv.DictReader(handle))
        _write(
            tables["games"],
            list(row),
            [row, dict(row)],
        )

        with pytest.raises(BoxscoreImportError, match="duplicate"):
            build_archive_payload(load_csv_tables(tables), season=2005)

    def test_a_missing_score_is_refused(self, tables: dict[str, Path]) -> None:
        """An ingest that carries no score cannot be checked, so it is not carried."""
        _write(
            tables["games"],
            ["game_id", "game_date", "away_team", "home_team", "away_score", "home_score", "stadium", "game_status"],
            [
                {
                    "game_id": "20050405OBLT0",
                    "game_date": "2005-04-05",
                    "away_team": "OB",
                    "home_team": "LT",
                    "away_score": "",
                    "home_score": 5,
                    "stadium": "사직",
                    "game_status": "COMPLETED",
                },
            ],
        )

        with pytest.raises(BoxscoreImportError, match="score"):
            build_archive_payload(load_csv_tables(tables), season=2005)

    def test_an_inning_for_an_unknown_game_is_refused(self, tables: dict[str, Path]) -> None:
        """A row whose parent is not in the games table would be an orphan insert."""
        _write(
            tables["innings"],
            ["game_id", "team_side", "inning", "runs"],
            [
                {"game_id": "20050405OBLT0", "team_side": "away", "inning": 1, "runs": 3},
                {"game_id": "20050405OBLT0", "team_side": "home", "inning": 1, "runs": 5},
                {"game_id": "20050406OBLT0", "team_side": "away", "inning": 1, "runs": 0},
            ],
        )

        with pytest.raises(BoxscoreImportError, match="unknown game"):
            build_archive_payload(load_csv_tables(tables), season=2005)

    def test_a_side_that_is_neither_away_nor_home_is_refused(self, tables: dict[str, Path]) -> None:
        _write(
            tables["innings"],
            ["game_id", "team_side", "inning", "runs"],
            [
                {"game_id": "20050405OBLT0", "team_side": "neutral", "inning": 1, "runs": 3},
            ],
        )

        with pytest.raises(BoxscoreImportError, match="team_side"):
            build_archive_payload(load_csv_tables(tables), season=2005)


class TestTheManifestTheIngestorRequires:
    def test_the_manifest_carries_the_digest_of_the_file_it_describes(self, tables: dict[str, Path]) -> None:
        """The digest is what binds a manifest to a file; a wrong one is worse than none."""
        payload = build_archive_payload(load_csv_tables(tables), season=2005)
        archive_path, manifest_path = write_archive(
            payload,
            season=2005,
            out_dir=tables["games"].parent,
            source_name="kbo_yearbook_ocr",
            source_url="https://library.example/yearbook/2005",
            authorization_ref="reading-room-2026-10-27",
        )

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        import hashlib

        digest = hashlib.sha256(archive_path.read_bytes()).hexdigest()
        assert manifest["sha256"] == digest
        assert manifest["season"] == 2005
        assert manifest["source_name"] == "kbo_yearbook_ocr"
        assert manifest["authorization_ref"] == "reading-room-2026-10-27"

    def test_the_placeholder_ref_is_refused(self, tables: dict[str, Path]) -> None:
        """The manifest is a provenance record; "unknown" is not one."""
        with pytest.raises(BoxscoreImportError, match="authorization"):
            build_manifest(
                season=2005,
                digest="0" * 64,
                source_name="kbo_yearbook_ocr",
                source_url="https://library.example/yearbook/2005",
                authorization_ref="unknown",
            )

    def test_a_missing_authorization_is_refused(self, tables: dict[str, Path]) -> None:
        with pytest.raises(BoxscoreImportError, match="authorization"):
            build_manifest(
                season=2005,
                digest="0" * 64,
                source_name="kbo_yearbook_ocr",
                source_url="https://library.example/yearbook/2005",
                authorization_ref="",
            )

    def test_the_written_archive_is_json_the_ingestor_can_load(self, tables: dict[str, Path]) -> None:
        payload = build_archive_payload(load_csv_tables(tables), season=2005)
        archive_path, _ = write_archive(
            payload,
            season=2005,
            out_dir=tables["games"].parent,
            source_name="kbo_yearbook_ocr",
            source_url="https://library.example/yearbook/2005",
            authorization_ref="reading-room-2026-10-27",
        )

        reloaded = json.loads(archive_path.read_text(encoding="utf-8"))
        assert reloaded["games"][0]["game_id"] == "20050405OBLT0"
        assert len(reloaded["game_inning_scores"]) == 3


class TestTheChecksTheIngestorItselfWillRepeat:
    """``ingest_historical_archive`` re-validates; this tool must produce what it accepts."""

    def test_the_manifest_satisfies_the_ingestors_required_fields(self, tables: dict[str, Path]) -> None:
        from src.cli.collection.ingest_historical_archive import MANIFEST_REQUIRED_FIELDS

        payload = build_archive_payload(load_csv_tables(tables), season=2005)
        _, manifest_path = write_archive(
            payload,
            season=2005,
            out_dir=tables["games"].parent,
            source_name="kbo_yearbook_ocr",
            source_url="https://library.example/yearbook/2005",
            authorization_ref="reading-room-2026-10-27",
        )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        assert all(manifest.get(field) for field in MANIFEST_REQUIRED_FIELDS)

    def test_the_payload_keys_are_the_ones_the_ingestor_reads(self, tables: dict[str, Path]) -> None:
        payload = build_archive_payload(load_csv_tables(tables), season=2005)

        # 게임 단위 네 표만 다룬다. 시즌 누적 표는 확보된 원천이 말해 주지 않는다.
        assert "player_season_batting" not in payload
        assert set(payload) <= {
            "games",
            "game_inning_scores",
            "game_batting_stats",
            "game_pitching_stats",
        }
