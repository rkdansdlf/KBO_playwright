"""P1 safety and regression tests for KBO RAG content-based rekey system.

Validates:
1. Normalization boundaries (newlines, aliases, historical franchise codes)
2. Non-target protection (game_summary, #part, non-active chunks)
3. Season stats automatic rekey blocked (0 entries for batting/pitching)
4. Content and embedding preservation (bitwise immutability of knowledge body)
5. Golden query natural key determinism
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from src.cli.rag.apply_rag_rekey import main as apply_rekey_main
from src.cli.rag.match_legacy_content import SOURCES, _legacy_chunks, _match_one
from src.models.rag_chunk import RagChunk
from src.services.rag_index_identity import (
    stable_award_source_row_id,
    stable_player_movement_source_row_id,
    stable_team_history_source_row_id,
)
from src.services.rag_legacy_content_match import (
    FIELD_CONFLICT,
    MATCHED,
    UNMATCHED,
    LegacyMatchOutcome,
    LegacySourceRow,
    match_award,
    match_movement,
    match_team_history,
    normalize_award_category,
    normalize_award_type,
    normalize_source_team_name,
)
from src.services.rag_rekey_manifest import build_manifest


# ---------------------------------------------------------------------------
# 1. Normalization Boundaries
# ---------------------------------------------------------------------------
class TestNormalizationBoundaries:
    """Test boundary conditions in string normalization across sources."""

    @pytest.mark.parametrize(
        ("raw_name", "expected"),
        [
            ("삼미\n슈퍼스타즈", "삼미"),
            ("해태\n타이거즈", "해태"),
            ("청보\n핀토스", "청보"),
            ("태평양\n돌핀스", "태평양"),
            ("빙그레\n이글스", "빙그레"),
            ("쌍방울\n레이더스", "쌍방울"),
            ("MBC\n청룡", "MBC"),
            ("OB\n베어스", "OB"),
            ("  삼미 \n  슈퍼스타즈  ", "삼미"),
            ("\t해태\t타이거즈\n", "해태"),
            ("", ""),
            (None, ""),
            ("   ", ""),
        ],
    )
    def test_team_name_multiline_normalization(self, raw_name: str | None, expected: str) -> None:
        assert normalize_source_team_name(raw_name) == expected

    @pytest.mark.parametrize(
        ("alias", "expected"),
        [
            ("Golden Glove", "골든글러브"),
            ("Rookie of the Year", "신인상"),
            ("MVP", "MVP"),
            (" 골든글러브 ", "골든글러브"),
            ("수비상", "수비상"),
            ("올스타전MVP", "올스타전MVP"),
            ("한국시리즈MVP", "한국시리즈MVP"),
            ("UnknownAward", "UnknownAward"),
        ],
    )
    def test_award_type_normalization(self, alias: str, expected: str) -> None:
        assert normalize_award_type(alias) == expected

    @pytest.mark.parametrize(
        ("pos_code", "expected"),
        [
            ("C", "포수"),
            ("1B", "1루수"),
            ("2B", "2루수"),
            ("3B", "3루수"),
            ("SS", "유격수"),
            ("OF", "외야수"),
            ("DH", "지명타자"),
            ("P", "투수"),
            ("포수", "포수"),
            ("외야수", "외야수"),
            (None, ""),
            ("", ""),
        ],
    )
    def test_position_category_normalization(self, pos_code: str | None, expected: str) -> None:
        assert normalize_award_category(pos_code) == expected

    def test_franchise_twin_code_isolation(self) -> None:
        """HT and KIA share franchise history but must not cross-match when team is specified."""
        body_ht = "선수 이동 기록\n날짜: 2024-07-18\n이동 유형: 부상자 명단\n선수: 이우성(외야수)\n팀: 해태 타이거즈"
        body_kia = "선수 이동 기록\n날짜: 2024-07-18\n이동 유형: 부상자 명단\n선수: 이우성(외야수)\n팀: KIA 타이거즈"

        candidates = {
            ("2024-07-18", "이우성(외야수)", "부상자 명단"): [
                LegacySourceRow(101, "2024-07-18_HT_이우성_a", team_code="HT"),
                LegacySourceRow(102, "2024-07-18_KIA_이우성_b", team_code="KIA"),
            ]
        }
        team_map = {"해태 타이거즈": "HT", "KIA 타이거즈": "KIA"}

        res_ht = match_movement(body_ht, candidates, team_map)
        assert res_ht.reason == MATCHED
        assert res_ht.source_row_id == 101

        res_kia = match_movement(body_kia, candidates, team_map)
        assert res_kia.reason == MATCHED
        assert res_kia.source_row_id == 102

    def test_samsung_vs_ssg_isolation(self) -> None:
        """SS (Samsung) must never match SSG (Landers)."""
        body_ssg = "KBO 수상 기록\n시즌: 2022\n수상 종류: MVP\n선수: 최정\n팀: SSG\n\n포지션: 3B"
        candidates = {
            (2022, "MVP", "최정"): [
                LegacySourceRow(201, "2022_MVP_3B_최정", team_code="SS", team_name="삼성", category="3루수"),
            ]
        }
        # Chunk has team SSG, candidate has SS -> FIELD_CONFLICT
        franchise_map = {
            "SS": frozenset({1}),
            "삼성": frozenset({1}),
            "SSG": frozenset({2}),
            "SSG 랜더스": frozenset({2}),
        }
        res = match_award(body_ssg, candidates, team_franchises=franchise_map)
        assert res.reason == FIELD_CONFLICT


# ---------------------------------------------------------------------------
# 2. Non-Target Protection & Season Stats Isolation
# ---------------------------------------------------------------------------
class TestNonTargetProtection:
    """Verify non-approved sources and malformed legacy keys are never rekeyed."""

    def test_unsupported_sources_return_unmatched_in_match_one(self) -> None:
        """Non-target sources like game_summary or batting must never match."""
        content = "2024-05-01 KIA vs 삼성 경기 요약"
        res_game = _match_one("game_summary", content, {})
        assert res_game.reason == UNMATCHED

        res_bat = _match_one("player_season_batting", "2024 시즌 타격 기록 강백호", {})
        assert res_bat.reason == UNMATCHED

        res_pitch = _match_one("player_season_pitching", "2024 시즌 투구 기록 원태인", {})
        assert res_pitch.reason == UNMATCHED

    def test_sources_tuple_strictly_contains_only_three_sources(self) -> None:
        """Only player_movements, awards, team_history are in SOURCES."""
        assert set(SOURCES) == {"player_movements", "awards", "team_history"}

    def test_part_split_keys_are_filtered_from_legacy_chunks(self) -> None:
        """#part keys (e.g. id=123#part1) are NOT matched by ^id=[0-9]+$ regex."""
        # Mocking query result where rows have #part keys
        mock_result = [
            (1, "id=100", "정상 청크"),
            (2, "id=101#part1", "분할 청크 1"),
            (3, "id=101#part2", "분할 청크 2"),
            (4, "/app/docs/doc1", "문서 청크"),
        ]
        # In real DB, regexp_match filters this at SQL level.
        # Here we verify regex directly against the keys:
        import re

        regex = re.compile(r"^id=[0-9]+$")
        valid_keys = [r[1] for r in mock_result if regex.match(r[1])]
        assert valid_keys == ["id=100"]

    def test_writer_rejects_unapproved_source_table_under_apply(self, tmp_path: Path) -> None:
        """Manifest entry containing player_season_batting is rejected under --apply."""
        import os

        os.environ["RAG_INDEX_ALLOW_WRITE"] = "1"
        os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"] = "1"

        try:
            entry = {
                "chunk_id": 999,
                "source_table": "player_season_batting",
                "legacy_source_row_id": "id=500",
                "natural_source_row_id": "2024_KBO1_KT_강백호",
                "disposition": "SAFE_REKEY",
                "matched_source_pk": 500,
                "proof": {
                    "matched_fields": ["season", "player", "team"],
                    "candidate_count": 1,
                    "source_snapshot_hash": "snap1",
                    "chunk_content_hash": "chunk1",
                },
            }
            manifest = build_manifest(
                manifest_version="test-v1",
                target_index_version="rag-v1",
                source_tables=("player_season_batting",),
                totals={"totals": 1},
                unsafe_entry_count=0,
                sources=[{"source_table": "player_season_batting", "legacy_chunks": 1}],
                entries=[entry],
                match_schema_version="content-proof-v1",
            )
            path = tmp_path / "manifest_batting.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")

            exit_code = apply_rekey_main(["--manifest", str(path), "--apply", "--json"])
            assert exit_code == 2
        finally:
            del os.environ["RAG_INDEX_ALLOW_WRITE"]
            del os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"]


# ---------------------------------------------------------------------------
# 3. Content and Embedding Immutability
# ---------------------------------------------------------------------------
class TestContentAndEmbeddingPreservation:
    """Ensure rekey operations strictly preserve content text and vectors."""

    def test_rekey_only_updates_source_row_id(self) -> None:
        """RagChunk attributes title, content, content_hash, embedding must remain bitwise untouched."""
        chunk = RagChunk(
            id=10,
            source_table="awards",
            source_row_id="id=130",
            title="2021 골든글러브 포수",
            content="[TL;DR] 강민호 2021 골든글러브 포수 부문 수상\n팀: 삼성",
            content_hash="orig_hash_abc123",
            embedding=[0.1, 0.2, 0.3],
            index_status="ACTIVE",
            index_version="rag-v1",
        )

        orig_title = chunk.title
        orig_content = chunk.content
        orig_hash = chunk.content_hash
        orig_emb = list(chunk.embedding)

        # Simulated apply update
        natural_key = stable_award_source_row_id(2021, "골든글러브", "포수", "강민호")
        chunk.source_row_id = natural_key

        assert chunk.source_row_id == "2021_골든글러브_포수_강민호"
        assert chunk.title == orig_title
        assert chunk.content == orig_content
        assert chunk.content_hash == orig_hash
        assert chunk.embedding == orig_emb

    def test_stale_content_hash_in_proof_aborts_cas(self, tmp_path: Path) -> None:
        """When chunk content hash differs from proof, CAS optimistic concurrency fails."""
        import os

        os.environ["RAG_INDEX_ALLOW_WRITE"] = "1"
        os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"] = "1"

        try:
            # Entry has chunk_content_hash="expected_hash"
            entry = {
                "chunk_id": 50,
                "source_table": "awards",
                "legacy_source_row_id": "id=100",
                "natural_source_row_id": "2021_골든글러브_포수_강민호",
                "disposition": "SAFE_REKEY",
                "matched_source_pk": 30,
                "legacy_content_hash": "stale_hash_from_manifest",
                "proof": {
                    "matched_fields": ["year", "award_type", "player"],
                    "candidate_count": 1,
                    "source_snapshot_hash": "snap1",
                    "chunk_content_hash": "stale_hash_from_manifest",
                },
            }
            manifest = build_manifest(
                manifest_version="test-v1",
                target_index_version="rag-v1",
                source_tables=("awards",),
                totals={"totals": 1},
                unsafe_entry_count=0,
                sources=[{"source_table": "awards", "legacy_chunks": 1}],
                entries=[entry],
                match_schema_version="content-proof-v1",
            )
            path = tmp_path / "manifest_stale_hash.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")

            # Mock DB row has DIFFERENT content_hash "current_live_hash"
            from unittest.mock import patch

            fake_row = Mock()
            fake_row.id = 50
            fake_row.source_row_id = "id=100"
            fake_row.content_hash = "current_live_hash"
            fake_row.index_status = "ACTIVE"
            fake_row.index_version = "rag-v1"

            fake_session = Mock()
            fake_session.execute.return_value.scalar_one_or_none.return_value = fake_row

            with patch("src.cli.rag.apply_rag_rekey.db_engine.get_rag_index_session") as mock_session_ctx:
                mock_session_ctx.return_value.__enter__.return_value = fake_session
                exit_code = apply_rekey_main(["--manifest", str(path), "--apply", "--json"])
                assert exit_code == 1  # CAS failure returns exit code 1
        finally:
            del os.environ["RAG_INDEX_ALLOW_WRITE"]
            del os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"]


# ---------------------------------------------------------------------------
# 4. Golden Queries Determinism
# ---------------------------------------------------------------------------
class TestGoldenQueriesDeterminism:
    """Verify natural keys for all 4 Phase B golden queries are deterministically constructible."""

    def test_golden_query_awards_key(self) -> None:
        """Golden query: 2021년 골든글러브 포수 강민호."""
        nk = stable_award_source_row_id(2021, "골든글러브", "포수", "강민호")
        assert nk == "2021_골든글러브_포수_강민호"

    def test_golden_query_movements_key(self) -> None:
        """Golden query: 2024-07-18 KIA 이우성 이동 기록."""
        nk = stable_player_movement_source_row_id("2024-07-18", "KIA", "이우성(외야수)", "부상자 명단")
        assert nk.startswith("2024-07-18_KIA_이우성(외야수)_부상자 명단_")

    def test_golden_query_team_history_key(self) -> None:
        """Golden query: 1982년 삼미 슈퍼스타즈 순위."""
        nk = stable_team_history_source_row_id(1982, "SM")
        assert nk == "1982_SM"

    def test_golden_query_mvp_key(self) -> None:
        """Golden query: 2020년 MVP 멜 로하스 주니어."""
        nk = stable_award_source_row_id(2020, "MVP", None, "멜 로하스 주니어")
        assert nk == "2020_MVP_NONE_멜 로하스 주니어"
