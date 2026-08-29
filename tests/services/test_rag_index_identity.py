"""Tests for RAG source-identity helper functions."""

from __future__ import annotations

from src.services.rag_index_identity import (
    chunk_content_hash,
    current_index_version,
    normalize_chunk_text,
    stable_award_source_row_id,
    stable_futures_source_row_id,
    stable_highlight_source_row_id,
    stable_milestone_source_row_id,
    PbpSourceIdentity,
    stable_pbp_source_row_id,
    stable_player_movement_source_row_id,
    stable_splits_source_row_id,
    stable_team_history_source_row_id,
)


def test_stable_award_source_row_id_with_category() -> None:
    """Awards with a category include it in the natural key."""
    key = stable_award_source_row_id(2025, "골든글러브", "투수", "원태인")
    assert key == "2025_골든글러브_투수_원태인"


def test_stable_award_source_row_id_without_category() -> None:
    """Awards without a category use an explicit sentinel segment."""
    key = stable_award_source_row_id(2024, "MVP", None, "김도영")
    assert key == "2024_MVP_NONE_김도영"

    key_empty = stable_award_source_row_id(2024, "신인상", "", "김택연")
    assert key_empty == "2024_신인상_NONE_김택연"


def test_stable_award_source_row_id_whitespace_trimming() -> None:
    """Whitespace is stripped from all award key segments."""
    key = stable_award_source_row_id(2023, " 골든글러브 ", " 1루수 ", " 오스틴 ")
    assert key == "2023_골든글러브_1루수_오스틴"


def test_stable_player_movement_source_row_id_is_deterministic() -> None:
    """Movement keys are derived from the source table unique tuple."""
    key = stable_player_movement_source_row_id("2026-08-29", "KIA", "홍길동", "트레이드")
    assert key.startswith("2026-08-29_KIA_홍길동_트레이드_")
    assert key == stable_player_movement_source_row_id("2026-08-29", "KIA", "홍길동", "트레이드")
    assert key != stable_player_movement_source_row_id("2026-08-29", "LG", "홍길동", "트레이드")


def test_stable_pbp_source_row_id_prefers_source_position() -> None:
    """PBP rows use the provider position when it is available."""
    assert (
        stable_pbp_source_row_id(PbpSourceIdentity("20260829KIAHH", 12, 3, "말", "투수", "타자", "안타", "H", "안타"))
        == "20260829KIAHH_12"
    )


def test_stable_pbp_source_row_id_accepts_named_identity_fields() -> None:
    """PBP identity fields can be supplied without positional ambiguity."""
    identity = PbpSourceIdentity(
        game_id="G1",
        source_row_index=None,
        inning=1,
        inning_half="초",
        pitcher_name="P",
        batter_name="B",
        play_description="안타",
        event_type="H",
        result="안타",
    )
    assert stable_pbp_source_row_id(identity).startswith("G1_content_")


def test_stable_pbp_source_row_id_uses_content_fallback() -> None:
    """PBP rows without a source position use a content-derived digest."""
    identity = PbpSourceIdentity("G1", None, 1, "초", "P", "B", "안타", "H", "안타")
    key = stable_pbp_source_row_id(identity)
    assert key.startswith("G1_content_")
    assert key == stable_pbp_source_row_id(identity)
    assert key != stable_pbp_source_row_id(PbpSourceIdentity("G1", None, 1, "초", "P", "B", "삼진", "K", "삼진"))


def test_stable_highlight_source_row_id_handles_summary_rows() -> None:
    """Highlights use event sequence or a description digest for summaries."""
    assert stable_highlight_source_row_id("G1", "CLUTCH", 4) == "G1_CLUTCH_4"
    summary_key = stable_highlight_source_row_id("G1", "CLUTCH", None, "결승타")
    assert summary_key.startswith("G1_CLUTCH_summary_")


def test_stable_team_history_source_row_id() -> None:
    """Team history natural key combines season and team code."""
    assert stable_team_history_source_row_id(1990, "LG") == "1990_LG"
    assert stable_team_history_source_row_id(1982, "OB ") == "1982_OB"


def test_stable_milestone_source_row_id() -> None:
    """Milestone natural key combines season, player_id, and category."""
    assert stable_milestone_source_row_id(2026, 50001, "홈런") == "2026_50001_홈런"
    assert stable_milestone_source_row_id(2026, "50002", "탈삼진") == "2026_50002_탈삼진"
    assert stable_milestone_source_row_id(2026, None, "안타") == "2026_UNKNOWN_안타"


def test_stable_futures_source_row_id() -> None:
    """Futures natural key uses the 13-character standard game_id."""
    assert stable_futures_source_row_id("20260401OBHT0") == "20260401OBHT0"
    assert stable_futures_source_row_id(" 20260401OBHT0 ") == "20260401OBHT0"


def test_stable_splits_source_row_id() -> None:
    """Splits natural key combines season, player_id, split_type, and split_key."""
    key = stable_splits_source_row_id(2026, 50001, "RISP", "득점권")
    assert key == "2026_50001_RISP_득점권"


def test_normalize_chunk_text_and_hash() -> None:
    """Normalized text handles whitespace and hashes consistently."""
    text1 = normalize_chunk_text("Title", "Content with   multiple    spaces\n\nand newlines")
    text2 = normalize_chunk_text("Title", "Content with multiple spaces and newlines")
    assert text1 == text2

    hash1 = chunk_content_hash("Title", "Content with   multiple    spaces\n\nand newlines")
    hash2 = chunk_content_hash("Title", "Content with multiple spaces and newlines")
    assert hash1 == hash2
    assert len(hash1) == 64


def test_current_index_version_default() -> None:
    """Default index version matches the v1 configuration."""
    assert current_index_version() == "rag-v1"
