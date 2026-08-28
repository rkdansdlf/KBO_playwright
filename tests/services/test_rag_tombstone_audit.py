"""Tests for read-only RAG tombstone identity classification."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.models.rag_chunk import RagChunk
from src.services.rag_tombstone_audit import audit_tombstone_session, classify_tombstone_identity_rekeys


def test_classify_tombstones_recognizes_legacy_team_code_rekeys() -> None:
    """Recognize active canonical replacements for deleted season-stat identities."""
    report = classify_tombstone_identity_rekeys(
        [
            "player_season_batting:100_2001_HT_REGULAR",
            "player_season_pitching:200_2001_SK_REGULAR",
        ],
        [
            "player_season_batting:100_2001_KIA_REGULAR",
            "player_season_pitching:200_2001_SSG_REGULAR",
        ],
    )

    assert report.classification == "EXPECTED_IDENTITY_REKEY"
    assert report.is_consistent
    assert report.expected_rekey_keys == (
        "player_season_batting:100_2001_HT_REGULAR",
        "player_season_pitching:200_2001_SK_REGULAR",
    )


def test_classify_tombstones_keeps_unexplained_rows_visible() -> None:
    """Do not silently classify malformed or missing replacement identities."""
    report = classify_tombstone_identity_rekeys(
        [
            "game:g1",
            "player_season_batting:100_2001_HT_REGULAR",
        ],
        ["player_season_batting:100_2001_KIA_REGULAR"],
    )

    assert report.classification == "MIXED"
    assert not report.is_consistent
    assert report.unexplained_keys == ("game:g1",)


def test_classify_tombstones_reports_empty_indexes_cleanly() -> None:
    """Return an explicit no-deleted-rows classification for an empty input."""
    report = classify_tombstone_identity_rekeys([], [])

    assert report.classification == "NO_DELETED_ROWS"
    assert report.is_consistent
    assert report.to_dict()["deleted"] == 0


def test_audit_tombstone_session_reads_deleted_and_active_identities() -> None:
    """Compare persisted tombstones with active canonical rows without mutation."""
    engine = create_engine("sqlite:///:memory:")
    RagChunk.__table__.create(engine)
    session = Session(engine)
    session.add_all(
        [
            RagChunk(
                source_table="player_season_pitching",
                source_row_id="100_2001_HT_REGULAR",
                content="legacy",
                index_status="DELETED",
            ),
            RagChunk(
                source_table="player_season_pitching",
                source_row_id="100_2001_KIA_REGULAR",
                content="canonical",
                index_status="ACTIVE",
            ),
        ]
    )
    session.commit()

    report = audit_tombstone_session(session)

    assert report.classification == "EXPECTED_IDENTITY_REKEY"
    assert session.query(RagChunk).count() == 2
    session.close()
    engine.dispose()
