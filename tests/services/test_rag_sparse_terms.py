"""Tests for normalized Oracle sparse term postings."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from src.models.base import Base
from src.models.rag_chunk import RagChunk
from src.models.rag_chunk_term import RagChunkTerm
from src.services.rag_sparse_terms import build_term_rows, normalize_sparse_token, search_keywords


def test_search_keywords_preserves_existing_query_contract() -> None:
    """Remove Korean particles without changing query token case."""
    assert search_keywords("김도영의 2026시즌 KT와") == ["김도영", "2026시즌", "KT"]


def test_search_keywords_strips_attached_punctuation() -> None:
    """Match document-side tokenization when punctuation trails a keyword."""
    assert search_keywords("KBO 비디오 판독 신청 규정은?") == ["KBO", "비디오", "판독", "신청", "규정"]


def test_normalize_sparse_token_is_case_insensitive_and_particle_aware() -> None:
    """Normalize document terms with the same particle rules as queries."""
    assert normalize_sparse_token("OPS") == "ops"
    assert normalize_sparse_token("선수의") == "선수"
    assert normalize_sparse_token("a") is None


def test_build_term_rows_counts_title_and_game_date() -> None:
    """Build one row per term with title weighting metadata."""
    rows = build_term_rows(
        7,
        "김도영 선수",
        "김도영 선수 OPS OPS",
        {"game_date": "2026-08-21"},
        source_table="player_basic",
    )

    by_token = {row["token"]: row for row in rows}
    assert by_token["김도영"] == {
        "rag_chunk_id": 7,
        "source_table": "player_basic",
        "token": "김도영",
        "term_count": 2,
        "title_count": 1,
        "game_date": "2026-08-21",
    }
    assert by_token["ops"]["term_count"] == 2


def test_build_sparse_terms_rebuilds_sqlite_postings() -> None:
    """Exercise the dry-run/apply builder against the portable ORM schema."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine, tables=[RagChunk.__table__, RagChunkTerm.__table__])
    with Session(engine) as session:
        session.add(
            RagChunk(
                source_table="teams",
                source_row_id="KIA",
                title="KIA 홈구장",
                content="광주 경기장",
                meta={"game_date": "2026-08-21"},
                index_status="ACTIVE",
            ),
        )
        session.commit()

        from src.cli.rag.build_oracle_sparse_index import build_sparse_terms

        dry_run = build_sparse_terms(session, apply=False, rebuild=False, batch_size=1)
        assert dry_run.dry_run is True
        assert dry_run.term_rows > 0
        assert session.execute(select(RagChunkTerm)).scalars().all() == []

        report = build_sparse_terms(session, apply=True, rebuild=True, batch_size=1)
        assert report.term_rows == dry_run.term_rows
        assert session.execute(select(RagChunkTerm)).scalars().all()


def test_max_posted_chunk_id_returns_scalar() -> None:
    """Return the highest indexed chunk ID for catch-up resumes."""
    from src.cli.rag.build_oracle_sparse_index import max_posted_chunk_id

    session = MagicMock()
    session.scalar.return_value = 42

    assert max_posted_chunk_id(session) == 42


def test_main_catch_up_resolves_after_id_from_max_posted() -> None:
    """Resolve the resume point automatically when --catch-up is supplied."""
    from src.cli.rag.build_oracle_sparse_index import SparseTermBuildReport, main

    report = SparseTermBuildReport(0, 0, 0, dry_run=True, rebuilt=False)
    captured: dict[str, int] = {}
    session = MagicMock()
    session.scalar.return_value = 100

    def fake_build(_session, options):
        captured["after_id"] = options.after_id
        return report

    with (
        patch("src.cli.rag.build_oracle_sparse_index.get_rag_index_session") as index_session,
        patch("src.cli.rag.build_oracle_sparse_index._build_sparse_terms", side_effect=fake_build),
    ):
        index_session.return_value.__enter__.return_value = session
        assert main(["--dry-run", "--catch-up", "--json"]) == 0

    assert captured["after_id"] == 100


def test_main_catch_up_rejects_after_id_and_rebuild(capsys) -> None:
    """Keep catch-up mutually exclusive with manual resume and rebuild flags."""
    from src.cli.rag.build_oracle_sparse_index import main

    assert main(["--dry-run", "--catch-up", "--after-id", "5"]) == 2
    assert "cannot be combined with --after-id" in capsys.readouterr().err

    assert main(["--dry-run", "--catch-up", "--rebuild"]) == 2
    assert "cannot be combined with --rebuild" in capsys.readouterr().err
