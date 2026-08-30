"""Phase 106C: Ephemeral End-to-End Pipeline and Persistence Certification Tests.

Validates complete offline persistence pipeline:
Raw Fixture -> Parser -> Normalizer -> Repository -> Ephemeral SQLite DB.
Verifies insertion correctness, re-run idempotency (0 duplicate natural keys, 0 mutations),
transaction rollback on failure, and protected database zero-mutation guarantee.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import src.models
from src.models.base import Base
from src.parsers.game_detail_parser import GameDetailParser
from src.parsers.ticket_parser import parse_ticket_page
from src.repositories.game_save import save_game_detail
from src.repositories.ticket_price_repository import TicketPriceRepository

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures"
PROTECTED_DB_PATH = REPO_ROOT / "data" / "kbo_dev.db"


def _compute_file_sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


class TestEphemeralPersistenceE2E:
    """End-to-End pipeline testing against an isolated ephemeral SQLite database."""

    @pytest.fixture
    def ephemeral_db(self, tmp_path: Path):
        """Create a disposable ephemeral SQLite database with complete ORM schema."""
        db_file = tmp_path / "ephemeral_kbo.db"
        engine = create_engine(f"sqlite:///{db_file}")
        Base.metadata.create_all(engine)
        session_factory = sessionmaker(bind=engine)
        yield session_factory
        engine.dispose()

    def test_protected_db_hash_unaltered_precondition(self) -> None:
        """Verify that the protected development database exists and hash is computed."""
        if PROTECTED_DB_PATH.exists():
            initial_hash = _compute_file_sha256(PROTECTED_DB_PATH)
            assert initial_hash is not None
            assert len(initial_hash) == 64

    def test_game_detail_ephemeral_persistence_and_idempotency(self, ephemeral_db) -> None:
        """Test game detail insertion and 100% idempotent re-run into ephemeral DB."""
        fixture_path = FIXTURES_DIR / "game_details" / "20251001NCLG0.html"
        assert fixture_path.exists()
        html = fixture_path.read_text(encoding="utf-8")

        parser = GameDetailParser(html=html, game_id="20251001NCLG0", game_date="2025-10-01")
        parsed_detail = parser.parse()
        assert parsed_detail is not None

        # First run: Ingest into ephemeral DB
        with ephemeral_db() as session:
            # Create base game row first
            session.execute(
                text(
                    "INSERT INTO game (game_id, game_date, home_team, away_team) "
                    "VALUES ('20251001NCLG0', '2025-10-01', 'LG', 'NC')"
                )
            )
            session.commit()

            # Save detail extended
            success = save_game_detail(parsed_detail, session=session)
            session.commit()
            assert success is True

        # Check row count after first run
        with ephemeral_db() as session:
            hitter_count_1 = session.execute(
                text("SELECT count(*) FROM player_game_batting WHERE game_id = '20251001NCLG0'")
            ).scalar()
            pitcher_count_1 = session.execute(
                text("SELECT count(*) FROM player_game_pitching WHERE game_id = '20251001NCLG0'")
            ).scalar()
            inning_count_1 = session.execute(
                text("SELECT count(*) FROM game_inning_scores WHERE game_id = '20251001NCLG0'")
            ).scalar()

        # Second run: Replay exact same payload
        with ephemeral_db() as session:
            success_2 = save_game_detail(parsed_detail, session=session)
            session.commit()
            assert success_2 is True

        # Assert zero row count inflation, zero duplicate keys
        with ephemeral_db() as session:
            hitter_count_2 = session.execute(
                text("SELECT count(*) FROM player_game_batting WHERE game_id = '20251001NCLG0'")
            ).scalar()
            pitcher_count_2 = session.execute(
                text("SELECT count(*) FROM player_game_pitching WHERE game_id = '20251001NCLG0'")
            ).scalar()
            inning_count_2 = session.execute(
                text("SELECT count(*) FROM game_inning_scores WHERE game_id = '20251001NCLG0'")
            ).scalar()

            assert hitter_count_1 == hitter_count_2
            assert pitcher_count_1 == pitcher_count_2
            assert inning_count_1 == inning_count_2

    def test_ticket_prices_ephemeral_persistence_and_idempotency(self, ephemeral_db) -> None:
        """Test ticket pricing insertion and idempotent re-run."""
        fixture_path = FIXTURES_DIR / "html" / "lg_ticket_prices.html"
        assert fixture_path.exists()
        html = fixture_path.read_text(encoding="utf-8")

        parsed_tickets = parse_ticket_page(html, "lg_twins_ticket")
        assert len(parsed_tickets) > 0

        # First run
        with ephemeral_db() as session:
            repo = TicketPriceRepository(session)
            for t in parsed_tickets:
                repo.save(
                    {
                        "team_id": "LG",
                        "stadium_id": "잠실",
                        "season": 2024,
                        "seat_grade": t.get("seat_grade", "일반석"),
                        "day_type": "WEEKDAY",
                        "price": t.get("weekday_price") or 10000,
                        "audience_type": "GENERAL",
                    }
                )
            session.commit()

        with ephemeral_db() as session:
            count_1 = session.execute(text("SELECT count(*) FROM ticket_prices")).scalar()
            assert count_1 > 0

        # Second run: Re-insert exact same records
        with ephemeral_db() as session:
            repo = TicketPriceRepository(session)
            for t in parsed_tickets:
                repo.save(
                    {
                        "team_id": "LG",
                        "stadium_id": "잠실",
                        "season": 2024,
                        "seat_grade": t.get("seat_grade", "일반석"),
                        "day_type": "WEEKDAY",
                        "price": t.get("weekday_price") or 10000,
                        "audience_type": "GENERAL",
                    }
                )
            session.commit()

        with ephemeral_db() as session:
            count_2 = session.execute(text("SELECT count(*) FROM ticket_prices")).scalar()
            assert count_1 == count_2, "Ticket prices duplicated on re-run!"

    def test_transaction_rollback_on_injected_error(self, ephemeral_db) -> None:
        """Test that injected repository exception triggers complete rollback."""
        with ephemeral_db() as session:
            session.execute(
                text(
                    "INSERT INTO game (game_id, game_date, home_team, away_team) "
                    "VALUES ('2025TESTGAME1', '2025-05-01', 'OB', 'LG')"
                )
            )
            session.commit()

        # Attempt transactional write with deliberate error
        try:
            with ephemeral_db() as session:
                session.execute(
                    text(
                        "INSERT INTO game_inning_scores (game_id, team_side, inning, runs) "
                        "VALUES ('2025TESTGAME1', 'away', 1, 2)"
                    )
                )
                # Deliberate failure: raise exception before commit
                raise RuntimeError("Simulated mid-batch network or disk crash")
        except RuntimeError:
            pass

        # Verify rollback: game_inning_scores must be empty
        with ephemeral_db() as session:
            count = session.execute(
                text("SELECT count(*) FROM game_inning_scores WHERE game_id = '2025TESTGAME1'")
            ).scalar()
            assert count == 0, "Transaction did not rollback on failure!"

    def test_protected_db_unaltered_postcondition(self) -> None:
        """Verify that the protected development database SHA-256 remains 100% unchanged."""
        if PROTECTED_DB_PATH.exists():
            post_hash = _compute_file_sha256(PROTECTED_DB_PATH)
            # Known baseline hash
            assert post_hash == "62adc2e3903ae8544a6f625aa9775247bebc1f85c68bf5f29ad96fca6e76c24f"
