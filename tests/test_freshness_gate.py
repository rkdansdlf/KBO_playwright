from __future__ import annotations

from datetime import date, time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.cli.freshness_gate import collect_freshness_issues, evaluate_freshness_gate
from src.models.game import Game, GameEvent, GameInningScore, GameLineup, GameMetadata
from src.utils.game_status import GAME_STATUS_COMPLETED, GAME_STATUS_DRAW


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game.__table__,
        GameMetadata.__table__,
        GameLineup.__table__,
        GameInningScore.__table__,
        GameEvent.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def test_freshness_gate_detects_missing_events_wpa_and_inning_mismatch():
    SessionLocal = _build_session_factory()

    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20250401LGSS0",
                game_date=date(2025, 4, 1),
                away_team="LG",
                home_team="SS",
                away_score=3,
                home_score=2,
                game_status=GAME_STATUS_COMPLETED,
            )
        )
        session.add(GameMetadata(game_id="20250401LGSS0", start_time=time(18, 30)))
        session.add(
            GameLineup(
                game_id="20250401LGSS0",
                team_side="away",
                team_code="LG",
                player_name="홍길동",
                batting_order=1,
                appearance_seq=1,
                standard_position="CF",
            )
        )
        session.add(
            GameInningScore(
                game_id="20250401LGSS0",
                team_side="away",
                team_code="LG",
                inning=1,
                runs=3,
            )
        )
        session.add(
            GameInningScore(
                game_id="20250401LGSS0",
                team_side="home",
                team_code="SS",
                inning=1,
                runs=2,
            )
        )

        session.add(
            Game(
                game_id="20250401KTNC0",
                game_date=date(2025, 4, 1),
                away_team="KT",
                home_team="NC",
                away_score=1,
                home_score=1,
                game_status=GAME_STATUS_DRAW,
            )
        )
        session.add(GameMetadata(game_id="20250401KTNC0", start_time=time(18, 30)))
        session.add(
            GameLineup(
                game_id="20250401KTNC0",
                team_side="away",
                team_code="KT",
                player_name="강백호",
                batting_order=1,
                appearance_seq=1,
                standard_position="LF",
            )
        )
        session.add(
            GameInningScore(
                game_id="20250401KTNC0",
                team_side="away",
                team_code="KT",
                inning=1,
                runs=0,
            )
        )
        session.add(
            GameInningScore(
                game_id="20250401KTNC0",
                team_side="home",
                team_code="NC",
                inning=1,
                runs=1,
            )
        )
        session.add(
            GameEvent(
                game_id="20250401KTNC0",
                event_seq=1,
                inning=1,
                inning_half="top",
                outs=0,
                batter_name="강백호",
                pitcher_name="구창모",
                description="볼넷",
                event_type="batting",
                bases_before="---",
                bases_after="1--",
                score_diff=0,
                base_state=0,
                home_score=0,
                away_score=0,
            )
        )
        session.commit()

        issues = collect_freshness_issues(session, target_date="20250401")
        failures = evaluate_freshness_gate(session, target_date="20250401")

    assert issues["missing_events"] == ["20250401LGSS0"]
    assert issues["missing_wpa"] == ["20250401KTNC0"]
    assert issues["inning_score_mismatch"] == ["20250401KTNC0"]
    assert any("missing_events" in failure for failure in failures)
    assert any("missing_wpa" in failure for failure in failures)
    assert any("inning_score_mismatch" in failure for failure in failures)
