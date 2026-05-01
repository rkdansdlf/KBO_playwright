from __future__ import annotations

import json
from datetime import date, time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.cli.freshness_gate import collect_freshness_issues, evaluate_freshness_gate
from src.models.game import (
    Game,
    GameEvent,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GameSummary,
)
from src.models.player import PlayerBasic
from src.utils.game_status import GAME_STATUS_COMPLETED, GAME_STATUS_DRAW


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        PlayerBasic.__table__,
        Game.__table__,
        GameMetadata.__table__,
        GameLineup.__table__,
        GameInningScore.__table__,
        GamePitchingStat.__table__,
        GameEvent.__table__,
        GameSummary.__table__,
        GameIdAlias.__table__,
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
                away_pitcher="임찬규",
                home_pitcher="원태인",
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
            GamePitchingStat(
                game_id="20250401LGSS0",
                team_side="away",
                player_name="임찬규",
                is_starting=True,
                appearance_seq=1,
            )
        )
        session.add(
            GamePitchingStat(
                game_id="20250401LGSS0",
                team_side="home",
                player_name="원태인",
                is_starting=True,
                appearance_seq=1,
            )
        )
        session.add(
            GameSummary(
                game_id="20250401LGSS0",
                summary_type="리뷰_WPA",
                detail_text=json.dumps({"crucial_moments": [{"wpa": 0.25}]}, ensure_ascii=False),
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
    assert issues["missing_starting_pitchers"] == ["20250401KTNC0"]
    assert issues["missing_pitching_stats"] == ["20250401KTNC0"]
    assert issues["missing_pitching_starters"] == []
    assert issues["missing_review_wpa"] == ["20250401KTNC0"]
    assert issues["missing_review_moments"] == []
    assert issues["inning_score_mismatch"] == ["20250401KTNC0"]
    assert any("missing_events" in failure for failure in failures)
    assert any("missing_wpa" in failure for failure in failures)
    assert any("missing_starting_pitchers" in failure for failure in failures)
    assert any("missing_pitching_stats" in failure for failure in failures)
    assert any("missing_review_wpa" in failure for failure in failures)
    assert any("inning_score_mismatch" in failure for failure in failures)


def test_freshness_gate_detects_incomplete_pitching_starters_and_empty_review_moments():
    SessionLocal = _build_session_factory()

    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20250402LGSS0",
                game_date=date(2025, 4, 2),
                away_team="LG",
                home_team="SS",
                away_score=4,
                home_score=1,
                away_pitcher="임찬규",
                home_pitcher="원태인",
                game_status=GAME_STATUS_COMPLETED,
            )
        )
        session.add(GameMetadata(game_id="20250402LGSS0", start_time=time(18, 30)))
        session.add(
            GameLineup(
                game_id="20250402LGSS0",
                team_side="away",
                team_code="LG",
                player_name="홍길동",
                batting_order=1,
                appearance_seq=1,
                standard_position="CF",
            )
        )
        session.add(GameInningScore(game_id="20250402LGSS0", team_side="away", inning=1, runs=4))
        session.add(GameInningScore(game_id="20250402LGSS0", team_side="home", inning=1, runs=1))
        session.add(
            GameEvent(
                game_id="20250402LGSS0",
                event_seq=1,
                wpa=0.2,
            )
        )
        session.add(
            GamePitchingStat(
                game_id="20250402LGSS0",
                team_side="away",
                player_name="임찬규",
                is_starting=True,
                appearance_seq=1,
            )
        )
        session.add(
            GameSummary(
                game_id="20250402LGSS0",
                summary_type="리뷰_WPA",
                detail_text=json.dumps({"crucial_moments": []}, ensure_ascii=False),
            )
        )
        session.commit()

        issues = collect_freshness_issues(session, target_date="20250402")

    assert issues["missing_pitching_stats"] == []
    assert issues["missing_pitching_starters"] == ["20250402LGSS0"]
    assert issues["missing_review_wpa"] == []
    assert issues["missing_review_moments"] == ["20250402LGSS0"]


def test_freshness_gate_detects_noise_inside_review_moments():
    SessionLocal = _build_session_factory()

    with SessionLocal() as session:
        session.add(
            Game(
                game_id="20250403LGSS0",
                game_date=date(2025, 4, 3),
                away_team="LG",
                home_team="SS",
                away_score=4,
                home_score=1,
                away_pitcher="임찬규",
                home_pitcher="원태인",
                game_status=GAME_STATUS_COMPLETED,
            )
        )
        session.add(GameMetadata(game_id="20250403LGSS0", start_time=time(18, 30)))
        session.add(
            GameLineup(
                game_id="20250403LGSS0",
                team_side="away",
                team_code="LG",
                player_name="홍길동",
                batting_order=1,
                appearance_seq=1,
                standard_position="CF",
            )
        )
        session.add(GameInningScore(game_id="20250403LGSS0", team_side="away", inning=1, runs=4))
        session.add(GameInningScore(game_id="20250403LGSS0", team_side="home", inning=1, runs=1))
        session.add(
            GameEvent(
                game_id="20250403LGSS0",
                event_seq=1,
                description="홍길동 : 좌전 안타",
                event_type="batting",
                wpa=0.2,
            )
        )
        session.add_all(
            [
                GamePitchingStat(
                    game_id="20250403LGSS0",
                    team_side="away",
                    player_name="임찬규",
                    is_starting=True,
                    appearance_seq=1,
                ),
                GamePitchingStat(
                    game_id="20250403LGSS0",
                    team_side="home",
                    player_name="원태인",
                    is_starting=True,
                    appearance_seq=1,
                ),
            ]
        )
        session.add(
            GameSummary(
                game_id="20250403LGSS0",
                summary_type="리뷰_WPA",
                detail_text=json.dumps(
                    {
                        "crucial_moments": [
                            {"description": "홍길동 : 좌전 안타", "wpa": 0.2},
                            {"description": "=====================================", "wpa": 0.9},
                        ]
                    },
                    ensure_ascii=False,
                ),
            )
        )
        session.commit()

        issues = collect_freshness_issues(session, target_date="20250403")
        failures = evaluate_freshness_gate(session, target_date="20250403")

    assert issues["review_moment_noise"] == ["20250403LGSS0"]
    assert issues["missing_review_moments"] == []
    assert any("review_moment_noise" in failure for failure in failures)


def test_freshness_gate_ignores_non_kbo_and_alias_games():
    SessionLocal = _build_session_factory()

    with SessionLocal() as session:
        session.add_all(
            [
                Game(
                    game_id="20241110PANL0",
                    game_date=date(2024, 11, 10),
                    away_team="PA",
                    home_team="NL",
                    away_score=9,
                    home_score=8,
                    game_status=GAME_STATUS_COMPLETED,
                ),
                Game(
                    game_id="20251011SSSSG0",
                    game_date=date(2025, 10, 11),
                    away_team="SS",
                    home_team="SSG",
                    away_score=3,
                    home_score=4,
                    game_status=GAME_STATUS_COMPLETED,
                ),
                Game(
                    game_id="20251011SSSK0",
                    game_date=date(2025, 10, 11),
                    away_team="SS",
                    home_team="SSG",
                    away_score=3,
                    home_score=4,
                    away_pitcher="후라도",
                    home_pitcher="김광현",
                    game_status=GAME_STATUS_COMPLETED,
                ),
                GameIdAlias(
                    alias_game_id="20251011SSSSG0",
                    canonical_game_id="20251011SSSK0",
                    source="test",
                    reason="duplicate",
                ),
                GameMetadata(game_id="20251011SSSK0", start_time=time(14, 0)),
                GameLineup(
                    game_id="20251011SSSK0",
                    team_side="away",
                    team_code="SS",
                    player_name="김지찬",
                    batting_order=1,
                    appearance_seq=1,
                ),
                GameInningScore(game_id="20251011SSSK0", team_side="away", inning=1, runs=3),
                GameInningScore(game_id="20251011SSSK0", team_side="home", inning=1, runs=4),
                GameEvent(
                    game_id="20251011SSSK0",
                    event_seq=1,
                    description="김지찬 : 우전 안타",
                    event_type="batting",
                    wpa=0.2,
                ),
                GamePitchingStat(
                    game_id="20251011SSSK0",
                    team_side="away",
                    player_name="후라도",
                    is_starting=True,
                    appearance_seq=1,
                ),
                GamePitchingStat(
                    game_id="20251011SSSK0",
                    team_side="home",
                    player_name="김광현",
                    is_starting=True,
                    appearance_seq=1,
                ),
                GameSummary(
                    game_id="20251011SSSK0",
                    summary_type="리뷰_WPA",
                    detail_text=json.dumps(
                        {"crucial_moments": [{"description": "김지찬 : 우전 안타", "wpa": 0.2}]},
                        ensure_ascii=False,
                    ),
                ),
            ]
        )
        session.commit()

        issues = collect_freshness_issues(session)

    assert all("20241110PANL0" not in values for values in issues.values())
    assert all("20251011SSSSG0" not in values for values in issues.values())
