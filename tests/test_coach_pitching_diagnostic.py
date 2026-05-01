from __future__ import annotations

import json
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.crawlers.player_pitching_all_series_crawler import PitcherStats
from src.cli.daily_review_batch import _upsert_review_summary
from src.models.game import Game, GamePitchingStat, GameSummary
from src.models.player import PlayerSeasonPitching
from src.services.context_aggregator import ContextAggregator
from src.utils.game_status import GAME_STATUS_COMPLETED


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game.__table__,
        GamePitchingStat.__table__,
        GameSummary.__table__,
        PlayerSeasonPitching.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def _seed_pitching_game(session):
    session.add(
        Game(
            game_id="20250401LGSS0",
            game_date=date(2025, 4, 1),
            away_team="LG",
            home_team="SS",
            away_score=4,
            home_score=2,
            game_status=GAME_STATUS_COMPLETED,
        )
    )
    session.add_all(
        [
            GamePitchingStat(
                game_id="20250401LGSS0",
                team_side="away",
                team_code="LG",
                player_id=1001,
                player_name="원정선발",
                is_starting=True,
                appearance_seq=1,
                innings_outs=18,
                pitches=92,
                earned_runs=1,
                strikeouts=6,
            ),
            GamePitchingStat(
                game_id="20250401LGSS0",
                team_side="away",
                team_code="LG",
                player_id=1002,
                player_name="원정불펜",
                is_starting=False,
                appearance_seq=2,
                innings_outs=9,
                pitches=31,
                earned_runs=1,
                strikeouts=2,
            ),
            GamePitchingStat(
                game_id="20250401LGSS0",
                team_side="home",
                team_code="SS",
                player_id=2001,
                player_name="홈선발",
                is_starting=True,
                appearance_seq=1,
                innings_outs=15,
                pitches=81,
                earned_runs=3,
                strikeouts=4,
            ),
            GamePitchingStat(
                game_id="20250401LGSS0",
                team_side="home",
                team_code="SS",
                player_id=2002,
                player_name="홈불펜",
                is_starting=False,
                appearance_seq=2,
                innings_outs=12,
                pitches=44,
                earned_runs=1,
                strikeouts=3,
            ),
        ]
    )
    session.add_all(
        [
            PlayerSeasonPitching(
                player_id=1001,
                season=2025,
                league="REGULAR",
                level="KBO1",
                source="CRAWLER",
                team_code="LG",
                games=20,
                games_started=20,
                innings_pitched=120.0,
                era=3.15,
                whip=1.18,
            ),
            PlayerSeasonPitching(
                player_id=1002,
                season=2025,
                league="REGULAR",
                level="KBO1",
                source="CRAWLER",
                team_code="LG",
                games=45,
                games_started=0,
                innings_pitched=42.0,
                era=2.95,
                holds=12,
            ),
        ]
    )
    session.commit()


def test_completed_pitching_breakdown_uses_game_rows_without_player_basic_join():
    SessionLocal = _build_session_factory()
    with SessionLocal() as session:
        _seed_pitching_game(session)

        payload = ContextAggregator(session).get_completed_game_pitching_breakdown(
            "20250401LGSS0"
        )

    assert payload["raw_counts"]["game_pitching_rows"] == 4
    assert payload["raw_counts"]["starter_rows"] == 2
    assert payload["raw_counts"]["bullpen_rows"] == 2
    assert payload["starters"]["away"]["player_name"] == "원정선발"
    assert payload["starters"]["away"]["season_stats_found"] is True
    assert payload["starters"]["home"]["player_name"] == "홈선발"
    assert payload["starters"]["home"]["season_stats_found"] is False
    assert payload["bullpen"]["away"]["totals"]["innings_outs"] == 9
    assert payload["bullpen"]["home"]["totals"]["pitches"] == 44


def test_coach_pitching_diagnostic_identifies_final_payload_drop():
    SessionLocal = _build_session_factory()
    with SessionLocal() as session:
        _seed_pitching_game(session)
        agg = ContextAggregator(session)

        missing = agg.diagnose_completed_game_coach_pitching("20250401LGSS0")
        assert missing["drop_stage"] == "final_review_payload_missing"

        session.add(
            GameSummary(
                game_id="20250401LGSS0",
                summary_type="리뷰_WPA",
                detail_text=json.dumps({"crucial_moments": []}, ensure_ascii=False),
            )
        )
        session.commit()
        missing_pitching = agg.diagnose_completed_game_coach_pitching("20250401LGSS0")
        assert missing_pitching["drop_stage"] == "final_review_payload_missing_pitching"

        summary = session.query(GameSummary).one()
        summary.detail_text = json.dumps(
            {
                "pitching_breakdown": {
                    "starters": {"away": {"player_name": "원정선발"}, "home": {"player_name": "홈선발"}},
                    "bullpen": {
                        "away": {"pitchers": [{"player_name": "원정불펜"}]},
                        "home": {"pitchers": [{"player_name": "홈불펜"}]},
                    },
                }
            },
            ensure_ascii=False,
        )
        session.commit()

        ok = agg.diagnose_completed_game_coach_pitching("20250401LGSS0")
        assert ok["drop_stage"] == "ok"


def test_review_summary_upsert_updates_duplicate_rows():
    SessionLocal = _build_session_factory()
    with SessionLocal() as session:
        session.add_all(
            [
                GameSummary(
                    game_id="20250401LGSS0",
                    summary_type="리뷰_WPA",
                    detail_text=json.dumps({"crucial_moments": []}, ensure_ascii=False),
                ),
                GameSummary(
                    game_id="20250401LGSS0",
                    summary_type="리뷰_WPA",
                    detail_text=json.dumps({"old": True}, ensure_ascii=False),
                ),
            ]
        )
        session.commit()

        review_json = json.dumps(
            {"pitching_breakdown": {"starters": {"away": {"player_name": "원정선발"}}}},
            ensure_ascii=False,
        )
        _upsert_review_summary(session, "20250401LGSS0", review_json)
        session.commit()

        summaries = session.query(GameSummary).order_by(GameSummary.id).all()
        assert [summary.detail_text for summary in summaries] == [review_json, review_json]


def test_pitcher_stats_repository_payload_preserves_starter_and_out_fields():
    stats = PitcherStats(
        player_id=1001,
        season=2025,
        league="REGULAR",
        games=20,
        games_started=20,
        innings_pitched=120.0,
        innings_outs=360,
        kbb=3.2,
    )

    payload = stats.to_repository_payload()

    assert payload["games_started"] == 20
    assert payload["innings_outs"] == 360
    assert payload["kbb"] == 3.2
