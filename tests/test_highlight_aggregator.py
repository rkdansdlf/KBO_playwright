from __future__ import annotations

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

import datetime
from src.models.base import Base
from src.models.game import Game, GameEvent, GameHighlight
from src.aggregators.highlight_aggregator import HighlightAggregator


def _build_engine():
    engine = create_engine("sqlite:///:memory:")
    # Create required tables for testing
    Game.__table__.create(bind=engine)
    GameEvent.__table__.create(bind=engine)
    GameHighlight.__table__.create(bind=engine)
    return engine


def _new_session():
    engine = _build_engine()
    Session = sessionmaker(bind=engine)
    return Session()


def _make_game(game_id: str) -> Game:
    return Game(
        game_id=game_id,
        game_status="COMPLETED",
        game_date=datetime.date(2024, 3, 23),
        home_team="SSG",
        away_team="HH",
        home_score=5,
        away_score=4,
    )


def _make_event(
    game_id: str,
    event_seq: int,
    inning: int,
    inning_half: str,
    description: str,
    event_type: str = "batting",
    wpa: float = 0.0,
    home_score: int = 0,
    away_score: int = 0,
    bases_before: str = "---",
) -> GameEvent:
    return GameEvent(
        game_id=game_id,
        event_seq=event_seq,
        inning=inning,
        inning_half=inning_half,
        outs=0,
        description=description,
        event_type=event_type,
        wpa=wpa,
        home_score=home_score,
        away_score=away_score,
        bases_before=bases_before,
        bases_after="---",
    )


class TestHighlightAggregator:

    def test_empty_events_returns_empty_highlights(self):
        """No events for a game -> no highlights generated."""
        session = _new_session()
        game_id = "20240323HHSSG0"
        session.add(_make_game(game_id))
        session.commit()

        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights(game_id)
        assert highlights == []

    def test_walkoff_detection(self):
        """Walk-off play in bottom of 9th or later should be correctly detected and tagged."""
        session = _new_session()
        game_id = "20240323HHSSG0"
        session.add(_make_game(game_id))
        
        # Home team SSG trailing 3-4, then wins 5-4 on a walk-off single in bottom of 9th
        session.add(_make_event(
            game_id, 1, 9, "top",
            description="상대팀 공격 종료",
            event_type="batting",
            wpa=-0.010,
            home_score=3,
            away_score=4,
            bases_before="---"
        ))
        session.add(_make_event(
            game_id, 2, 9, "bottom",
            description="최정 : 끝내기 2타점 안타!",
            event_type="batting",
            wpa=0.450,
            home_score=5,
            away_score=4,
            bases_before="-23"
        ))
        session.commit()

        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights(game_id)

        assert len(highlights) >= 1
        h = [hl for hl in highlights if hl.highlight_type == "WALK_OFF"][0]
        assert "끝내기" in h.tags
        assert "역전" in h.tags
        assert h.wpa == 0.450
        assert h.importance_score > 0.95  # 0.45 WPA + 0.5 walk-off + 0.25 lead change + 0.09 inning

    def test_lead_change_and_tying_runs(self):
        """Lead changes, tying runs, and go-ahead runs should be tagged correctly."""
        session = _new_session()
        game_id = "20240323HHSSG0"
        session.add(_make_game(game_id))

        # Event 1: Away team scores, breaking tie: 0-0 -> 1-0 Away (Away leads)
        # Note: score_diff = Home - Away. So Home 0, Away 1 means score_diff = -1
        session.add(_make_event(
            game_id, 1, 1, "top",
            description="노시환 : 1타점 적시타",
            event_type="batting",
            wpa=0.120,
            home_score=0,
            away_score=1,
            bases_before="-2-"
        ))
        # Event 2: Home team hits 2-run HR, reversing lead: 0-1 Away -> 2-1 Home (Home leads)
        # score_diff goes from -1 to +1
        session.add(_make_event(
            game_id, 2, 3, "bottom",
            description="최정 : 2루타 2타점 홈런",
            event_type="batting",
            wpa=0.250,
            home_score=2,
            away_score=1,
            bases_before="1--"
        ))
        # Event 3: Away team ties: 2-1 Home -> 2-2 Tie
        # score_diff goes from +1 to 0
        session.add(_make_event(
            game_id, 3, 5, "top",
            description="채은성 : 솔로 홈런",
            event_type="batting",
            wpa=0.180,
            home_score=2,
            away_score=2,
            bases_before="---"
        ))
        session.commit()

        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights(game_id)

        # Sorted by importance score desc
        # Event 2 ( 최정 HR) WPA 0.25 + 0.25 (역전) + 0.05 (홈런) + 0.03 (3회) = 0.58
        # Event 3 ( 채은성 HR) WPA 0.18 + 0.15 (동점) + 0.05 (홈런) + 0.05 (5회) = 0.43
        # Event 1 ( 노시환 적시타) WPA 0.12 + 0.10 (동점 균열) + 0.01 (1회) = 0.23
        assert len(highlights) == 3
        
        # Check Event 2 (lead change)
        assert highlights[0].highlight_type == "LEAD_CHANGE"
        assert "역전" in highlights[0].tags
        assert "홈런" in highlights[0].tags
        
        # Check Event 3 (tying run)
        assert highlights[1].highlight_type == "GAME_TYING"
        assert "동점" in highlights[1].tags
        
        # Check Event 1 (go ahead)
        assert highlights[2].highlight_type == "GO_AHEAD"
        assert "동점 균열" in highlights[2].tags

    def test_special_situation_tags(self):
        """Bases loaded, grand slam, and double play moments should be tagged correctly."""
        session = _new_session()
        game_id = "20240323HHSSG0"
        session.add(_make_game(game_id))

        # Event 1: Grand slam with bases loaded
        session.add(_make_event(
            game_id, 1, 4, "bottom",
            description="한유섬 : 만루 홈런!!",
            event_type="batting",
            wpa=0.350,
            home_score=4,
            away_score=0,
            bases_before="123"
        ))
        # Event 2: Double play (병살)
        session.add(_make_event(
            game_id, 2, 6, "top",
            description="안치홍 : 유격수 병살타 아웃",
            event_type="batting",
            wpa=-0.080,
            home_score=4,
            away_score=0,
            bases_before="1--"
        ))
        session.commit()

        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights(game_id)

        assert len(highlights) == 2
        
        # Grand slam assertions
        gs_h = [h for h in highlights if h.event_seq == 1][0]
        assert "만루" in gs_h.tags
        assert "홈런" in gs_h.tags
        assert "만루홈런" in gs_h.tags
        
        # Double play assertions
        dp_h = [h for h in highlights if h.event_seq == 2][0]
        assert "병살" in dp_h.tags
        assert dp_h.wpa == -0.080

    def test_save_highlights_persists_to_db(self):
        """Highlights should be deleted and repersisted in local database successfully."""
        session = _new_session()
        game_id = "20240323HHSSG0"
        session.add(_make_game(game_id))
        session.commit()

        agg = HighlightAggregator(session)
        mock_highlights = [
            GameHighlight(game_id=game_id, event_seq=1, highlight_type="BIG_PLAY", description="Play 1", tags=["태그"]),
            GameHighlight(game_id=game_id, event_seq=2, highlight_type="BIG_PLAY", description="Play 2", tags=["태그"])
        ]
        
        # First save
        saved_count = agg.save_highlights(game_id, mock_highlights)
        assert saved_count == 2
        assert session.query(GameHighlight).filter(GameHighlight.game_id == game_id).count() == 2
        
        # Resave (should delete first)
        new_highlights = [
            GameHighlight(game_id=game_id, event_seq=3, highlight_type="WALK_OFF", description="New Play", tags=["끝내기"])
        ]
        saved_count_new = agg.save_highlights(game_id, new_highlights)
        assert saved_count_new == 1
        assert session.query(GameHighlight).filter(GameHighlight.game_id == game_id).count() == 1
        db_highlight = session.query(GameHighlight).filter(GameHighlight.game_id == game_id).first()
        assert db_highlight.event_seq == 3
        assert db_highlight.highlight_type == "WALK_OFF"
