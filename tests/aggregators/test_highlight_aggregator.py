"""Tests for HighlightAggregator — game highlight detection from PBP events."""

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.aggregators.highlight_aggregator import HighlightAggregator
from src.models.game import Game, GameEvent, GameHighlight


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(bind=engine)
    GameEvent.__table__.create(bind=engine)
    GameHighlight.__table__.create(bind=engine)
    sess = sessionmaker(bind=engine)()
    yield sess
    sess.close()


def _add_game(session, game_id="20250101", status="COMPLETED"):
    session.add(Game(
        game_id=game_id,
        stadium="잠실",
        game_status=status,
        game_date=date(2025, 1, 1),
        home_team="LG",
        away_team="SS",
    ))
    session.commit()


def _add_event(session, game_id="20250101", event_seq=1, inning=1,
               inning_half="top", description=None, event_type="hit",
               wpa=0.05, home_score=0, away_score=0,
               bases_before="000", batter_id=10001):
    session.add(GameEvent(
        game_id=game_id,
        event_seq=event_seq,
        inning=inning,
        inning_half=inning_half,
        description=description,
        event_type=event_type,
        wpa=wpa,
        home_score=home_score,
        away_score=away_score,
        bases_before=bases_before,
        batter_id=batter_id,
    ))
    session.commit()


class TestHighlightAggregator:
    def test_no_events_returns_empty(self, session):
        _add_game(session)
        agg = HighlightAggregator(session)
        assert agg.aggregate_game_highlights("20250101") == []

    def test_low_wpa_event_skipped(self, session):
        _add_game(session)
        _add_event(session, wpa=0.02)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        assert len(highlights) == 0

    def test_high_wpa_triggers_big_play(self, session):
        _add_game(session)
        _add_event(session, wpa=0.10)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        assert len(highlights) == 1
        h = highlights[0]
        assert h.highlight_type == "BIG_PLAY"
        assert h.wpa == 0.10
        assert h.importance_score >= 0.10

    def test_walkoff_detection_bottom_ninth(self, session):
        _add_game(session)
        _add_event(session, event_seq=1, inning=9, inning_half="bottom",
                   home_score=1, away_score=0, description="끝내기 안타", wpa=0.30)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        assert len(highlights) >= 1
        h = highlights[0]
        assert h.highlight_type == "WALK_OFF"
        assert "끝내기" in h.tags

    def test_walkoff_by_description(self, session):
        _add_game(session)
        _add_event(session, description="끝내기 홈런", wpa=0.35)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        assert len(highlights) == 1
        assert highlights[0].highlight_type == "WALK_OFF"

    def test_lead_change_detection(self, session):
        _add_game(session)
        _add_event(session, home_score=2, away_score=1, wpa=0.15)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        assert len(highlights) == 1
        assert highlights[0].highlight_type == "LEAD_CHANGE"
        assert "역전" in highlights[0].tags

    def test_game_tying_detection(self, session):
        _add_game(session)
        _add_event(session, home_score=1, away_score=1, wpa=0.12)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        assert len(highlights) == 1
        assert highlights[0].highlight_type == "GAME_TYING"

    def test_go_ahead_detection(self, session):
        _add_game(session)
        # Need to track prev score: first event at 0-0, second event breaks tie
        _add_event(session, event_seq=1, home_score=0, away_score=0, wpa=0.01)
        _add_event(session, event_seq=2, home_score=1, away_score=0, wpa=0.10)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        go_ahead = [h for h in highlights if h.highlight_type == "GO_AHEAD"]
        assert len(go_ahead) == 1

    def test_home_run_tagged(self, session):
        _add_game(session)
        _add_event(session, description="홈런", event_type="HR", wpa=0.20)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        assert len(highlights) == 1
        assert "홈런" in highlights[0].tags

    def test_grand_slam_tagged(self, session):
        _add_game(session)
        _add_event(session, description="만루 홈런", event_type="HR",
                   bases_before="123", wpa=0.40)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        assert len(highlights) == 1
        assert "만루" in highlights[0].tags
        assert "만루홈런" in highlights[0].tags
        assert "홈런" in highlights[0].tags

    def test_bases_loaded_tag(self, session):
        _add_game(session)
        _add_event(session, bases_before="123", wpa=0.08)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        assert len(highlights) == 1
        assert "만루" in highlights[0].tags

    def test_double_play_tagged(self, session):
        _add_game(session)
        _add_event(session, description="병살타", wpa=0.06)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        assert len(highlights) == 1
        assert "병살" in highlights[0].tags

    def test_importance_score_calculation(self, session):
        _add_game(session)
        _add_event(session, inning=9, inning_half="bottom",
                   home_score=2, away_score=1, description="끝내기 홈런",
                   wpa=0.35)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        h = highlights[0]
        # Importance = wpa(0.35) + walkoff(0.5) + hr(0.05) + inning_bonus(9*0.01) = 0.99
        assert h.importance_score == pytest.approx(0.99, abs=0.01)

    def test_null_wpa_filtered_out(self, session):
        _add_game(session)
        _add_event(session, wpa=None)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        assert len(highlights) == 0

    def test_substitution_events_excluded(self, session):
        _add_game(session)
        _add_event(session, event_type="substitution", wpa=0.05)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        assert len(highlights) == 0

    def test_unknown_events_excluded(self, session):
        _add_game(session)
        _add_event(session, event_type="unknown", wpa=0.05)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        assert len(highlights) == 0

    def test_importance_sorted_descending(self, session):
        _add_game(session)
        _add_event(session, event_seq=1, wpa=0.10, description="single")
        _add_event(session, event_seq=2, wpa=0.30, description="홈런")
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        assert len(highlights) == 2
        assert highlights[0].importance_score >= highlights[1].importance_score

    def test_save_highlights(self, session):
        _add_game(session)
        _add_event(session, wpa=0.10)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        count = agg.save_highlights("20250101", highlights)
        assert count == 1
        saved = session.query(GameHighlight).all()
        assert len(saved) == 1

    def test_save_highlights_replaces_old(self, session):
        _add_game(session)
        _add_event(session, wpa=0.10)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        agg.save_highlights("20250101", highlights)
        agg.save_highlights("20250101", highlights)
        saved = session.query(GameHighlight).all()
        assert len(saved) == 1

    def test_event_type_case_insensitive_filter(self, session):
        _add_game(session)
        _add_event(session, event_type="OTHER", wpa=0.10)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        assert len(highlights) == 0

    def test_null_event_type_filtered(self, session):
        _add_game(session)
        _add_event(session, event_type=None, wpa=0.10)
        agg = HighlightAggregator(session)
        highlights = agg.aggregate_game_highlights("20250101")
        assert len(highlights) == 0
