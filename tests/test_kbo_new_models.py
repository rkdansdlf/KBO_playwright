"""Tests for newly added KBO ORM models."""

from __future__ import annotations

from datetime import date

from src.models import (
    FuturesGameSchedule,
    FuturesTeamStandings,
    KboPressRelease,
    PlayerDraftHistory,
    PlayerMilestone,
    PlayerSplitsStat,
)


def test_futures_schedule_model() -> None:
    """Test creation and representation of FuturesGameSchedule model."""
    game = FuturesGameSchedule(
        season=2026,
        game_date=date(2026, 4, 1),
        game_id="FUT_20260401_SSG_LG",
        away_team="SSG",
        home_team="LG",
        stadium="이천",
        game_status="SCHEDULED",
    )
    assert game.season == 2026
    assert "FUT_20260401" in repr(game)


def test_futures_standings_model() -> None:
    """Test FuturesTeamStandings model."""
    standing = FuturesTeamStandings(
        season=2026,
        division="북부",
        team_code="LG",
        games_played=10,
        wins=7,
        losses=3,
        draws=0,
        win_pct=0.700,
        games_behind=0.0,
        rank=1,
    )
    assert standing.rank == 1
    assert "북부" in repr(standing)


def test_kbo_press_release_model() -> None:
    """Test KboPressRelease model."""
    press = KboPressRelease(
        notice_id="1001",
        published_date=date(2026, 5, 10),
        category="공시",
        title="2026 KBO 상벌위원회 결과 공시",
        source_url="https://www.koreabaseball.com/News/Notice/1001",
    )
    assert press.notice_id == "1001"
    assert "1001" in repr(press)


def test_player_draft_model() -> None:
    """Test PlayerDraftHistory model."""
    draft = PlayerDraftHistory(
        season=2026,
        draft_type="1차",
        round_num=1,
        pick_seq=1,
        team_code="한화",
        player_name="김신인",
        position="투수",
        school="덕수고",
    )
    assert draft.pick_seq == 1
    assert "김신인" in repr(draft)


def test_player_splits_model() -> None:
    """Test PlayerSplitsStat model."""
    split = PlayerSplitsStat(
        season=2026,
        player_id="60001",
        player_name="홍길동",
        team_code="KIA",
        split_type="scoring_position",
        split_key="득점권",
        ab=50,
        hits=18,
        hr=3,
        rbi=22,
        avg=0.360,
    )
    assert split.avg == 0.360
    assert "scoring_position" in repr(split)


def test_player_milestone_model() -> None:
    """Test PlayerMilestone model."""
    milestone = PlayerMilestone(
        season=2026,
        player_id="70001",
        player_name="최형우",
        team_code="KIA",
        milestone_category="2000안타",
        current_val=1988,
        target_val=2000,
        remaining_val=12,
        is_achieved=False,
    )
    assert milestone.remaining_val == 12
    assert "2000안타" in repr(milestone)
