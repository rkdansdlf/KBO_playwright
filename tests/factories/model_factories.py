from datetime import date, time

from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameHighlight,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GamePlayByPlay,
    GameSummary,
    GameValidationMetrics,
    PlayerGameBatting,
    PlayerGamePitching,
)
from src.models.player import (
    Player,
    PlayerBasic,
    PlayerIdentity,
    PlayerMovement,
    PlayerSeasonBaserunning,
    PlayerSeasonBatting,
    PlayerSeasonFielding,
    PlayerSeasonPitching,
)
from src.models.standings import TeamStandingsDaily
from src.models.team import Team, TeamDailyRoster, TeamSeasonBaserunning, TeamSeasonFielding
from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching


def make_game(**kwargs) -> Game:
    defaults = dict(
        game_id="20250401LGSS0",
        game_date=date(2025, 4, 1),
        stadium="잠실",
        home_team="LG",
        away_team="SSG",
        home_score=5,
        away_score=3,
        season_id=2025,
        game_status="COMPLETED",
        is_primary=True,
    )
    defaults.update(kwargs)
    return Game(**defaults)


def make_game_id_alias(**kwargs) -> GameIdAlias:
    defaults = dict(
        alias_game_id="ALT_001",
        canonical_game_id="20250401LGSS0",
        source="test",
        reason="testing",
    )
    defaults.update(kwargs)
    return GameIdAlias(**defaults)


def make_game_summary(**kwargs) -> GameSummary:
    defaults = dict(
        game_id="20250401LGSS0",
        summary_type="STORY",
        player_name="Test Player",
        detail_text="Test summary detail",
    )
    defaults.update(kwargs)
    return GameSummary(**defaults)


def make_game_play_by_play(**kwargs) -> GamePlayByPlay:
    defaults = dict(
        game_id="20250401LGSS0",
        inning=1,
        inning_half="초",
        pitcher_name="Test Pitcher",
        batter_name="Test Batter",
        play_description="Strikeout swinging",
        event_type="K",
        result="Strikeout",
    )
    defaults.update(kwargs)
    return GamePlayByPlay(**defaults)


def make_game_metadata(**kwargs) -> GameMetadata:
    defaults = dict(
        game_id="20250401LGSS0",
        stadium_code="JAMSIL",
        stadium_name="잠실야구장",
        attendance=23750,
        start_time=time(18, 30),
        end_time=time(21, 15),
        game_time_minutes=165,
    )
    defaults.update(kwargs)
    return GameMetadata(**defaults)


def make_game_inning_score(**kwargs) -> GameInningScore:
    defaults = dict(
        game_id="20250401LGSS0",
        team_side="home",
        team_code="LG",
        inning=1,
        runs=2,
    )
    defaults.update(kwargs)
    return GameInningScore(**defaults)


def make_game_lineup(**kwargs) -> GameLineup:
    defaults = dict(
        game_id="20250401LGSS0",
        team_side="home",
        team_code="LG",
        player_id=12345,
        player_name="Test Batter",
        batting_order=1,
        position="3B",
        is_starter=True,
        appearance_seq=1,
    )
    defaults.update(kwargs)
    return GameLineup(**defaults)


def make_game_batting_stat(**kwargs) -> GameBattingStat:
    defaults = dict(
        game_id="20250401LGSS0",
        team_side="home",
        team_code="LG",
        player_id=12345,
        player_name="Test Batter",
        batting_order=1,
        appearance_seq=1,
        plate_appearances=4,
        at_bats=4,
        hits=2,
        avg=0.500,
        obp=0.500,
        slg=0.750,
        ops=1.250,
    )
    defaults.update(kwargs)
    return GameBattingStat(**defaults)


def make_game_pitching_stat(**kwargs) -> GamePitchingStat:
    defaults = dict(
        game_id="20250401LGSS0",
        team_side="home",
        team_code="LG",
        player_id=12345,
        player_name="Test Pitcher",
        appearance_seq=1,
        innings_outs=9,
        hits_allowed=5,
        runs_allowed=3,
        earned_runs=2,
        walks_allowed=2,
        strikeouts=7,
        era=2.00,
        whip=1.00,
    )
    defaults.update(kwargs)
    return GamePitchingStat(**defaults)


def make_game_event(**kwargs) -> GameEvent:
    defaults = dict(
        game_id="20250401LGSS0",
        event_seq=1,
        inning=1,
        inning_half="초",
        outs=2,
        batter_name="Test Batter",
        pitcher_name="Test Pitcher",
        description="Single to center",
        event_type="single",
        result_code="S",
        rbi=0,
        home_score=0,
        away_score=0,
    )
    defaults.update(kwargs)
    return GameEvent(**defaults)


def make_game_validation_metrics(**kwargs) -> GameValidationMetrics:
    defaults = dict(
        game_id="20250401LGSS0",
        validation_status="verified",
    )
    defaults.update(kwargs)
    return GameValidationMetrics(**defaults)


def make_game_highlight(**kwargs) -> GameHighlight:
    defaults = dict(
        game_id="20250401LGSS0",
        highlight_type="BIG_PLAY",
        description="Game-winning home run",
        importance_score=0.95,
    )
    defaults.update(kwargs)
    return GameHighlight(**defaults)


def make_player_game_batting(**kwargs) -> PlayerGameBatting:
    defaults = dict(
        game_id="20250401LGSS0",
        player_id=12345,
        player_name="Test Batter",
        team_side="home",
        team_code="LG",
        plate_appearances=4,
        at_bats=3,
        hits=1,
        avg=0.333,
    )
    defaults.update(kwargs)
    return PlayerGameBatting(**defaults)


def make_player_game_pitching(**kwargs) -> PlayerGamePitching:
    defaults = dict(
        game_id="20250401LGSS0",
        player_id=12345,
        player_name="Test Pitcher",
        team_side="home",
        team_code="LG",
        innings_outs=9,
        earned_runs=2,
        era=2.00,
    )
    defaults.update(kwargs)
    return PlayerGamePitching(**defaults)


def make_player_basic(**kwargs) -> PlayerBasic:
    defaults = dict(
        player_id=12345,
        name="테스트선수",
        team="LG",
        position="내야수",
    )
    defaults.update(kwargs)
    return PlayerBasic(**defaults)


def make_player(**kwargs) -> Player:
    defaults = dict(
        status="ACTIVE",
        is_foreign_player=False,
    )
    defaults.update(kwargs)
    return Player(**defaults)


def make_player_identity(**kwargs) -> PlayerIdentity:
    defaults = dict(
        player_id=1,
        name_kor="테스트선수",
        name_eng="Test Player",
        is_primary=True,
    )
    defaults.update(kwargs)
    return PlayerIdentity(**defaults)


def make_player_season_batting(**kwargs) -> PlayerSeasonBatting:
    defaults = dict(
        player_id=12345,
        season=2025,
        league="REGULAR",
        level="KBO1",
        source="TEST",
        team_code="LG",
    )
    defaults.update(kwargs)
    return PlayerSeasonBatting(**defaults)


def make_player_season_pitching(**kwargs) -> PlayerSeasonPitching:
    defaults = dict(
        player_id=12345,
        season=2025,
        league="REGULAR",
        level="KBO1",
        source="TEST",
        team_code="LG",
    )
    defaults.update(kwargs)
    return PlayerSeasonPitching(**defaults)


def make_player_movement(**kwargs) -> PlayerMovement:
    defaults = dict(
        movement_date=date(2025, 4, 1),
        section="Trade",
        team_code="LG",
        player_name="Test Player",
        resolution_status="unresolved",
    )
    defaults.update(kwargs)
    return PlayerMovement(**defaults)


def make_player_season_fielding(**kwargs) -> PlayerSeasonFielding:
    defaults = dict(
        player_id=12345,
        team_id="LG",
        year=2025,
        position_id="3B",
    )
    defaults.update(kwargs)
    return PlayerSeasonFielding(**defaults)


def make_player_season_baserunning(**kwargs) -> PlayerSeasonBaserunning:
    defaults = dict(
        player_id=12345,
        team_id="LG",
        year=2025,
    )
    defaults.update(kwargs)
    return PlayerSeasonBaserunning(**defaults)


def make_team(**kwargs) -> Team:
    defaults = dict(
        team_id="LG",
        team_name="LG Twins",
        team_short_name="LG",
        city="서울",
        founded_year=1990,
        stadium_name="잠실야구장",
        is_active=True,
    )
    defaults.update(kwargs)
    return Team(**defaults)


def make_team_daily_roster(**kwargs) -> TeamDailyRoster:
    defaults = dict(
        roster_date=date(2025, 4, 1),
        team_code="LG",
        player_id=12345,
        player_name="Test Player",
        position="투수",
        person_type="player",
    )
    defaults.update(kwargs)
    return TeamDailyRoster(**defaults)


def make_team_season_batting(**kwargs):
    defaults = dict(
        team_id="LG",
        team_name="LG Twins",
        season=2025,
        league="REGULAR",
        games=144,
        plate_appearances=5500,
        at_bats=5000,
        hits=1400,
        avg=0.280,
    )
    defaults.update(kwargs)
    return TeamSeasonBatting(**defaults)


def make_team_season_pitching(**kwargs):
    defaults = dict(
        team_id="LG",
        team_name="LG Twins",
        season=2025,
        league="REGULAR",
        games=144,
        wins=85,
        losses=55,
        era=3.75,
    )
    defaults.update(kwargs)
    return TeamSeasonPitching(**defaults)


def make_team_season_fielding(**kwargs) -> TeamSeasonFielding:
    defaults = dict(
        season=2025,
        team_code="LG",
    )
    defaults.update(kwargs)
    return TeamSeasonFielding(**defaults)


def make_team_season_baserunning(**kwargs) -> TeamSeasonBaserunning:
    defaults = dict(
        season=2025,
        team_code="LG",
    )
    defaults.update(kwargs)
    return TeamSeasonBaserunning(**defaults)


def make_standings_daily(**kwargs) -> TeamStandingsDaily:
    defaults = dict(
        standings_date=date(2025, 4, 1),
        team_code="LG",
        games_played=1,
        wins=1,
        losses=0,
        draws=0,
        win_pct=1.0,
        games_behind=0.0,
        rank=1,
        top_5=True,
    )
    defaults.update(kwargs)
    return TeamStandingsDaily(**defaults)
