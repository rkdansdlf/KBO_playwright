import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models.game import Game, GamePitchingStat
from src.models.season import KboSeason
from src.models.player import PlayerBasic
from src.aggregators.season_stat_aggregator import SeasonStatAggregator

def test_aggregate_pitching_season_decisions(tmp_path):
    # Setup test database
    engine = create_engine(f"sqlite:///{tmp_path / 'test_aggregator.db'}")
    for table in (
        KboSeason.__table__,
        Game.__table__,
        GamePitchingStat.__table__,
        PlayerBasic.__table__,
    ):
        table.create(bind=engine)
        
    SessionLocal = sessionmaker(bind=engine)
    
    with SessionLocal() as session:
        # Seed Season
        season = KboSeason(season_id=1, season_year=2025, league_type_code=1, league_type_name="KBO 정규시즌")
        session.add(season)
        
        # Seed Games
        g1 = Game(game_id="20250401OBWO0", season_id=1, game_date=datetime.date(2025, 4, 1))
        g2 = Game(game_id="20250402OBWO0", season_id=1, game_date=datetime.date(2025, 4, 2))
        g3 = Game(game_id="20250403OBWO0", season_id=1, game_date=datetime.date(2025, 4, 3))
        session.add_all([g1, g2, g3])
        
        # Seed Pitching Stats
        # Pitcher has 3 games:
        # Game 1: decision='W', wins=1
        # Game 2: decision=None, wins=1
        # Game 3: decision='W', wins=2
        # Summing wins column would give 4 (wrong)
        # Checking decision column gives 2 (correct)
        p1 = GamePitchingStat(
            game_id="20250401OBWO0",
            player_id=9999,
            player_name="테스트투수",
            appearance_seq=1,
            team_side="away",
            decision="W",
            wins=1,
            losses=0,
            saves=0,
            holds=0,
            innings_outs=15,
        )
        p2 = GamePitchingStat(
            game_id="20250402OBWO0",
            player_id=9999,
            player_name="테스트투수",
            appearance_seq=1,
            team_side="away",
            decision=None,
            wins=1,
            losses=0,
            saves=0,
            holds=0,
            innings_outs=6,
        )
        p3 = GamePitchingStat(
            game_id="20250403OBWO0",
            player_id=9999,
            player_name="테스트투수",
            appearance_seq=1,
            team_side="away",
            decision="W",
            wins=2,
            losses=0,
            saves=0,
            holds=0,
            innings_outs=18,
        )
        session.add_all([p1, p2, p3])
        session.commit()
        
        # Test aggregate_pitching_season
        stats = SeasonStatAggregator.aggregate_pitching_season(session, 9999, 2025, "regular")
        assert stats is not None
        assert stats["games"] == 3
        assert stats["wins"] == 2
        assert stats["losses"] == 0
        assert stats["saves"] == 0
        assert stats["holds"] == 0
        assert stats["innings_outs"] == 39
        assert stats["innings_pitched"] == 13.0
        
        # Test aggregate_pitching_season_bulk
        bulk_stats = SeasonStatAggregator.aggregate_pitching_season_bulk(session, 2025, "regular")
        assert len(bulk_stats) == 1
        p_stats = bulk_stats[0]
        assert p_stats["player_id"] == 9999
        assert p_stats["games"] == 3
        assert p_stats["wins"] == 2
        assert p_stats["losses"] == 0
        assert p_stats["saves"] == 0
        assert p_stats["holds"] == 0
