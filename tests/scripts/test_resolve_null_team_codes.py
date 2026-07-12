from __future__ import annotations

from datetime import date
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.player import (
    PlayerBasic,
    PlayerSeasonBatting,
    PlayerSeasonPitching,
    PlayerSeasonFielding,
    PlayerSeasonBaserunning,
)
from src.models.game import (
    Game,
    GameLineup,
    GameBattingStat,
    GamePitchingStat,
)
from src.models.season import KboSeason
from src.models.team import TeamDailyRoster
from src.models.roster_transaction import RosterTransaction
from scripts.maintenance.resolve_null_team_codes import resolve_team_codes, find_null_rows


def test_resolve_null_team_codes():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # 1. Seed season info
        season = KboSeason(season_id=1, season_year=2025, league_type_code="1", league_type_name="KBO Regular")
        session.add(season)

        # 2. Seed basic player stubs
        p1 = PlayerBasic(player_id=101, name="홍길동", team="LG Twins")
        p2 = PlayerBasic(player_id=102, name="임꺽정", team="두산")
        p3 = PlayerBasic(player_id=103, name="장길산", team="기아")
        p4 = PlayerBasic(player_id=104, name="홍경래", team="한화")
        p5 = PlayerBasic(player_id=105, name="이순신", team="SSG")
        session.add_all([p1, p2, p3, p4, p5])

        # 3. Seed Null team code season rows
        session.add_all(
            [
                # Player 101 - batting and pitching nulls
                PlayerSeasonBatting(player_id=101, season=2025, league="REGULAR", level="FUTURES", team_code=None),
                PlayerSeasonPitching(player_id=101, season=2025, league="REGULAR", level="FUTURES", team_code=None),
                # Player 102 - null
                PlayerSeasonBatting(player_id=102, season=2025, league="REGULAR", level="FUTURES", team_code=None),
                # Player 103 - null
                PlayerSeasonBatting(player_id=103, season=2025, league="REGULAR", level="FUTURES", team_code=""),
                # Player 104 - null
                PlayerSeasonBatting(player_id=104, season=2025, league="REGULAR", level="FUTURES", team_code=None),
                # Player 105 - null
                PlayerSeasonBatting(player_id=105, season=2025, league="REGULAR", level="FUTURES", team_code=None),
            ]
        )

        # 4. Seed heuristics data
        # H1: Game stats resolve (Player 101 -> LG)
        game1 = Game(game_id="20250501LGD0", game_date=date(2025, 5, 1), season_id=1, home_team="LG", away_team="OB")
        session.add(game1)
        session.add(
            GameBattingStat(
                game_id="20250501LGD0",
                player_id=101,
                player_name="홍길동",
                team_side="home",
                team_code="LG",
                appearance_seq=1,
            )
        )

        # H2: Other season stats resolve (Player 102 -> OB)
        session.add(PlayerSeasonFielding(player_id=102, team_id="OB", year=2025, position_id="C", games=5))

        # H3: Lineups resolve (Player 103 -> KIA)
        session.add(
            GameLineup(
                game_id="20250501LGD0",
                team_side="home",
                team_code="KIA",
                player_id=103,
                player_name="장길산",
                appearance_seq=1,
            )
        )

        # H4: Roster history resolve (Player 104 -> HH)
        session.add(
            TeamDailyRoster(
                roster_date=date(2025, 6, 1), team_code="HH", player_id=104, player_name="홍경래", position="투수"
            )
        )

        # H5: Single career resolve (Player 105 -> SSG)
        # Seed an older season batting stat with SSG
        session.add(PlayerSeasonBatting(player_id=105, season=2024, league="REGULAR", level="FUTURES", team_code="SSG"))

        session.commit()

        # Check null rows initially found
        nulls = find_null_rows(session)
        assert len(nulls) == 6

        # Dry run resolve
        stats_dry = resolve_team_codes(session, apply=False)
        assert stats_dry["total"] == 6
        assert stats_dry["resolved_game_stats"] == 1  # batting for 101
        assert stats_dry["resolved_season_stats"] == 1  # 102
        assert stats_dry["resolved_lineups"] == 1  # 103
        assert stats_dry["resolved_rosters"] == 1  # 104
        assert stats_dry["resolved_single_career"] == 2  # 105 and 101 pitching
        assert stats_dry["unresolved"] == 0
        assert stats_dry["updated"] == 0

        # Run actual resolve with apply=True
        stats_apply = resolve_team_codes(session, apply=True)
        assert stats_apply["updated"] == 6
        session.commit()

        # Check no null rows left
        nulls_after = find_null_rows(session)
        assert len(nulls_after) == 0

        # Verify values in DB
        p1_bat = session.query(PlayerSeasonBatting).filter_by(player_id=101, season=2025).one()
        assert p1_bat.team_code == "LG"

        p2_bat = session.query(PlayerSeasonBatting).filter_by(player_id=102, season=2025).one()
        assert p2_bat.team_code == "OB"

        p3_bat = session.query(PlayerSeasonBatting).filter_by(player_id=103, season=2025).one()
        assert p3_bat.team_code == "KIA"

        p4_bat = session.query(PlayerSeasonBatting).filter_by(player_id=104, season=2025).one()
        assert p4_bat.team_code == "HH"

        p5_bat = session.query(PlayerSeasonBatting).filter_by(player_id=105, season=2025).one()
        assert p5_bat.team_code == "SSG"

    finally:
        session.close()
        engine.dispose()
