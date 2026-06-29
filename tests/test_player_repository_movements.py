from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.player import Player, PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching
from src.models.team import Team, TeamDailyRoster
from src.repositories.player_repository import PlayerRepository


def test_resolve_movement_player_uses_position_for_ambiguous_names():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Team.__table__,
        PlayerBasic.__table__,
        Player.__table__,
        TeamDailyRoster.__table__,
        PlayerSeasonBatting.__table__,
        PlayerSeasonPitching.__table__,
    ):
        table.create(engine)

    Session = sessionmaker(bind=engine)
    with Session() as session:
        session.add(Team(team_id="LG", team_name="LG 트윈스", team_short_name="LG", city="서울"))
        session.add_all(
            [
                PlayerBasic(player_id=1001, name="김지성", position="내야수", team=""),
                PlayerBasic(player_id=1002, name="김지성", position="투수", team=""),
            ],
        )
        session.commit()

        repo = PlayerRepository()

        assert repo._resolve_movement_player_id(session, "김지성", "LG", 2017) is None
        assert repo._resolve_movement_player_id(session, "김지성(내야수)", "LG", 2017) == 1001


def test_resolve_movement_player_uses_unique_profile_mirror():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Team.__table__,
        PlayerBasic.__table__,
        Player.__table__,
        TeamDailyRoster.__table__,
        PlayerSeasonBatting.__table__,
        PlayerSeasonPitching.__table__,
    ):
        table.create(engine)

    Session = sessionmaker(bind=engine)
    with Session() as session:
        session.add(Team(team_id="LG", team_name="LG 트윈스", team_short_name="LG", city="서울"))
        session.add_all(
            [
                PlayerBasic(player_id=1001, name="배재준", position="투수", team="LG"),
                PlayerBasic(player_id=63145, name="배재준", position="투수", team="LG"),
                Player(kbo_person_id="63145", player_basic_id=63145),
            ],
        )
        session.commit()

        repo = PlayerRepository()

        assert repo._resolve_movement_player_id(session, "배재준(투수)", "LG", 2017) == 63145


def test_resolve_movement_player_uses_same_year_roster_link():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Team.__table__,
        PlayerBasic.__table__,
        Player.__table__,
        TeamDailyRoster.__table__,
        PlayerSeasonBatting.__table__,
        PlayerSeasonPitching.__table__,
    ):
        table.create(engine)

    Session = sessionmaker(bind=engine)
    with Session() as session:
        session.add(Team(team_id="SS", team_name="삼성 라이온즈", team_short_name="삼성", city="대구"))
        session.add_all(
            [
                PlayerBasic(player_id=60146, name="이승현", position=None, team=None),
                PlayerBasic(player_id=62415, name="이승현", position=None, team=None),
                TeamDailyRoster(
                    roster_date=date(2024, 9, 1),
                    team_code="SS",
                    player_id=60146,
                    player_basic_id=60146,
                    person_type="player",
                    player_name="이승현",
                    position="투수",
                ),
            ],
        )
        session.commit()

        repo = PlayerRepository()

        assert repo._resolve_movement_player_id(session, "이승현(투수)", "SS", 2024) == 60146


def test_resolve_movement_player_uses_franchise_season_history():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Team.__table__,
        PlayerBasic.__table__,
        Player.__table__,
        TeamDailyRoster.__table__,
        PlayerSeasonBatting.__table__,
        PlayerSeasonPitching.__table__,
    ):
        table.create(engine)

    Session = sessionmaker(bind=engine)
    with Session() as session:
        session.add_all(
            [
                Team(team_id="HT", team_name="해태 타이거즈", team_short_name="해태", city="광주", franchise_id=5),
                Team(team_id="KIA", team_name="KIA 타이거즈", team_short_name="KIA", city="광주", franchise_id=5),
            ],
        )
        session.add_all(
            [
                PlayerBasic(player_id=1001, name="박찬호", position=None, team=None),
                PlayerBasic(player_id=1002, name="박찬호", position=None, team=None),
                PlayerSeasonBatting(
                    player_id=1001,
                    season=2017,
                    league="REGULAR",
                    level="KBO1",
                    team_code="KIA",
                ),
            ],
        )
        session.commit()

        repo = PlayerRepository()

        assert repo._resolve_movement_player_id(session, "박찬호(내야수)", "HT", 2017) == 1001
