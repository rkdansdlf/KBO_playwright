from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.crawlers.staff_register_crawler import (
    StaffRegisterCrawler,
    _parse_birth_date,
    _parse_hands,
    _parse_hw,
    _parse_player_id,
)
from src.models.player import PlayerBasic
from src.models.team import Team


def test_parsing_helpers():
    # Test _parse_player_id
    assert _parse_player_id("/Record/Player/HitterDetail/Basic.aspx?playerId=91350") == 91350
    assert _parse_player_id("/Record/Retire/Hitter.aspx?playerId=96340") == 96340
    assert _parse_player_id("no-id-here") is None
    assert _parse_player_id(None) is None

    # Test _parse_hw
    assert _parse_hw("180cm, 85kg") == (180, 85)
    assert _parse_hw("175cm, 70kg") == (175, 70)
    assert _parse_hw("invalid text") == (None, None)

    # Test _parse_birth_date
    assert _parse_birth_date("1985-05-15") == date(1985, 5, 15)
    assert _parse_birth_date("invalid-date") is None

    # Test _parse_hands
    assert _parse_hands("우투우타") == ("R", "R")
    assert _parse_hands("좌투좌타") == ("L", "L")
    assert _parse_hands("우투양타") == ("R", "S")
    assert _parse_hands("invalid") == (None, None)


def test_staff_crawler_save_to_db(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    # Create tables
    for table in (Team.__table__, PlayerBasic.__table__):
        table.create(engine)

    Session = sessionmaker(bind=engine)
    with Session() as session:
        # Seed teams
        session.add(Team(team_id="LG", team_name="LG 트윈스", team_short_name="LG", city="서울"))
        session.commit()

        # Mock PlayerBasicRepository to use our in-memory engine
        from src.repositories.player_basic_repository import PlayerBasicRepository

        # We need to override the engine in the repository instance.
        # Let's patch PlayerBasicRepository properties or just monkeypatch the session mapping.
        # But wait, PlayerBasicRepository uses SessionLocal() or similar.
        # Let's mock PlayerBasicRepository.upsert_players to write directly to our session.
        def mock_upsert_players(self, players):
            count = 0
            for r in players:
                # Basic mock upsert
                existing = session.query(PlayerBasic).filter_by(player_id=r["player_id"]).first()
                if existing:
                    existing.name = r["name"]
                    existing.team = r["team"]
                    existing.status = r["status"]
                    existing.staff_role = r["staff_role"]
                    existing.status_source = r["status_source"]
                else:
                    pb = PlayerBasic(
                        player_id=r["player_id"],
                        name=r["name"],
                        team=r["team"],
                        status=r["status"],
                        staff_role=r["staff_role"],
                        status_source=r["status_source"],
                        career="Test School",
                    )
                    session.add(pb)
                count += 1
            session.commit()
            return count

        monkeypatch.setattr(PlayerBasicRepository, "upsert_players", mock_upsert_players)

        crawler = StaffRegisterCrawler(headless=True)

        mock_records = [
            {
                "player_id": 91350,
                "name": "염경엽",
                "uniform_no": "85",
                "team": "LG",
                "birth_date": "1968-03-01",
                "birth_date_date": date(1968, 3, 1),
                "height_cm": 178,
                "weight_kg": 75,
                "throws": "R",
                "bats": "R",
                "status": "staff",
                "staff_role": "manager",
                "status_source": "register",
            },
            {
                "player_id": 99999,
                "name": "홍길동",
                "uniform_no": "77",
                "team": "LG",
                "birth_date": "1980-01-01",
                "birth_date_date": date(1980, 1, 1),
                "height_cm": 180,
                "weight_kg": 80,
                "throws": "L",
                "bats": "L",
                "status": "staff",
                "staff_role": "coach",
                "status_source": "register",
            },
        ]

        # Dry run shouldn't write to DB
        count_dry = crawler.save_to_db(mock_records, dry_run=True)
        assert count_dry == 2
        assert session.query(PlayerBasic).count() == 0

        # Save to DB
        count_save = crawler.save_to_db(mock_records, dry_run=False)
        assert count_save == 2
        assert session.query(PlayerBasic).count() == 2

        manager = session.query(PlayerBasic).filter_by(player_id=91350).first()
        assert manager.name == "염경엽"
        assert manager.status == "staff"
        assert manager.staff_role == "manager"
        assert manager.status_source == "register"

        coach = session.query(PlayerBasic).filter_by(player_id=99999).first()
        assert coach.name == "홍길동"
        assert coach.status == "staff"
        assert coach.staff_role == "coach"
        assert coach.status_source == "register"
