import pytest
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.player import PlayerBasic, PlayerMovement
from src.models.team import Team
from src.models.fa_contract import FAContract
from src.crawlers.fa_crawler import parse_amount_krw, resolve_player_basic_id, FACrawler


def test_parse_amount_krw():
    assert parse_amount_krw("75억원") == 750000
    assert parse_amount_krw("6억 5천만원") == 65000
    assert parse_amount_krw("5000만원") == 5000
    assert parse_amount_krw("비공개") is None
    assert parse_amount_krw(None) is None


def test_resolve_player_basic_id():
    engine = create_engine("sqlite:///:memory:")
    # Create tables
    for table in (Team.__table__, PlayerBasic.__table__):
        table.create(engine)

    Session = sessionmaker(bind=engine)
    with Session() as session:
        # Add team and players
        session.add(Team(team_id="LG", team_name="LG 트윈스", team_short_name="LG", city="서울"))
        session.add_all([
            PlayerBasic(player_id=1001, name="염경엽", team="LG"),
            PlayerBasic(player_id=1002, name="홍길동", team="KT"),
            PlayerBasic(player_id=1003, name="이순신", team="두산"),
        ])
        session.commit()

        # Test exact match
        assert resolve_player_basic_id(session, "염경엽", "LG") == 1001
        
        # Test unique name match (fallback)
        assert resolve_player_basic_id(session, "홍길동", "LG") == 1002
        
        # Test duplicate name but matching team
        # Let's add another 이순신
        session.add(PlayerBasic(player_id=1004, name="이순신", team="LG"))
        session.commit()
        
        # Now "이순신" has two records: 1003 (두산), 1004 (LG)
        assert resolve_player_basic_id(session, "이순신", "LG") == 1004


def test_fa_crawler_save_to_db():
    engine = create_engine("sqlite:///:memory:")
    # Create tables
    for table in (Team.__table__, PlayerBasic.__table__, PlayerMovement.__table__, FAContract.__table__):
        table.create(engine)

    Session = sessionmaker(bind=engine)
    with Session() as session:
        # Seed teams
        session.add(Team(team_id="LG", team_name="LG 트윈스", team_short_name="LG", city="서울"))
        session.add(PlayerBasic(player_id=91350, name="염경엽", team="LG"))
        session.commit()

        crawler = FACrawler(headless=True)
        
        mock_data = [
            {
                "year": 2024,
                "player_name": "염경엽",
                "team": "LG",
                "duration": "4년",
                "amount": "75억원",
                "remarks": "옵션 10억",
                "fa_type": "retained",
                "old_team": "LG",
                "new_team": "LG"
            }
        ]

        # Dry run shouldn't write to DB
        crawler.save_to_db(mock_data, session, dry_run=True)
        assert session.query(FAContract).count() == 0
        assert session.query(PlayerMovement).count() == 0

        # Save to DB
        crawler.save_to_db(mock_data, session, dry_run=False)
        
        assert session.query(FAContract).count() == 1
        assert session.query(PlayerMovement).count() == 1

        contract = session.query(FAContract).first()
        assert contract.player_name == "염경엽"
        assert contract.player_basic_id == 91350
        assert contract.total_amount_krw == 750000
        assert contract.contract_duration == "4년"
        assert contract.total_amount == "75억원"
        assert contract.remarks == "옵션 10억"

        movement = session.query(PlayerMovement).first()
        assert movement.player_name == "염경엽"
        assert "FA계약: 4년, 75억원" in movement.remarks
