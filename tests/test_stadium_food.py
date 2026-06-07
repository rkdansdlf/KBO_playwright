import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from scripts.legacy.maintenance.seed_data import seed_stadium_foods
from src.models.stadium_food import StadiumFood


def test_stadium_food_model_basic():
    """Verify that we can create the stadium_foods table, insert, and query data."""
    engine = create_engine("sqlite:///:memory:")
    StadiumFood.__table__.create(engine)

    Session = sessionmaker(bind=engine)
    with Session() as session:
        # Create a new food record
        food = StadiumFood(
            stadium_name="수원 kt wiz 파크",
            restaurant_name="보영만두",
            menu_item="군만두 / 쫄면",
            location="3루 내야 복도 2층",
            description="군만두와 쫄면 조합이 끝내주는 수원 대표 야구장 먹거리",
            is_famous=True,
            recommended_by="팬 추천",
        )
        session.add(food)
        session.commit()

        # Query and assert
        db_food = session.query(StadiumFood).filter_by(restaurant_name="보영만두").first()
        assert db_food is not None
        assert db_food.stadium_name == "수원 kt wiz 파크"
        assert db_food.menu_item == "군만두 / 쫄면"
        assert db_food.is_famous is True
        assert db_food.recommended_by == "팬 추천"


def test_stadium_food_unique_constraint():
    """Verify that the unique constraint on (stadium_name, restaurant_name, menu_item) is enforced."""
    engine = create_engine("sqlite:///:memory:")
    StadiumFood.__table__.create(engine)

    Session = sessionmaker(bind=engine)
    with Session() as session:
        # Insert first record
        food1 = StadiumFood(stadium_name="잠실야구장", restaurant_name="잠실원샷", menu_item="원샷치킨")
        session.add(food1)
        session.commit()

        # Insert duplicate record (should fail)
        food2 = StadiumFood(
            stadium_name="잠실야구장", restaurant_name="잠실원샷", menu_item="원샷치킨", description="다른 설명"
        )
        session.add(food2)
        with pytest.raises(IntegrityError):
            session.commit()


def test_seed_stadium_foods():
    """Verify that seed_stadium_foods correctly parses and seeds CSV data."""
    engine = create_engine("sqlite:///:memory:")
    StadiumFood.__table__.create(engine)

    # Let's locate the real stadium_foods.csv file to test seeding
    # For testing, we can check if it exists, or create a mock CSV
    test_csv_path = "data/stadium_foods.csv"

    Session = sessionmaker(bind=engine)
    with Session() as session:
        if os.path.exists(test_csv_path):
            seed_stadium_foods(session, test_csv_path)

            # Assert that foods are inserted
            count = session.query(StadiumFood).count()
            assert count > 0

            # Verify specific record
            jamsil_one_shot = (
                session.query(StadiumFood).filter_by(stadium_name="잠실야구장", restaurant_name="잠실원샷").first()
            )
            assert jamsil_one_shot is not None
            assert jamsil_one_shot.is_famous is True

            sajik_songheon = (
                session.query(StadiumFood).filter_by(stadium_name="부산 사직 야구장", restaurant_name="송헌집").first()
            )
            assert sajik_songheon is not None
            assert sajik_songheon.is_famous is True
            assert "미슐랭" in sajik_songheon.description
        else:
            pytest.skip("data/stadium_foods.csv does not exist, skipping integration seed test.")
