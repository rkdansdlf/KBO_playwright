import os
import unittest
from datetime import date
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

# Setup test imports
from src.models.base import Base
from src.models.player import PlayerBasic
from scripts.backfill_birthdates import _parse_birth_date, backfill


class TestBirthdateParsing(unittest.TestCase):
    def test_parse_birth_date_standard(self):
        self.assertEqual(_parse_birth_date("1989-01-10"), date(1989, 1, 10))
        self.assertEqual(_parse_birth_date("1997.11.14"), date(1997, 11, 14))
        self.assertEqual(_parse_birth_date("2002/07/05"), date(2002, 7, 5))
        self.assertEqual(_parse_birth_date("20011031"), date(2001, 10, 31))

    def test_parse_birth_date_two_digit_years(self):
        self.assertEqual(_parse_birth_date("97-11-14"), date(1997, 11, 14))
        self.assertEqual(_parse_birth_date("02.07.05"), date(2002, 7, 5))
        self.assertEqual(_parse_birth_date("15/03/12"), date(2015, 3, 12))

    def test_parse_birth_date_korean(self):
        self.assertEqual(_parse_birth_date("1989년 11월 21일"), date(1989, 11, 21))
        self.assertEqual(_parse_birth_date("2002년02월15일"), date(2002, 2, 15))

    def test_parse_birth_date_single_digit(self):
        self.assertEqual(_parse_birth_date("1990.7.3"), date(1990, 7, 3))
        self.assertEqual(_parse_birth_date("1991년 1월 5일"), date(1991, 1, 5))
        self.assertEqual(_parse_birth_date("1988/9/8"), date(1988, 9, 8))

    def test_parse_birth_date_invalid(self):
        self.assertIsNone(_parse_birth_date(""))
        self.assertIsNone(_parse_birth_date("-"))
        self.assertIsNone(_parse_birth_date("None"))
        self.assertIsNone(_parse_birth_date("1990.13.45"))
        self.assertIsNone(_parse_birth_date("not a date"))


class TestBirthdateBackfillIntegration(unittest.TestCase):
    def setUp(self):
        # Create an in-memory SQLite DB
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

        # Mock SessionLocal in backfill_birthdates module
        import scripts.backfill_birthdates

        self.original_session_local = scripts.backfill_birthdates.SessionLocal
        scripts.backfill_birthdates.SessionLocal = self.Session

    def tearDown(self):
        # Restore SessionLocal
        import scripts.backfill_birthdates

        scripts.backfill_birthdates.SessionLocal = self.original_session_local

    def test_integration_backfill(self):
        # Populate DB with mock data
        with self.Session() as session:
            p1 = PlayerBasic(player_id=1, name="HitterA", birth_date="1990-05-12")
            p2 = PlayerBasic(player_id=2, name="PitcherB", birth_date="1995.7.3")
            p3 = PlayerBasic(
                player_id=3, name="InfielderC", birth_date="1998년 10월 22일"
            )
            p4 = PlayerBasic(player_id=4, name="OutfielderD", birth_date="-")
            p5 = PlayerBasic(player_id=5, name="CoachE", birth_date=None)

            session.add_all([p1, p2, p3, p4, p5])
            session.commit()

        # Run backfill
        count = backfill()
        self.assertEqual(count, 3)

        # Verify DB updates
        with self.Session() as session:
            res = session.execute(
                select(PlayerBasic).order_by(PlayerBasic.player_id)
            ).scalars().all()

            self.assertEqual(res[0].birth_date_date, date(1990, 5, 12))
            self.assertEqual(res[1].birth_date_date, date(1995, 7, 3))
            self.assertEqual(res[2].birth_date_date, date(1998, 10, 22))
            self.assertIsNone(res[3].birth_date_date)
            self.assertIsNone(res[4].birth_date_date)


if __name__ == "__main__":
    unittest.main()
