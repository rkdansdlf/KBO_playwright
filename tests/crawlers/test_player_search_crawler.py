import pytest
from datetime import date
from dataclasses import dataclass

from src.crawlers.player_search_crawler import parse_birth_date, player_row_to_dict


@dataclass
class FakePlayerRow:
    player_id: int
    uniform_no: str | None
    name: str
    team: str | None
    position: str | None
    birth_date: str | None
    height_cm: int | None
    weight_kg: int | None
    career: str | None


class TestParseBirthDate:
    def test_iso_format(self):
        assert parse_birth_date("1990-05-15") == date(1990, 5, 15)

    def test_dot_format(self):
        assert parse_birth_date("1990.05.15") == date(1990, 5, 15)

    def test_slash_format(self):
        assert parse_birth_date("1990/05/15") == date(1990, 5, 15)

    def test_compact_format(self):
        assert parse_birth_date("19900515") == date(1990, 5, 15)

    def test_two_digit_year(self):
        assert parse_birth_date("90-05-15") == date(1990, 5, 15)

    def test_none_returns_none(self):
        assert parse_birth_date(None) is None

    def test_empty_string_returns_none(self):
        assert parse_birth_date("") is None

    def test_invalid_date_returns_none(self):
        assert parse_birth_date("not-a-date") is None


class TestPlayerRowToDict:
    def test_active_player(self):
        row = FakePlayerRow(
            player_id=12345,
            uniform_no="7",
            name="홍길동",
            team="LG",
            position="내야수",
            birth_date="1990-05-15",
            height_cm=180,
            weight_kg=80,
            career="KBO",
        )
        result = player_row_to_dict(row)
        assert result["player_id"] == 12345
        assert result["name"] == "홍길동"
        assert result["team"] == "LG"
        assert result["status"] == "active"
        assert result["staff_role"] is None

    def test_staff_player(self):
        row = FakePlayerRow(
            player_id=99999,
            uniform_no=None,
            name="김감독",
            team="SS",
            position="감독",
            birth_date=None,
            height_cm=None,
            weight_kg=None,
            career=None,
        )
        result = player_row_to_dict(row)
        assert result["status"] == "staff"
        assert result["staff_role"] == "manager"

    def test_birth_date_parsed(self):
        row = FakePlayerRow(
            player_id=1,
            uniform_no=None,
            name="A",
            team="NC",
            position="투수",
            birth_date="1992-03-20",
            height_cm=None,
            weight_kg=None,
            career=None,
        )
        result = player_row_to_dict(row)
        assert result["birth_date_date"] == date(1992, 3, 20)
