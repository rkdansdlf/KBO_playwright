"""Tests for parser instantiation and basic functionality."""

from __future__ import annotations

import pytest

from src.parsers.base_parser import BaseStadiumParser
from src.parsers.food_parser import parse_food
from src.parsers.parking_parser import parse_parking
from src.parsers.seat_parser import parse_seat_sections
from src.parsers.ticket_parser import parse_ticket_page


class TestBaseStadiumParser:
    def test_not_implemented(self):
        parser = BaseStadiumParser(html="", source_key="test")
        with pytest.raises(NotImplementedError):
            parser.parse()


class TestFoodParser:
    def test_empty_html(self):
        result = parse_food("", "test")
        assert result == [] or isinstance(result, list)

    def test_invalid_html(self):
        result = parse_food("<html><body></body></html>", "test")
        assert isinstance(result, list)


class TestParkingParser:
    def test_empty_html(self):
        result = parse_parking("", "test")
        assert result == [] or isinstance(result, list)

    def test_invalid_html(self):
        result = parse_parking("<html><body></body></html>", "test")
        assert isinstance(result, list)


class TestSeatParser:
    def test_empty_html(self):
        result = parse_seat_sections("", "test")
        assert result == [] or isinstance(result, list)

    def test_invalid_html(self):
        result = parse_seat_sections("<html><body></body></html>", "test")
        assert isinstance(result, list)


class TestTicketParser:
    def test_empty_html(self):
        result = parse_ticket_page("", "test", {})
        assert result == [] or isinstance(result, list)

    def test_invalid_html(self):
        result = parse_ticket_page("<html><body></body></html>", "test", {})
        assert isinstance(result, list)
