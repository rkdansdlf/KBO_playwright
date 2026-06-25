from __future__ import annotations

from src.parsers.registry import (
    PARSER_REGISTRY,
    _parse_ticket_map,
    get_parser,
)


class TestParserRegistry:
    def test_all_team_events_registered(self):
        expected_keys = [
            "lg_twins_events",
            "hanwha_eagles_events",
            "doosan_bears_events",
            "ssg_landers_events",
            "nc_dinos_events",
            "kia_tigers_events",
            "lotte_giants_events",
            "samsung_lions_events",
            "kt_wiz_events",
            "kiwoom_heroes_events",
        ]
        for key in expected_keys:
            assert key in PARSER_REGISTRY, f"Missing: {key}"

    def test_all_team_tickets_registered(self):
        expected_keys = [
            "lg_twins_ticket",
            "hanwha_eagles_ticket",
            "samsung_lions_ticket",
            "kt_wiz_ticket",
            "doosan_bears_ticket",
            "lotte_giants_ticket",
            "kia_tigers_ticket",
            "nc_dinos_ticket",
            "ssg_landers_ticket",
            "kiwoom_heroes_ticket",
        ]
        for key in expected_keys:
            assert key in PARSER_REGISTRY, f"Missing: {key}"

    def test_kbo_ticket_map_registered(self):
        assert "kbo_ticket_map" in PARSER_REGISTRY

    def test_total_count(self):
        assert len(PARSER_REGISTRY) >= 20


class TestGetParser:
    def test_returns_callable_for_valid_key(self):
        parser = get_parser("lg_twins_events")
        assert callable(parser)

    def test_returns_none_for_unknown_key(self):
        assert get_parser("nonexistent_key") is None

    def test_returns_none_for_empty_string(self):
        assert get_parser("") is None


class TestParseTicketMap:
    def test_returns_empty_list(self):
        result = _parse_ticket_map("<html></html>", "kbo_ticket_map")
        assert result == []

    def test_returns_empty_list_with_none_metadata(self):
        result = _parse_ticket_map("<html></html>", "kbo_ticket_map", None)
        assert result == []
