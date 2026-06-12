from src.parsers.food_parser import parse_food
from src.parsers.parking_parser import parse_parking
from src.parsers.registry import (
    PARSED_DOMAINS_NESTED,
    PARSER_REGISTRY,
    get_parser,
    is_nested_domain,
)
from src.parsers.roster_parser import parse_mobile_roster
from src.parsers.seat_parser import parse_seat_sections
from src.parsers.team_event_parser import parse_team_events
from src.parsers.ticket_parser import parse_ticket_page


class TestParserRegistry:
    def test_registry_contains_all_team_events(self):
        assert PARSER_REGISTRY["lg_twins_events"] is parse_team_events
        assert PARSER_REGISTRY["hanwha_eagles_events"] is parse_team_events
        assert PARSER_REGISTRY["doosan_bears_events"] is parse_team_events
        assert PARSER_REGISTRY["ssg_landers_events"] is parse_team_events
        assert PARSER_REGISTRY["nc_dinos_events"] is parse_team_events
        assert PARSER_REGISTRY["kia_tigers_events"] is parse_team_events
        assert PARSER_REGISTRY["lotte_giants_events"] is parse_team_events
        assert PARSER_REGISTRY["samsung_lions_events"] is parse_team_events
        assert PARSER_REGISTRY["kt_wiz_events"] is parse_team_events
        assert PARSER_REGISTRY["kiwoom_heroes_events"] is parse_team_events

    def test_registry_contains_all_ticket_entries(self):
        ticket_keys = [
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
        for key in ticket_keys:
            assert PARSER_REGISTRY[key] is parse_ticket_page, f"Missing {key}"

    def test_registry_contains_seat_entries(self):
        assert PARSER_REGISTRY["lg_twins_seat"] is parse_seat_sections
        assert PARSER_REGISTRY["seoul_stadium_seat"] is parse_seat_sections

    def test_registry_contains_parking_entries(self):
        assert PARSER_REGISTRY["ssg_landers_parking"] is parse_parking
        assert PARSER_REGISTRY["daegu_parking"] is parse_parking
        assert PARSER_REGISTRY["jamsil_parking_official"] is parse_parking

    def test_registry_contains_food_entries(self):
        assert PARSER_REGISTRY["lotte_giants_fnb"] is parse_food
        assert PARSER_REGISTRY["nc_dinos_food_seat"] is parse_food
        assert PARSER_REGISTRY["gujangfood_com"] is parse_food

    def test_registry_contains_roster_entries(self):
        assert PARSER_REGISTRY["kbo_today_roster"] is parse_mobile_roster
        assert PARSER_REGISTRY["kbo_player_register"] is parse_mobile_roster
        assert PARSER_REGISTRY["kbo_player_movement"] is parse_mobile_roster

    def test_registry_ticket_map_entry(self):
        result = PARSER_REGISTRY["kbo_ticket_map"]("<html></html>", "kbo_ticket_map")
        assert result == []

    def test_get_parser_known_key(self):
        assert get_parser("lg_twins_events") is parse_team_events

    def test_get_parser_unknown_key(self):
        assert get_parser("nonexistent_key") is None

    def test_get_parser_empty_string(self):
        assert get_parser("") is None

    def test_registry_size(self):
        assert len(PARSER_REGISTRY) >= 30


class TestNestedDomain:
    def test_nested_domains(self):
        assert is_nested_domain("parking") is True
        assert is_nested_domain("food") is True

    def test_non_nested_domains(self):
        assert is_nested_domain("events") is False
        assert is_nested_domain("ticket") is False
        assert is_nested_domain("roster") is False
        assert is_nested_domain("unknown") is False
        assert is_nested_domain("") is False

    def test_parsed_domains_nested_set(self):
        assert {"parking", "food"} == PARSED_DOMAINS_NESTED
