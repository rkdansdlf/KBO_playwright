import pytest

from src.parsers.parking_parser import parse_parking, PARKING_FEE_PATTERN, STADIUM_FROM_SOURCE_KEY


class TestParkingParser:
    def test_fee_pattern_matches(self):
        text = "기본요금: 5,000원 추가요금 3,000원 무료"
        matches = PARKING_FEE_PATTERN.findall(text)
        assert len(matches) == 2
        assert matches[0] == ("기본", "5,000")

    def test_parse_ssg_parking(self):
        html = """
        <html><body>
        <h1>주차장 안내</h1>
        <p>기본요금: 5,000원</p>
        <p>추가요금: 3,000원</p>
        <p>경기일 무료 주차</p>
        </body></html>
        """
        result = parse_parking(html, "ssg_landers_parking")
        assert len(result) == 1
        lot = result[0]
        assert lot["lot"]["stadium_id"] == "MUNHAK"
        assert "MUNHAK" in lot["lot"]["name"]
        assert len(lot["fee_rules"]) >= 2

    def test_unknown_source_key_defaults(self):
        result = parse_parking("<html></html>", "unknown")
        assert len(result) == 1
        assert result[0]["lot"]["stadium_id"] == "UNKNOWN"

    def test_empty_html_returns_no_fees(self):
        result = parse_parking("<html><body><p>주차장 안내</p></body></html>", "ssg_landers_parking")
        assert len(result) == 1
        assert result[0]["fee_rules"] == []

    def test_output_schema(self):
        html = "<html><body><p>기본요금: 5,000원</p></body></html>"
        result = parse_parking(html, "ssg_landers_parking")
        assert len(result) == 1
        entry = result[0]
        assert "lot" in entry
        assert "fee_rules" in entry
        lot = entry["lot"]
        assert lot["stadium_id"] == "MUNHAK"
        assert lot["lot_type"] == "official"
        assert lot["is_event_day_available"] is True
        assert lot["reservation_required"] is False
        assert len(entry["fee_rules"]) == 1
        assert entry["fee_rules"][0]["label"] == "기본"
        assert entry["fee_rules"][0]["amount"] == 5000


class TestParkingConstants:
    def test_stadium_mappings(self):
        assert STADIUM_FROM_SOURCE_KEY["ssg_landers_parking"] == "MUNHAK"
        assert STADIUM_FROM_SOURCE_KEY["daegu_parking"] == "DAEGU"
        assert STADIUM_FROM_SOURCE_KEY["jamsil_parking_official"] == "JAMSIL"
