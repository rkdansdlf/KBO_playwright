from src.parsers.parking_parser import (
    PARKING_FEE_PATTERN,
    STADIUM_FROM_SOURCE_KEY,
    parse_parking,
)


class TestParkingParser:
    def test_parse_basic_fees(self):
        html = "<html><body><p>기본요금: 5,000원 추가시간: 1,000원</p></body></html>"
        result = parse_parking(html, "ssg_landers_parking")
        assert len(result) == 1
        item = result[0]
        assert item["lot"]["stadium_id"] == "MUNHAK"
        assert item["lot"]["name"] == "MUNHAK 주차장"
        assert item["lot"]["lot_type"] == "official"
        assert len(item["fee_rules"]) == 2
        assert item["fee_rules"][0]["label"] == "기본"
        assert item["fee_rules"][0]["amount"] == 5000
        assert item["fee_rules"][1]["label"] == "추가"
        assert item["fee_rules"][1]["amount"] == 1000

    def test_parse_event_day_fee(self):
        html = "<html><body><p>행사요금: 10,000원 경기요금: 8,000원</p></body></html>"
        result = parse_parking(html, "daegu_parking")
        item = result[0]
        assert item["lot"]["stadium_id"] == "DAEGU"
        labels = [f["label"] for f in item["fee_rules"]]
        assert "행사" in labels
        assert "경기" in labels

    def test_unknown_source_key_defaults_unknown(self):
        result = parse_parking("<html></html>", "unknown")
        assert result[0]["lot"]["stadium_id"] == "UNKNOWN"

    def test_empty_html(self):
        result = parse_parking("", "ssg_landers_parking")
        assert len(result) == 1
        assert result[0]["fee_rules"] == []

    def test_no_fee_matches_returns_empty_fees(self):
        html = "<html><body><p>주차 가능합니다</p></body></html>"
        result = parse_parking(html, "ssg_landers_parking")
        assert len(result) == 1
        assert result[0]["fee_rules"] == []

    def test_output_schema(self):
        html = "<html><body><p>기본요금: 3,000원</p></body></html>"
        result = parse_parking(html, "jamsil_parking_official")
        item = result[0]
        lot = item["lot"]
        assert lot["stadium_id"] == "JAMSIL"
        assert lot["lot_type"] == "official"
        assert lot["is_event_day_available"] is True
        assert lot["reservation_required"] is False
        assert len(item["fee_rules"]) == 1

    def test_multiple_fee_amounts(self):
        html = "<html><body><p>기본 2,000원 추가 1,500원 일일 5,000원 무료 0원</p></body></html>"
        result = parse_parking(html, "ssg_landers_parking")
        assert len(result[0]["fee_rules"]) == 4
        amounts = [f["amount"] for f in result[0]["fee_rules"]]
        assert 2000 in amounts
        assert 1500 in amounts
        assert 5000 in amounts
        assert 0 in amounts

    def test_free_label_parsing(self):
        html = "<html><body><p>무료: 0원</p></body></html>"
        result = parse_parking(html, "ssg_landers_parking")
        assert result[0]["fee_rules"][0]["label"] == "무료"
        assert result[0]["fee_rules"][0]["amount"] == 0

    def test_large_fee_amounts(self):
        html = "<html><body><p>일일요금: 100,000원</p></body></html>"
        result = parse_parking(html, "ssg_landers_parking")
        assert result[0]["fee_rules"][0]["amount"] == 100000

    def test_with_metadata(self):
        html = "<html><body><p>기본: 5,000원</p></body></html>"
        result = parse_parking(html, "ssg_landers_parking", {"source": "test"})
        assert len(result) == 1

    def test_stadium_mappings(self):
        assert STADIUM_FROM_SOURCE_KEY["ssg_landers_parking"] == "MUNHAK"
        assert STADIUM_FROM_SOURCE_KEY["daegu_parking"] == "DAEGU"
        assert STADIUM_FROM_SOURCE_KEY["jamsil_parking_official"] == "JAMSIL"


class TestParkingFeePattern:
    def test_pattern_basic(self):
        m = PARKING_FEE_PATTERN.search("기본요금: 5,000원")
        assert m
        assert m.group(1) == "기본"
        assert m.group(2) == "5,000"

    def test_pattern_without_label_suffix(self):
        m = PARKING_FEE_PATTERN.search("추가 2,000원")
        assert m
        assert m.group(1) == "추가"

    def test_pattern_amount_without_commas(self):
        m = PARKING_FEE_PATTERN.search("기본: 3,000원")
        assert m
        assert m.group(2) == "3,000"

    def test_pattern_no_match_for_non_fee(self):
        assert not PARKING_FEE_PATTERN.search("주차장 안내")
        assert not PARKING_FEE_PATTERN.search("환영합니다")
