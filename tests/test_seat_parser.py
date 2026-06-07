from src.parsers.seat_parser import SECTION_PATTERNS, STADIUM_FROM_SOURCE_KEY, parse_seat_sections


class TestSeatParser:
    def test_section_pattern_matches_seats(self):
        text = "블루석 오렌지석 레드석 1층"
        matches = []
        for p in SECTION_PATTERNS:
            matches.extend(p.findall(text))
        assert len(matches) >= 3

    def test_parse_lg_seats(self):
        html = """
        <html><body>
        <p>블루석: 일반석, 오렌지석: 프리미엄, 레드석: 스페셜</p>
        <p>1층 테이블석: 150,000원, 2층 지정석: 100,000원</p>
        </body></html>
        """
        result = parse_seat_sections(html, "lg_twins_seat")
        names = [s["section_name"] for s in result]
        assert len(names) >= 3
        assert "블루석" in names

    def test_unknown_source_key_defaults_stadium(self):
        result = parse_seat_sections("<html></html>", "unknown")
        assert len(result) == 0

    def test_output_schema(self):
        html = "<html><body><p>블루석</p></body></html>"
        result = parse_seat_sections(html, "lg_twins_seat")
        assert len(result) > 0
        item = result[0]
        assert item["stadium_id"] == "JAMSIL"
        assert item["section_name"]
        assert item["section_code"] == item["section_name"]
        assert item["seat_grade"] == item["section_name"]
        assert item["source_id"] is None

    def test_deduplicates_sections(self):
        html = "<html><body><p>블루석 블루석 블루석</p><p>오렌지석</p></body></html>"
        result = parse_seat_sections(html, "lg_twins_seat")
        names = [s["section_name"] for s in result]
        assert names.count("블루석") == 1

    def test_skips_short_names(self):
        html = "<html><body><p>석 존 zone Zone</p></body></html>"
        result = parse_seat_sections(html, "lg_twins_seat")
        assert len(result) == 0


class TestSeatConstants:
    def test_stadium_mappings(self):
        assert STADIUM_FROM_SOURCE_KEY["lg_twins_seat"] == "JAMSIL"
        assert STADIUM_FROM_SOURCE_KEY["seoul_stadium_seat"] == "JAMSIL"
