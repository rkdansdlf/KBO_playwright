
from src.parsers.seat_parser import (
    SECTION_PATTERNS,
    STADIUM_FROM_SOURCE_KEY,
    SeatSectionParser,
    parse_seat_sections,
)


class TestSeatSectionParser:
    def test_parse_lg_seats(self):
        html = """
        <html><body>
        <p>블루석: 일반석, 오렌지석: 프리미엄</p>
        <p>1층 레드석: 50,000원, 네이비석: 40,000원</p>
        </body></html>
        """
        result = parse_seat_sections(html, "lg_twins_seat")
        names = [s["section_name"] for s in result]
        assert "블루석" in names
        assert "오렌지석" in names
        assert len(names) >= 2

    def test_unknown_source_key_defaults_unknown(self):
        result = parse_seat_sections("<html></html>", "unknown")
        assert len(result) == 0

    def test_output_schema(self):
        html = "<html><body><p>블루석</p></body></html>"
        result = parse_seat_sections(html, "lg_twins_seat")
        assert len(result) > 0
        item = result[0]
        assert item["stadium_id"] == "JAMSIL"
        assert item["section_name"] == "블루석"
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

    def test_empty_html_returns_empty(self):
        result = parse_seat_sections("", "lg_twins_seat")
        assert result == []

    def test_seoul_stadium_maps_to_jamsil(self):
        html = "<html><body><p>레드석</p></body></html>"
        result = parse_seat_sections(html, "seoul_stadium_seat")
        assert result[0]["stadium_id"] == "JAMSIL"

    def test_seat_section_parser_class_directly(self):
        html = "<html><body><p>골드석</p></body></html>"
        parser = SeatSectionParser(html, "lg_twins_seat")
        result = parser.parse()
        assert len(result) == 1
        assert result[0]["section_name"] == "골드석"

    def test_patterns_match_zone_variants(self):
        text = "프리미엄존 레드존 네이비존 블루zone"
        matches = []
        for p in SECTION_PATTERNS:
            matches.extend(p.findall(text))
        assert len(matches) >= 2
        assert "프리미엄존" in matches or "레드존" in matches or "네이비존" in matches or "블루zone" in matches

    def test_stadium_mappings_constants(self):
        assert STADIUM_FROM_SOURCE_KEY["lg_twins_seat"] == "JAMSIL"
        assert STADIUM_FROM_SOURCE_KEY["seoul_stadium_seat"] == "JAMSIL"

    def test_parse_with_metadata(self):
        html = "<html><body><p>블루석</p></body></html>"
        result = parse_seat_sections(html, "lg_twins_seat", {"extra": "data"})
        assert len(result) == 1


class TestSeatSectionPatterns:
    def test_hangul_suffix_pattern(self):
        p = SECTION_PATTERNS[0]
        assert p.search("블루석")
        assert p.search("오렌지석")
        assert p.search("지정석")
        assert p.search("프리미엄존")
        assert p.search("오렌지zone")
        assert p.search("블루Zone")
        assert not p.search("석")  # too short

    def test_color_pattern(self):
        p = SECTION_PATTERNS[1]
        assert p.search("블루석")
        assert p.search("오렌지존")
        assert p.search("레드석")
        assert p.search("네이비석")
        assert p.search("그린존")
        assert p.search("골드존")
        assert p.search("1F 지정석")
        assert p.search("2f 프리미엄존")
