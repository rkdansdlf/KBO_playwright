from __future__ import annotations

import pytest

from src.parsers.base_parser import BaseHtmlParser, BaseJsonParser, BaseParser, BaseStadiumParser
from src.parsers.dto import ParseResult, ScheduleGameParsed


class SimpleConcreteParser(BaseParser[list[str]]):
    def parse(self) -> list[str]:
        if self.raw_content == "fail":
            raise ValueError("Intentional parse failure")
        return [str(self.raw_content)]


class SimpleHtmlParser(BaseHtmlParser[dict[str, str]]):
    def parse(self) -> dict[str, str]:
        title = self.select_text("h1")
        body = self.select_text(".content")
        return {"title": title, "body": body}


class SimpleJsonParser(BaseJsonParser[dict[str, int]]):
    def parse(self) -> dict[str, int]:
        if not isinstance(self.payload, dict):
            raise TypeError("Expected dict payload")
        return {k: int(v) for k, v in self.payload.items()}


class TestBaseParser:
    def test_base_parser_attributes(self):
        parser = SimpleConcreteParser("hello", source_key="test_src", metadata={"year": 2026})
        assert parser.raw_content == "hello"
        assert parser.source_key == "test_src"
        assert parser.metadata == {"year": 2026}
        assert parser.parse() == ["hello"]

    def test_parse_safe_success(self):
        parser = SimpleConcreteParser("success", source_key="src1")
        result = parser.parse_safe()
        assert isinstance(result, ParseResult)
        assert result.success is True
        assert result.data == ["success"]
        assert result.errors == []
        assert result.has_errors() is False

    def test_parse_safe_failure(self):
        parser = SimpleConcreteParser("fail", source_key="src2", metadata={"attempt": 1})
        result = parser.parse_safe()
        assert isinstance(result, ParseResult)
        assert result.success is False
        assert result.data == []
        assert len(result.errors) == 1
        assert "ValueError" in result.errors[0]
        assert result.has_errors() is True
        assert result.metadata == {"attempt": 1}


class TestBaseHtmlParser:
    def test_select_text_and_all_text(self):
        html = """
        <html>
            <body>
                <h1>KBO League</h1>
                <div class="content">2026 Season Schedule</div>
                <ul>
                    <li class="item">Team A</li>
                    <li class="item">Team B</li>
                </ul>
            </body>
        </html>
        """
        parser = SimpleHtmlParser(html, source_key="html_src")
        assert parser.select_text("h1") == "KBO League"
        assert parser.select_text(".content") == "2026 Season Schedule"
        assert parser.select_text(".nonexistent", default="N/A") == "N/A"
        assert parser.select_all_text(".item") == ["Team A", "Team B"]

        parsed = parser.parse()
        assert parsed == {"title": "KBO League", "body": "2026 Season Schedule"}

    def test_extract_table_rows(self):
        html = """
        <table>
            <thead><tr><th>Team</th><th>W</th><th>L</th></tr></thead>
            <tbody>
                <tr><td>LG</td><td>80</td><td>50</td></tr>
                <tr><td>KIA</td><td>75</td><td>55</td></tr>
            </tbody>
        </table>
        """
        parser = SimpleHtmlParser(html)
        table_elem = parser.soup.select_one("table")
        assert table_elem is not None
        rows = parser.extract_table_rows(table_elem)
        assert len(rows) == 2
        assert rows[0] == {"Team": "LG", "W": "80", "L": "50"}
        assert rows[1] == {"Team": "KIA", "W": "75", "L": "55"}


class TestBaseJsonParser:
    def test_parse_valid_json_string(self):
        parser = SimpleJsonParser('{"runs": "5", "hits": "10"}')
        data = parser.parse()
        assert data == {"runs": 5, "hits": 10}

    def test_parse_valid_dict(self):
        parser = SimpleJsonParser({"runs": 5, "hits": 10})
        data = parser.parse()
        assert data == {"runs": 5, "hits": 10}

    def test_parse_invalid_json_string(self):
        parser = SimpleJsonParser("not a json")
        result = parser.parse_safe()
        assert result.success is True
        assert result.data == {}


class TestBaseStadiumParser:
    def test_init_sets_soup_and_text(self):
        html = "<html><body><p>Hello World</p></body></html>"
        parser = BaseStadiumParser(html, "test_key", {"foo": "bar"})
        assert parser.source_key == "test_key"
        assert parser.metadata == {"foo": "bar"}
        assert parser.text == "Hello World"
        assert parser.soup is not None

    def test_init_empty_html(self):
        parser = BaseStadiumParser("", "empty_key")
        assert parser.source_key == "empty_key"
        assert parser.metadata == {}
        assert parser.text == ""

    def test_init_none_metadata_defaults_to_empty(self):
        parser = BaseStadiumParser("<html></html>", "key", None)
        assert parser.metadata == {}

    def test_parse_raises_not_implemented(self):
        parser = BaseStadiumParser("<html></html>", "key")
        with pytest.raises(NotImplementedError):
            parser.parse()

    def test_text_multiline_gets_joined(self):
        html = "<html><body><p>Line 1</p><p>Line 2</p></body></html>"
        parser = BaseStadiumParser(html, "key")
        assert parser.text == "Line 1 Line 2"

    def test_text_with_special_chars(self):
        html = "<html><body><div>가나다 123 !@#</div></body></html>"
        parser = BaseStadiumParser(html, "key")
        assert "가나다" in parser.text


class TestDTOStructures:
    def test_schedule_game_parsed(self):
        dto = ScheduleGameParsed(
            game_id="20260401LGOB0",
            game_date="2026-04-01",
            season=2026,
            away_team="LG",
            home_team="OB",
            stadium="JAMSIL",
            status="SCHEDULED",
        )
        assert dto.game_id == "20260401LGOB0"
        assert dto.season == 2026
        assert dto.status == "SCHEDULED"
