from src.parsers.base_parser import BaseStadiumParser


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
        try:
            parser.parse()
            raise AssertionError("Should have raised NotImplementedError")
        except NotImplementedError:
            pass

    def test_text_multiline_gets_joined(self):
        html = "<html><body><p>Line 1</p><p>Line 2</p></body></html>"
        parser = BaseStadiumParser(html, "key")
        assert parser.text == "Line 1 Line 2"

    def test_text_with_special_chars(self):
        html = "<html><body><div>가나다 123 !@#</div></body></html>"
        parser = BaseStadiumParser(html, "key")
        assert "가나다" in parser.text
