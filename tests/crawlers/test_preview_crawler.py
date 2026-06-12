from src.crawlers.preview_crawler import PreviewCrawler


class TestCoerceApiPayload:
    def setup_method(self):
        self.crawler = PreviewCrawler()

    def test_json_string(self):
        assert self.crawler._coerce_api_payload('{"key": "val"}') == {"key": "val"}

    def test_aspnet_wrapper(self):
        result = self.crawler._coerce_api_payload({"d": [1, 2, 3]})
        assert result == [1, 2, 3]

    def test_none_returns_none(self):
        assert self.crawler._coerce_api_payload(None) is None

    def test_invalid_json_string_returns_none(self):
        assert self.crawler._coerce_api_payload("{invalid}") is None


class TestToFlag:
    def setup_method(self):
        self.crawler = PreviewCrawler()

    def test_numeric_flags(self):
        assert self.crawler._to_flag(1) is True
        assert self.crawler._to_flag(0) is False

    def test_string_flags(self):
        assert self.crawler._to_flag("1") is True
        assert self.crawler._to_flag("0") is False

    def test_truthy_strings(self):
        assert self.crawler._to_flag("true") is True
        assert self.crawler._to_flag("false") is False

    def test_none_is_false(self):
        assert self.crawler._to_flag(None) is False


class TestCleanText:
    def setup_method(self):
        self.crawler = PreviewCrawler()

    def test_removes_whitespace(self):
        assert self.crawler._clean_text("  hello  ") == "hello"

    def test_none_returns_empty(self):
        assert self.crawler._clean_text(None) == ""

    def test_already_clean(self):
        assert self.crawler._clean_text("hello") == "hello"
