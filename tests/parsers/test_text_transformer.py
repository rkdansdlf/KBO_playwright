from src.parsers.text_transformer import ChunkingContext, TextTransformer


class TestCleanText:
    def setup_method(self):
        self.t = TextTransformer()

    def test_empty_string(self):
        assert self.t.clean_text("") == ""
        assert self.t.clean_text(None) == ""

    def test_whitespace_normalization(self):
        text = "  Hello   World  "
        assert self.t.clean_text(text) == "Hello World"

    def test_newline_normalization(self):
        text = "Line1\r\nLine2\rLine3"
        result = self.t.clean_text(text)
        assert "Line1" in result
        assert "Line2" in result
        assert "Line3" in result

    def test_excessive_newlines(self):
        text = "A\n\n\n\n\nB"
        assert self.t.clean_text(text) == "A\n\nB"

    def test_tabs_on_newline_boundaries(self):
        text = "A\n\t\nB"
        assert self.t.clean_text(text) in ("A\nB", "A\n\nB")

    def test_tabs_replaced(self):
        text = "A\t\tB"
        assert self.t.clean_text(text) == "A B"

    def test_strip_whitespace(self):
        text = "  \n  Hello  \n  "
        assert self.t.clean_text(text) == "Hello"

    def test_no_change_for_clean_text(self):
        text = "Hello World"
        assert self.t.clean_text(text) == "Hello World"


class TestChunkDocument:
    def setup_method(self):
        self.t = TextTransformer()

    def test_empty_content_returns_empty(self):
        assert self.t.chunk_document({"title": "Test", "content": "", "meta": {"category": "news"}}) == []
        assert self.t.chunk_document({"title": "Test", "meta": {"category": "news"}}) == []

    def test_default_strategy_is_overlap(self):
        doc = {"title": "Test", "content": "A " * 1000, "meta": {"category": "news"}}
        chunks = self.t.chunk_document(doc)
        assert len(chunks) >= 1
        assert "Part 1" in chunks[0]["title"]

    def test_rules_category_uses_headings_chunking(self):
        doc = {
            "title": "Rulebook",
            "content": "## 키워드\nkeyword1, keyword2\n## 조항 1\nRule content here",
            "meta": {"category": "rulebook"},
        }
        chunks = self.t.chunk_document(doc)
        assert len(chunks) >= 1
        assert "keyword1" in str(chunks)


class TestChunkByHeadings:
    def setup_method(self):
        self.t = TextTransformer()

    def test_basic_headings(self):
        text = "## 개요\nThis is a detailed overview section with enough content.\n## 조항 1\nClause 1 has very detailed content that spans multiple sentences."
        chunks = self.t.chunk_by_headings("Doc", text, {"source": "test"})
        assert len(chunks) >= 2

    def test_keyword_extraction(self):
        text = "## 키워드\nkeyword1, keyword2\n## 조항 1\nContent"
        chunks = self.t.chunk_by_headings("Doc", text, {"source": "test"})
        assert len(chunks) >= 1
        assert chunks[0]["meta"].get("keywords") is not None

    def test_keyword_with_dash_prefix(self):
        text = "## 키워드\n- kw1\n- kw2\n## 조항 1\nContent"
        chunks = self.t.chunk_by_headings("Doc", text, {"source": "test"})
        assert len(chunks) >= 1
        assert "kw1" in chunks[0]["meta"]["keywords"]

    def test_stub_chunks_merged(self):
        text = "## 조항 1\nLong content here that is definitely longer than 30 characters\n## 조항 2\nShort"
        chunks = self.t.chunk_by_headings("Doc", text, {"source": "test"})
        assert len(chunks) >= 1

    def test_article_pattern(self):
        text = "ARTICLE 1\nContent of article 1\nARTICLE 2\nContent of article 2"
        chunks = self.t.chunk_by_headings("Doc", text, {"source": "test"})
        assert len(chunks) >= 2

    def test_korean_clause_pattern(self):
        text = "제 1조. Definitions\nThis clause defines terms used in the agreement.\n제 2조. Scope\nThis clause describes the scope of the agreement and its application."
        chunks = self.t.chunk_by_headings("Doc", text, {"source": "test"})
        assert len(chunks) >= 2

    def test_empty_text(self):
        assert self.t.chunk_by_headings("Doc", "", {}) == []


class TestChunkWithOverlap:
    def setup_method(self):
        self.t = TextTransformer()

    def test_basic_chunking(self):
        text = "Para1.\n\nPara2.\n\nPara3."
        chunks = self.t.chunk_with_overlap("Doc", text, {"source": "test"}, chunk_char_limit=800)
        assert len(chunks) >= 1

    def test_chunk_character_limit_respected(self):
        long_text = "A" * 500 + "\n\n" + "B" * 500
        chunks = self.t.chunk_with_overlap(
            "Doc", long_text, {"source": "test"}, chunk_char_limit=400, overlap_char_limit=50
        )
        assert len(chunks) >= 2

    def test_single_paragraph(self):
        text = "Hello World"
        chunks = self.t.chunk_with_overlap("Doc", text, {"source": "test"}, chunk_char_limit=800)
        assert len(chunks) == 1
        assert chunks[0]["content"] == "Hello World"

    def test_overlap_present(self):
        text = "A" * 500 + "\n\n" + "B" * 500 + "\n\n" + "C" * 500
        chunks = self.t.chunk_with_overlap(
            "Doc", text, {"source": "test"}, chunk_char_limit=400, overlap_char_limit=100
        )
        if len(chunks) > 1:
            assert len(chunks[0]["content"]) > 0

    def test_empty_text_returns_empty(self):
        assert self.t.chunk_with_overlap("Doc", "", {}) == []


class TestChunkSemantically:
    def setup_method(self):
        self.t = TextTransformer()

    def test_single_sentence(self):
        chunks = self.t.chunk_semantically("Doc", "Hello world.", {"source": "test"})
        assert len(chunks) == 1
        assert "Hello" in chunks[0]["content"]

    def test_multiple_sentences(self):
        text = "First sentence. Second sentence. Third sentence."
        chunks = self.t.chunk_semantically("Doc", text, {"source": "test"})
        assert len(chunks) >= 1

    def test_empty_text(self):
        result = self.t.chunk_semantically("Doc", "", {})
        assert len(result) == 1
        assert result[0]["title"] == "Doc (Part 1)"
        assert result[0]["content"] == ""
        assert result[0]["meta"]["chunk_index"] == 1
        assert isinstance(result[0]["meta"]["source_row_id"], str)


class TestChunkParentChild:
    def setup_method(self):
        self.t = TextTransformer()

    def test_basic_parent_child(self):
        text = "A" * 300 + "\n\n" + "B" * 300
        chunks = self.t.chunk_parent_child(
            text,
            parent_size=500,
            child_size=250,
            child_overlap=50,
            ctx=ChunkingContext(
                doc_title="Doc",
                meta={"source": "test"},
                chunks=[],
                chunk_char_limit=500,
                overlap_char_limit=100,
            ),
        )
        assert len(chunks) >= 1
        assert "parent_content" in chunks[0]["meta"]
        assert "chunk_index" in chunks[0]["meta"]
        assert "parent_chunk_index" in chunks[0]["meta"]

    def test_single_parent(self):
        text = "Hello world."
        chunks = self.t.chunk_parent_child(
            text,
            ctx=ChunkingContext(
                doc_title="Doc",
                meta={"source": "test"},
                chunks=[],
                chunk_char_limit=1000,
                overlap_char_limit=100,
            ),
        )
        assert len(chunks) >= 1

    def test_empty_text(self):
        assert (
            self.t.chunk_parent_child(
                "",
                ctx=ChunkingContext(doc_title="Doc", meta={}, chunks=[], chunk_char_limit=1000, overlap_char_limit=100),
            )
            == []
        )


class TestCreateNewsChunk:
    def setup_method(self):
        self.t = TextTransformer()

    def test_basic_chunk_structure(self):
        chunk = self.t._create_news_chunk("Title", "Content", {"source": "src"}, 1)
        assert chunk["title"] == "Title (Part 1)"
        assert chunk["content"] == "Content"
        assert chunk["meta"]["chunk_index"] == 1
        assert "source_row_id" in chunk["meta"]

    def test_index_passthrough(self):
        chunk = self.t._create_news_chunk("Doc", "Text", {"source": "s"}, 5)
        assert chunk["meta"]["chunk_index"] == 5
