"""
Unit tests for TextTransformer cleansing and chunking logic.
"""

from __future__ import annotations

from src.parsers.text_transformer import TextTransformer


def test_clean_text():
    transformer = TextTransformer()

    # Test whitespace and tabs cleanup
    dirty_text = "  Hello \t  world! \n\n\n This is a   test.  "
    cleaned = transformer.clean_text(dirty_text)
    assert cleaned == "Hello world!\n\nThis is a test."

    # Test newline normalizations
    dirty_newlines = "Line 1\r\nLine 2\r\rLine 3\n\n\n\nLine 4"
    cleaned_newlines = transformer.clean_text(dirty_newlines)
    assert cleaned_newlines == "Line 1\nLine 2\n\nLine 3\n\nLine 4"


def test_chunk_by_headings():
    transformer = TextTransformer()

    doc = {
        "title": "KBO 공식 야구 규칙",
        "content": """
## 개요
이 문서는 KBO 규칙입니다.

### 조항 1: 피치 클락
투수는 18초 내 투구해야 한다.

제 2조: 몸에 맞는 공
타자는 피하려 노력해야 한다.
        """,
        "meta": {"source": "rules.pdf", "category": "rulebook"},
    }

    chunks = transformer.chunk_document(doc)

    # We expect 3 chunks based on headers: 개요, 조항 1, 제 2조
    assert len(chunks) == 3

    assert "개요" in chunks[0]["title"]
    assert "이 문서는 KBO 규칙입니다." in chunks[0]["content"]
    assert chunks[0]["meta"]["category"] == "rulebook"
    assert chunks[0]["meta"]["source_row_id"] is not None

    assert "조항 1" in chunks[1]["title"]
    assert "피치 클락" in chunks[1]["content"]

    assert "제 2조" in chunks[2]["title"]
    assert "몸에 맞는 공" in chunks[2]["content"]


def test_chunk_with_overlap():
    transformer = TextTransformer()

    # Create a text with several paragraphs
    paragraphs = [
        "First paragraph. " * 20,  # ~340 chars
        "Second paragraph. " * 20,  # ~360 chars
        "Third paragraph. " * 20,  # ~340 chars
        "Fourth paragraph. " * 20,  # ~360 chars
    ]
    doc = {
        "title": "KBO Daily News",
        "content": "\n\n".join(paragraphs),
        "meta": {"source": "https://naver.com/news/1", "category": "news"},
    }

    # Let's chunk it with limit 500 and overlap 100
    chunks = transformer.chunk_with_overlap(
        doc["title"], doc["content"], doc["meta"], chunk_char_limit=500, overlap_char_limit=100
    )

    # Since each paragraph is 340-360 chars, combining two goes over 500.
    # So each paragraph should roughly trigger a new chunk, with overlap text appended.
    assert len(chunks) >= 4

    # Check that chunks overlap (i.e. end of chunk 1 exists in chunk 2)
    chunk1_content = chunks[0]["content"]
    chunk2_content = chunks[1]["content"]

    # The last 100 characters of chunk 1 should overlap into the beginning of chunk 2
    overlap_part = chunk1_content[-80:]  # check smaller slice to avoid boundary spaces issue
    assert overlap_part in chunk2_content


def test_chunk_semantically(monkeypatch):
    transformer = TextTransformer()

    # Mock EmbeddingService.get_embeddings_batch to return predefined similarities
    # We have 3 sentences
    sentences = [
        "First sentence about baseball rules.",
        "Second sentence also about baseball pitches.",
        "Completely different topic about football stadiums.",
    ]
    doc = {
        "title": "Sports News",
        "content": " ".join(sentences),
        "meta": {"source": "https://sports.com", "category": "news"},
    }

    # Mock embeddings: first two similar (high dot product), third different (low dot product)
    mock_embeddings = [
        [1.0, 0.0],  # s1
        [0.9, 0.43],  # s2 (sim with s1 ~0.9)
        [0.1, 0.99],  # s3 (sim with s2 ~0.5)
    ]

    class MockEmbeddingService:
        def get_embeddings_batch(self, texts):
            return mock_embeddings[: len(texts)]

    # Inject mock
    monkeypatch.setattr("src.services.embedding_service.EmbeddingService", MockEmbeddingService)

    # Run with threshold 0.7. Should split before sentence 3.
    chunks = transformer.chunk_semantically(doc["title"], doc["content"], doc["meta"], similarity_threshold=0.7)

    # Expect 2 chunks: Chunk 1 has s1 & s2, Chunk 2 has s3
    assert len(chunks) == 2
    assert "First sentence" in chunks[0]["content"]
    assert "Second sentence" in chunks[0]["content"]
    assert "Completely different" in chunks[1]["content"]
    assert "First sentence" not in chunks[1]["content"]


def test_chunk_parent_child():
    transformer = TextTransformer()

    doc = {
        "title": "Detailed Rules",
        "content": "Paragraph one is short.\n\nParagraph two is also very short.",
        "meta": {"source": "rules.txt", "category": "news"},
    }

    # Run with small limits to trigger parent and child creation
    chunks = transformer.chunk_parent_child(
        doc["title"], doc["content"], doc["meta"], parent_size=50, child_size=25, child_overlap=5
    )

    assert len(chunks) > 0
    # Every child must have parent_content and parent_chunk_index in metadata
    for chunk in chunks:
        assert "parent_content" in chunk["meta"]
        assert "parent_chunk_index" in chunk["meta"]
        assert chunk["meta"]["parent_content"] is not None


# ─────────────────────────────────────────────────────────────────
# New tests: Phase 1 static crawler enhancement
# ─────────────────────────────────────────────────────────────────


def test_chunk_by_headings_pdf_style():
    """PDF-style clause text (no markdown #) should still be split correctly."""
    transformer = TextTransformer()

    doc = {
        "title": "KBO 공식 규칙서",
        "content": (
            "제 1조. 경기 진행\n"
            "투수는 18초 내 투구해야 한다.\n\n"
            "제 2조. 방해와 보크\n"
            "보크는 투수의 불법 투구 동작이다.\n\n"
            "ARTICLE 3 Interference\n"
            "Catcher interference rules apply.\n"
        ),
        "meta": {"source": "kbo_rules.pdf", "category": "rulebook"},
    }
    chunks = transformer.chunk_document(doc)

    # Expect 3 chunks: 제1조, 제2조, ARTICLE 3
    assert len(chunks) == 3, f"Expected 3 chunks, got {len(chunks)}: {[c['title'] for c in chunks]}"
    assert any("1조" in c["content"] or "1조" in c["title"] for c in chunks)
    assert any("2조" in c["content"] or "2조" in c["title"] for c in chunks)
    assert any("ARTICLE" in c["content"] or "ARTICLE" in c["title"] for c in chunks)


def test_chunk_by_headings_keyword_extraction():
    """'## 키워드' section should be extracted into meta['keywords'] list."""
    transformer = TextTransformer()

    doc = {
        "title": "KBO 야구 규칙",
        "content": (
            "## 개요\n"
            "야구 규칙 개요입니다.\n\n"
            "## 조항 1: 피치 클락\n"
            "투수는 18초 내 투구해야 한다.\n\n"
            "## 키워드\n"
            "피치클락, 18초, 23초\n"
            "ABS, 자동투구판정\n"
        ),
        "meta": {"source": "rules.md", "category": "rulebook"},
    }
    chunks = transformer.chunk_document(doc)

    # Keywords block should NOT become its own chunk
    titles = [c["title"] for c in chunks]
    assert not any("키워드" in t for t in titles), f"Keyword block should not be a standalone chunk, got: {titles}"

    # Every chunk should carry the keywords in meta
    for chunk in chunks:
        assert "keywords" in chunk["meta"], f"Missing keywords in meta for chunk '{chunk['title']}'"
        kws = chunk["meta"]["keywords"]
        assert isinstance(kws, list) and len(kws) > 0
        # Spot-check that at least one expected keyword is present
        all_kws = ", ".join(kws)
        assert "피치클락" in all_kws or "ABS" in all_kws, f"Expected keywords not found: {all_kws}"


def test_chunk_by_headings_stub_merge():
    """Chunks under 50 chars should be merged into the preceding chunk."""
    transformer = TextTransformer()

    doc = {
        "title": "규칙서",
        "content": (
            "## 개요\n이것은 충분히 긴 개요 섹션입니다. " * 5 + "\n\n## 짧음\n짧다.\n"  # Only 3 chars — stub chunk
        ),
        "meta": {"source": "test.md", "category": "rulebook"},
    }
    chunks = transformer.chunk_document(doc)

    # The stub chunk ("짧다.") should have been merged into "개요" chunk
    assert len(chunks) == 1, (
        f"Stub chunk should be merged into preceding chunk, got {len(chunks)} chunks: {[c['title'] for c in chunks]}"
    )
    assert "짧다" in chunks[0]["content"]
