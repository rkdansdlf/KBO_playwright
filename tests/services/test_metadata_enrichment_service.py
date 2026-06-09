from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.services.metadata_enrichment_service import MetadataEnrichmentService


class TestMetadataEnrichmentService:
    def test_enrich_chunk_disabled(self):
        with patch.dict("os.environ", {"ENABLE_METADATA_ENRICHMENT": "0"}, clear=True):
            svc = MetadataEnrichmentService()
            result = svc.enrich_chunk("Some content here that is long enough to pass.")
            assert result == {"summary": "", "keywords": [], "questions": []}

    def test_enrich_chunk_no_api_key(self):
        with patch.dict("os.environ", {}, clear=True):
            svc = MetadataEnrichmentService()
            assert not svc.enabled

    def test_enrich_short_content(self):
        with patch.dict("os.environ", {"ENABLE_METADATA_ENRICHMENT": "1", "GEMINI_API_KEY": "sk-or-v1-test"}):
            svc = MetadataEnrichmentService()
            result = svc.enrich_chunk("short")
            assert result == {"summary": "", "keywords": [], "questions": []}

    def test_enrich_blank_content(self):
        with patch.dict("os.environ", {"ENABLE_METADATA_ENRICHMENT": "1", "GEMINI_API_KEY": "sk-or-v1-test"}):
            svc = MetadataEnrichmentService()
            result = svc.enrich_chunk("   ")
            assert result == {"summary": "", "keywords": [], "questions": []}

    def test_parse_json_response_empty(self):
        svc = MetadataEnrichmentService()
        result = svc._parse_json_response("")
        assert result == {"summary": "", "keywords": [], "questions": []}

    def test_parse_json_response_valid(self):
        svc = MetadataEnrichmentService()
        text = '{"summary": "test", "keywords": ["a", "b"], "questions": ["q1"]}'
        result = svc._parse_json_response(text)
        assert result["summary"] == "test"
        assert result["keywords"] == ["a", "b"]
        assert result["questions"] == ["q1"]

    def test_parse_json_response_with_fences(self):
        svc = MetadataEnrichmentService()
        text = '```json\n{"summary": "test", "keywords": ["a"], "questions": []}\n```'
        result = svc._parse_json_response(text)
        assert result["summary"] == "test"
        assert result["keywords"] == ["a"]

    def test_parse_json_response_with_fences_no_lang(self):
        svc = MetadataEnrichmentService()
        text = '```\n{"summary": "test", "keywords": [], "questions": []}\n```'
        result = svc._parse_json_response(text)
        assert result["summary"] == "test"

    def test_parse_json_response_invalid_json(self):
        svc = MetadataEnrichmentService()
        result = svc._parse_json_response("{invalid}")
        assert result == {"summary": "", "keywords": [], "questions": []}

    def test_parse_json_response_non_list_keywords(self):
        svc = MetadataEnrichmentService()
        text = '{"summary": "x", "keywords": "bad", "questions": "bad"}'
        result = svc._parse_json_response(text)
        assert result["keywords"] == []
        assert result["questions"] == []

    def test_call_openrouter_success(self):
        with patch.dict("os.environ", {"ENABLE_METADATA_ENRICHMENT": "1", "GEMINI_API_KEY": "sk-or-v1-test"}):
            svc = MetadataEnrichmentService()
            with patch("httpx.Client") as MockClient:
                mock_instance = MagicMock()
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "choices": [{"message": {"content": '{"summary": "s", "keywords": ["k"], "questions": ["q"]}'}}]
                }
                mock_instance.post.return_value = mock_response
                MockClient.return_value.__enter__.return_value = mock_instance
                result = svc.enrich_chunk("This is a sufficiently long content to trigger enrichment call. " * 5)
                assert result["summary"] == "s"
                assert result["keywords"] == ["k"]

    def test_call_openrouter_api_error(self):
        with patch.dict("os.environ", {"ENABLE_METADATA_ENRICHMENT": "1", "GEMINI_API_KEY": "sk-or-v1-test"}):
            svc = MetadataEnrichmentService()
            with patch("httpx.Client") as MockClient:
                mock_instance = MagicMock()
                mock_response = MagicMock()
                mock_response.status_code = 429
                mock_response.text = "Rate limited"
                mock_instance.post.return_value = mock_response
                MockClient.return_value.__enter__.return_value = mock_instance
                result = svc.enrich_chunk("A" * 100)
                assert result == {"summary": "", "keywords": [], "questions": []}

    def test_call_openrouter_exception(self):
        with patch.dict("os.environ", {"ENABLE_METADATA_ENRICHMENT": "1", "GEMINI_API_KEY": "sk-or-v1-test"}):
            svc = MetadataEnrichmentService()
            with patch("httpx.Client") as MockClient:
                mock_instance = MagicMock()
                mock_instance.post.side_effect = Exception("network")
                MockClient.return_value.__enter__.return_value = mock_instance
                result = svc.enrich_chunk("A" * 100)
                assert result == {"summary": "", "keywords": [], "questions": []}

    def test_call_google_success(self):
        with patch.dict("os.environ", {"ENABLE_METADATA_ENRICHMENT": "1", "GEMINI_API_KEY": "AIza-test123"}):
            svc = MetadataEnrichmentService()
            with patch("httpx.Client") as MockClient:
                mock_instance = MagicMock()
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = {
                    "candidates": [{"content": {"parts": [{"text": '{"summary": "s", "keywords": ["k"], "questions": ["q"]}'}]}}]
                }
                mock_instance.post.return_value = mock_response
                MockClient.return_value.__enter__.return_value = mock_instance
                result = svc.enrich_chunk("A" * 100)
                assert result["summary"] == "s"
                assert result["keywords"] == ["k"]

    def test_call_google_api_error(self):
        with patch.dict("os.environ", {"ENABLE_METADATA_ENRICHMENT": "1", "GEMINI_API_KEY": "AIza-test123"}):
            svc = MetadataEnrichmentService()
            with patch("httpx.Client") as MockClient:
                mock_instance = MagicMock()
                mock_response = MagicMock()
                mock_response.status_code = 400
                mock_response.text = "Bad Request"
                mock_instance.post.return_value = mock_response
                MockClient.return_value.__enter__.return_value = mock_instance
                result = svc.enrich_chunk("A" * 100)
                assert result == {"summary": "", "keywords": [], "questions": []}

    def test_call_google_exception(self):
        with patch.dict("os.environ", {"ENABLE_METADATA_ENRICHMENT": "1", "GEMINI_API_KEY": "AIza-test123"}):
            svc = MetadataEnrichmentService()
            with patch("httpx.Client") as MockClient:
                mock_instance = MagicMock()
                mock_instance.post.side_effect = Exception("network")
                MockClient.return_value.__enter__.return_value = mock_instance
                result = svc.enrich_chunk("A" * 100)
                assert result == {"summary": "", "keywords": [], "questions": []}
