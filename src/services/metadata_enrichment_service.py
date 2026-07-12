"""Service to enrich text chunk metadata using Gemini API (via Google or OpenRouter).

Extract keywords, summaries, and expected questions to boost RAG search match rate.

"""

from __future__ import annotations

import json
import logging
import os
from http import HTTPStatus
from typing import Any

import httpx

logger = logging.getLogger(__name__)

METADATA_ENRICHMENT_EXCEPTIONS = (httpx.HTTPError, json.JSONDecodeError, RuntimeError, ValueError, TypeError, OSError)
MIN_ENRICHMENT_CONTENT_LENGTH = 50


class MetadataEnrichmentService:
    """Calls Gemini API to analyze text content and extract keywords, summaries, and questions."""

    def __init__(self) -> None:
        """Initialize a new instance."""
        self.api_key = os.getenv("GEMINI_API_KEY")
        self.enabled = os.getenv("ENABLE_METADATA_ENRICHMENT", "0") == "1"
        if not self.api_key:
            self.enabled = False

    def enrich_chunk(self, content: str) -> dict[str, Any]:
        """Enrichens a text chunk with summary, keywords, and expected questions.

        Return a dict with 'summary', 'keywords', and 'questions'.

        Args:
            content: Content.
            content: Content.

        """
        if not self.enabled or not content or len(content.strip()) < MIN_ENRICHMENT_CONTENT_LENGTH:
            return {"summary": "", "keywords": [], "questions": []}

        # Prompt instruction
        prompt = (
            "아래 텍스트 본문을 분석하여 RAG 검색 매칭 성능을 극대화하기 위한 분석 정보를 추출해 주세요.\n"
            "반드시 아래 지정된 JSON 포맷으로만 응답해야 하며, 어떠한 마크다운 코드 "
            "블록(```json 등)이나 설명글도 포함하지 마세요.\n\n"
            "JSON 포맷:\n"
            "{\n"
            '  "summary": "텍스트의 핵심 한 줄 요약",\n'
            '  "keywords": ["핵심키워드1", "핵심키워드2", "핵심키워드3"],\n'
            '  "questions": ["사용자가 이 정보를 찾기 위해 던질 법한 예상 질문 1", "예상 질문 2"]\n'
            "}\n\n"
            f"텍스트 본문:\n{content}"
        )

        if self.api_key and self.api_key.startswith("sk-or-v1-"):
            return self._call_openrouter(prompt)
        return self._call_google(prompt)

    def _call_openrouter(self, prompt: str) -> dict[str, Any]:
        url = "https://openrouter.ai/api/v1/chat/completions"
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}

        model = os.getenv("ENRICHMENT_MODEL", "google/gemini-flash-1.5")

        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "response_format": {"type": "json_object"},
        }

        try:
            with httpx.Client(headers=headers, timeout=15.0) as client:
                res = client.post(url, json=payload)
                if res.status_code == HTTPStatus.OK:
                    data = res.json()
                    content_str = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                    return self._parse_json_response(content_str)
                logger.warning("⚠️ OpenRouter Enrichment API status %s: %s", res.status_code, res.text)
        except METADATA_ENRICHMENT_EXCEPTIONS:
            logger.exception("⚠️ Exception in OpenRouter enrichment")
        return {"summary": "", "keywords": [], "questions": []}

    def _call_google(self, prompt: str) -> dict[str, Any]:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={self.api_key}"
        headers = {"Content-Type": "application/json"}

        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"responseMimeType": "application/json"},
        }

        try:
            with httpx.Client(headers=headers, timeout=15.0) as client:
                res = client.post(url, json=payload)
                if res.status_code == HTTPStatus.OK:
                    data = res.json()
                    content_str = (
                        data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                    )
                    return self._parse_json_response(content_str)
                logger.warning("⚠️ Google Gemini Enrichment API status %s: %s", res.status_code, res.text)
        except METADATA_ENRICHMENT_EXCEPTIONS:
            logger.exception("⚠️ Exception in Gemini enrichment")
        return {"summary": "", "keywords": [], "questions": []}

    def _parse_json_response(self, text: str) -> dict[str, Any]:
        if not text:
            return {"summary": "", "keywords": [], "questions": []}

        # Strip markdown fences if present
        text = text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].startswith("```"):
                lines = lines[:-1]
            text = "\n".join(lines).strip()

        try:
            data = json.loads(text)
            return {
                "summary": data.get("summary", ""),
                "keywords": data.get("keywords", []) if isinstance(data.get("keywords"), list) else [],
                "questions": data.get("questions", []) if isinstance(data.get("questions"), list) else [],
            }
        except METADATA_ENRICHMENT_EXCEPTIONS:
            logger.exception("⚠️ Error parsing enrichment JSON. Raw content: %s", text[:100])

        return {"summary": "", "keywords": [], "questions": []}
