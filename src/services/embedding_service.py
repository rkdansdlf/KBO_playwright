"""
Service to fetch vector embeddings from Gemini API or OpenRouter API.
"""
from __future__ import annotations

import os
import httpx
from typing import List, Optional

from src.utils.safe_print import safe_print as print

class EmbeddingService:
    """
    Connects to external embedding providers to generate vector arrays for chunk texts.
    """

    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            print("⚠️ Warning: GEMINI_API_KEY is not configured in environment.")

    def get_embedding(self, text: str) -> List[float]:
        """
        Generates embedding for a single text string.
        """
        results = self.get_embeddings_batch([text])
        return results[0] if results else []

    def get_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Generates embeddings for a batch of text strings.
        """
        if not self.api_key:
            print("❌ GEMINI_API_KEY missing. Returning zero-vectors as fallback.")
            return [[0.0] * 1536 for _ in texts]

        # Detect OpenRouter key format
        if self.api_key.startswith("sk-or-v1-"):
            return self._fetch_openrouter_embeddings(texts)
        else:
            return self._fetch_google_embeddings(texts)

    def _fetch_openrouter_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Calls OpenRouter's OpenAI-compatible embeddings endpoint.
        """
        url = "https://openrouter.ai/api/v1/embeddings"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": application_json := "application/json"
        }
        
        # Default to openai/text-embedding-3-small which returns 1536-dimensional vectors
        model = os.getenv("EMBEDDING_MODEL", "openai/text-embedding-3-small")
        
        payload = {
            "model": model,
            "input": texts
        }

        try:
            with httpx.Client(headers=headers, timeout=30.0) as client:
                res = client.post(url, json=payload)
                if res.status_code == 200:
                    data = res.json()
                    # OpenRouter / OpenAI format: {"data": [{"embedding": [...]}, ...]}
                    records = data.get("data", [])
                    # Make sure they are returned in order
                    sorted_records = sorted(records, key=lambda x: x.get("index", 0))
                    embeddings = [item.get("embedding") for item in sorted_records]
                    return embeddings
                else:
                    print(f"❌ OpenRouter Embedding API returned status {res.status_code}: {res.text}")
        except Exception as e:
            print(f"❌ Exception fetching OpenRouter embeddings: {e}")

        # Fallback empty vectors
        return [[0.0] * 1536 for _ in texts]

    def _fetch_google_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Calls standard Google Gemini AI Studio Embeddings API.
        """
        # Google text-embedding-004 supports batching via batchEmbedContents
        url = f"https://generativelanguage.googleapis.com/v1beta/models/text-embedding-004:batchEmbedContents?key={self.api_key}"
        headers = {
            "Content-Type": "application/json"
        }

        requests_payload = []
        for text in texts:
            requests_payload.append({
                "model": "models/text-embedding-004",
                "content": {
                    "parts": [{
                        "text": text
                    }]
                }
            })

        payload = {"requests": requests_payload}

        try:
            with httpx.Client(headers=headers, timeout=30.0) as client:
                res = client.post(url, json=payload)
                if res.status_code == 200:
                    data = res.json()
                    # Google format: {"embeddings": [{"values": [...]}, ...]}
                    embeddings_data = data.get("embeddings", [])
                    return [item.get("values", []) for item in embeddings_data]
                else:
                    print(f"❌ Google Embedding API returned status {res.status_code}: {res.text}")
        except Exception as e:
            print(f"❌ Exception fetching Google embeddings: {e}")

        # Fallback empty vectors (Google text-embedding-004 dimensions = 768)
        return [[0.0] * 768 for _ in texts]
