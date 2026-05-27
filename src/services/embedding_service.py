"""
Service to fetch vector embeddings from Gemini API or OpenRouter API.
"""
from __future__ import annotations

import logging
import os
import httpx
from typing import List, Optional

from src.utils.safe_print import safe_print as print

logger = logging.getLogger(__name__)

class EmbeddingService:
    """
    Connects to external embedding providers to generate vector arrays for chunk texts.
    """

    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            print("⚠️ Warning: GEMINI_API_KEY is not configured in environment.")

    def adjust_embedding_dimension(self, embedding: List[float], target_dim: int = 256) -> List[float]:
        """
        Truncates or pads embedding list to target_dim.
        If truncating, L2 normalization is applied.
        """
        if not embedding:
            return [0.0] * target_dim
        
        current_dim = len(embedding)
        if current_dim == target_dim:
            return embedding
            
        if current_dim > target_dim:
            truncated = embedding[:target_dim]
            import math
            norm = math.sqrt(sum(x * x for x in truncated))
            if norm > 1e-9:
                return [x / norm for x in truncated]
            return truncated
        else:
            return embedding + [0.0] * (target_dim - current_dim)

    def _compute_hash(self, text: str) -> str:
        import hashlib
        # Normalize whitespace to make hash robust to minor formatting changes
        cleaned = " ".join(text.split()).strip()
        return hashlib.sha256(cleaned.encode("utf-8")).hexdigest()

    def get_embedding(self, text: str) -> List[float]:
        """
        Generates embedding for a single text string.
        """
        results = self.get_embeddings_batch([text])
        return results[0] if results else [0.0] * 256

    def get_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Generates embeddings for a batch of text strings, utilizing a local SQLite cache.
        """
        if not texts:
            return []

        # 1. Determine model name
        if self.api_key and self.api_key.startswith("sk-or-v1-"):
            model_name = os.getenv("EMBEDDING_MODEL", "openai/text-embedding-3-small")
        else:
            model_name = "models/text-embedding-004"

        # 2. Compute hashes
        hashes = [self._compute_hash(t) for t in texts]
        
        # 3. Try to fetch from SQLite cache
        cached_map = {}
        try:
            from src.db.engine import SessionLocal
            from src.models.embedding_cache import EmbeddingCache
            from sqlalchemy import select
            
            with SessionLocal() as session:
                stmt = select(EmbeddingCache).where(
                    EmbeddingCache.text_hash.in_(hashes),
                    EmbeddingCache.model_name == model_name
                )
                cache_rows = session.scalars(stmt).all()
                for row in cache_rows:
                    import json
                    emb = row.embedding
                    if isinstance(emb, str):
                        try:
                            emb = json.loads(emb)
                        except Exception:
                            pass
                    cached_map[row.text_hash] = emb
        except Exception:
            logger.exception("⚠️ Warning: Embedding cache lookup error (continuing without cache)")

        # 4. Identify which texts need API calls
        missing_indices = []
        missing_texts = []
        for idx, h in enumerate(hashes):
            if h not in cached_map:
                missing_indices.append(idx)
                missing_texts.append(texts[idx])

        # 5. Call API for missing embeddings
        new_embeddings = []
        if missing_texts:
            if not self.api_key:
                print("❌ GEMINI_API_KEY missing. Returning zero-vectors as fallback.")
                new_embeddings = [[0.0] * 256 for _ in missing_texts]
            else:
                if self.api_key.startswith("sk-or-v1-"):
                    raw_embeddings = self._fetch_openrouter_embeddings(missing_texts)
                else:
                    raw_embeddings = self._fetch_google_embeddings(missing_texts)
                new_embeddings = [self.adjust_embedding_dimension(emb) for emb in raw_embeddings]

            # 6. Save newly generated embeddings to cache
            try:
                from src.db.engine import SessionLocal
                from src.models.embedding_cache import EmbeddingCache
                
                with SessionLocal() as session:
                    for idx, emb in enumerate(new_embeddings):
                        text_hash = hashes[missing_indices[idx]]
                        # Prevent duplicate insert if somehow triggered
                        existing = session.get(EmbeddingCache, (text_hash, model_name))
                        if not existing:
                            cache_entry = EmbeddingCache(
                                text_hash=text_hash,
                                model_name=model_name,
                                embedding=emb
                            )
                            session.add(cache_entry)
                    session.commit()
            except Exception:
                logger.exception("⚠️ Warning: Failed to save to embedding cache")

            # Merge new embeddings into cached map
            for idx, emb in enumerate(new_embeddings):
                cached_map[hashes[missing_indices[idx]]] = emb

        # 7. Construct final list in original order
        return [cached_map[h] for h in hashes]


    def _fetch_openrouter_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Calls OpenRouter's OpenAI-compatible embeddings endpoint.
        """
        url = "https://openrouter.ai/api/v1/embeddings"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # Default to openai/text-embedding-3-small which returns 1536-dimensional vectors
        model = os.getenv("EMBEDDING_MODEL", "openai/text-embedding-3-small")
        
        payload = {
            "model": model,
            "input": texts,
            "dimensions": 256
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
        except Exception:
            logger.exception("❌ Exception fetching OpenRouter embeddings")

        # Fallback empty vectors
        return [[0.0] * 256 for _ in texts]

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
                },
                "outputDimensionality": 256
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
        except Exception:
            logger.exception("❌ Exception fetching Google embeddings")

        # Fallback empty vectors (Google text-embedding-004 dimensions = 768, target = 256)
        return [[0.0] * 256 for _ in texts]

