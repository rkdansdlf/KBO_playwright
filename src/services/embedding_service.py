"""Service to fetch vector embeddings from OpenRouter."""

from __future__ import annotations

import contextlib
import json
import logging
import os
import time
from http import HTTPStatus
from typing import Any

import httpx
from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)
load_dotenv()

EMBEDDING_DB_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, OSError)
EMBEDDING_HTTP_EXCEPTIONS = (httpx.HTTPError, ValueError, TypeError, RuntimeError, OSError)
EMBEDDING_NORMALIZATION_EPSILON = 1e-9
EMBEDDING_TARGET_DIMENSION = 1536
DEFAULT_OPENROUTER_EMBEDDING_MODEL = "perplexity/pplx-embed-v1-4b"
OPENROUTER_RATE_LIMIT_RETRIES = 6
OPENROUTER_RATE_LIMIT_BASE_DELAY_SECONDS = 2.0
OPENROUTER_RATE_LIMIT_MAX_DELAY_SECONDS = 60.0


def _is_zero_vector(embedding: list[float]) -> bool:
    """Return True when every component of the embedding is exactly 0.0."""
    return bool(embedding) and all(value == 0.0 for value in embedding)


class EmbeddingService:
    """Connects to external embedding providers to generate vector arrays for chunk texts."""

    def __init__(self, *, cache_enabled: bool = True) -> None:
        """Initialize a new instance."""
        self.api_key = os.getenv("OPENROUTER_API_KEY")
        self.cache_enabled = cache_enabled
        if not self.api_key:
            logger.warning("⚠️ Warning: OPENROUTER_API_KEY is not configured in environment.")

    def adjust_embedding_dimension(
        self, embedding: list[float], target_dim: int = EMBEDDING_TARGET_DIMENSION
    ) -> list[float]:
        """Truncate or pad embedding list to target_dim.

        If truncating, L2 normalization is applied.

        Args:
            embedding: Embedding.
            target_dim: Target Dim.
            embedding: Embedding.
            target_dim: Target Dim.

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
            if norm > EMBEDDING_NORMALIZATION_EPSILON:
                return [x / norm for x in truncated]
            return truncated
        return embedding + [0.0] * (target_dim - current_dim)

    def _compute_hash(self, text: str) -> str:
        import hashlib

        # Normalize whitespace to make hash robust to minor formatting changes
        cleaned = " ".join(text.split()).strip()
        return hashlib.sha256(cleaned.encode("utf-8")).hexdigest()

    def get_embedding(self, text: str) -> list[float]:
        """Generate embedding for a single text string.

        Args:
            text: Text.
            text: Text.

        """
        results = self.get_embeddings_batch([text])

        return results[0] if results else [0.0] * EMBEDDING_TARGET_DIMENSION

    def get_embeddings_batch(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings for a batch of text strings using the primary DB cache.

        Args:
            texts: Texts.
            texts: Texts.

        """
        if not texts:
            return []

        model_name = self._model_name()
        hashes = [self._compute_hash(t) for t in texts]
        cached_map = self._load_cached_embeddings(hashes, model_name) if self.cache_enabled else {}
        missing_indices, missing_texts = self._missing_embedding_inputs(texts, hashes, cached_map)

        if missing_texts:
            new_embeddings = self._fetch_missing_embeddings(missing_texts)
            if self.cache_enabled:
                self._save_cached_embeddings(hashes, missing_indices, model_name, new_embeddings)
            self._merge_new_embeddings(cached_map, hashes, missing_indices, new_embeddings)

        return [cached_map[h] for h in hashes]

    def _model_name(self) -> str:
        return os.getenv("EMBEDDING_MODEL", DEFAULT_OPENROUTER_EMBEDDING_MODEL)

    def _load_cached_embeddings(self, hashes: list[str], model_name: str) -> dict[str, list[float]]:
        cached_map = {}
        try:
            from sqlalchemy import select

            from src.db.engine import get_rag_index_session
            from src.models.embedding_cache import EmbeddingCache

            with get_rag_index_session() as session:
                stmt = select(EmbeddingCache).where(
                    EmbeddingCache.text_hash.in_(hashes),
                    EmbeddingCache.model_name == model_name,
                )
                for row in session.scalars(stmt).all():
                    emb = row.embedding
                    if isinstance(emb, str):
                        with contextlib.suppress(json.JSONDecodeError, TypeError):
                            emb = json.loads(emb)
                    if _is_zero_vector(emb):
                        continue
                    cached_map[row.text_hash] = emb
        except EMBEDDING_DB_EXCEPTIONS:
            logger.exception("⚠️ Warning: Embedding cache lookup error (continuing without cache)")
        return cached_map

    def _missing_embedding_inputs(
        self,
        texts: list[str],
        hashes: list[str],
        cached_map: dict[str, list[float]],
    ) -> tuple[list[int], list[str]]:
        missing_indices = []
        missing_texts = []
        for idx, text_hash in enumerate(hashes):
            if text_hash not in cached_map:
                missing_indices.append(idx)
                missing_texts.append(texts[idx])
        return missing_indices, missing_texts

    def _fetch_missing_embeddings(self, missing_texts: list[str]) -> list[list[float]]:
        if not self.api_key:
            logger.error("❌ OPENROUTER_API_KEY missing. Returning zero-vectors as fallback.")
            return [[0.0] * EMBEDDING_TARGET_DIMENSION for _ in missing_texts]
        raw_embeddings = self._fetch_openrouter_embeddings(missing_texts)
        return [self.adjust_embedding_dimension(emb) for emb in raw_embeddings]

    def _save_cached_embeddings(
        self,
        hashes: list[str],
        missing_indices: list[int],
        model_name: str,
        new_embeddings: list[list[float]],
    ) -> None:
        try:
            from src.db.engine import get_rag_index_session
            from src.models.embedding_cache import EmbeddingCache

            with get_rag_index_session() as session:
                seen: set[str] = set()
                for idx, emb in enumerate(new_embeddings):
                    text_hash = hashes[missing_indices[idx]]
                    if text_hash in seen:
                        continue
                    seen.add(text_hash)
                    if _is_zero_vector(emb):
                        continue
                    existing = session.get(EmbeddingCache, (text_hash, model_name))
                    if not existing:
                        session.add(EmbeddingCache(text_hash=text_hash, model_name=model_name, embedding=emb))
                session.commit()
        except EMBEDDING_DB_EXCEPTIONS:
            logger.exception("⚠️ Warning: Failed to save to embedding cache")

    def _merge_new_embeddings(
        self,
        cached_map: dict[str, list[float]],
        hashes: list[str],
        missing_indices: list[int],
        new_embeddings: list[list[float]],
    ) -> None:
        for idx, emb in enumerate(new_embeddings):
            cached_map[hashes[missing_indices[idx]]] = emb

    def _fetch_openrouter_embeddings(self, texts: list[str]) -> list[list[float]]:
        """Call OpenRouter's's OpenAI-compatible embeddings endpoint.

        Args:
            texts: Texts.
            texts: Texts.

        """
        url = "https://openrouter.ai/api/v1/embeddings"

        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}

        # pplx-embed-v1-4b supports MRL-style dimension trimming; other providers may differ
        model = self._model_name()

        response = self._post_openrouter(
            url,
            headers,
            {"model": model, "input": texts, "dimensions": EMBEDDING_TARGET_DIMENSION},
        )

        # Some providers reject the OpenAI-style `dimensions` parameter with a 4xx.
        # Retry without it; adjust_embedding_dimension normalizes the native vector.
        if response is not None and response.status_code in (
            HTTPStatus.BAD_REQUEST,
            HTTPStatus.UNPROCESSABLE_ENTITY,
        ):
            logger.warning(
                "⚠️ OpenRouter embedding API rejected 'dimensions' (status %s) — retrying without it",
                response.status_code,
            )
            response = self._post_openrouter(url, headers, {"model": model, "input": texts})

        if response is None:
            return [[0.0] * EMBEDDING_TARGET_DIMENSION for _ in texts]

        if response.status_code == HTTPStatus.OK:
            data = response.json()
            # OpenRouter / OpenAI format: {"data": [{"embedding": [...]}, ...]}
            records = data.get("data", [])
            # Make sure they are returned in order
            sorted_records = sorted(records, key=lambda x: x.get("index", 0))
            return [item.get("embedding") for item in sorted_records]
        logger.error("❌ OpenRouter Embedding API returned status %s: %s", response.status_code, response.text)

        # Fallback empty vectors
        return [[0.0] * EMBEDDING_TARGET_DIMENSION for _ in texts]

    def _post_openrouter(self, url: str, headers: dict[str, str], payload: dict[str, Any]) -> httpx.Response | None:
        """POST an embedding request to the OpenRouter API.

        Args:
            url: Url.
            headers: Headers.
            payload: Payload.

        Returns:
            Response, or None when the transport raised an exception.

        """
        try:
            with httpx.Client(headers=headers, timeout=30.0) as client:
                for attempt in range(OPENROUTER_RATE_LIMIT_RETRIES + 1):
                    response = client.post(url, json=payload)
                    if response.status_code != HTTPStatus.TOO_MANY_REQUESTS:
                        return response
                    if attempt == OPENROUTER_RATE_LIMIT_RETRIES:
                        return response
                    delay = _rate_limit_delay(response, attempt)
                    logger.warning(
                        "OpenRouter rate limit (429); retrying in %.1fs (%d/%d)",
                        delay,
                        attempt + 1,
                        OPENROUTER_RATE_LIMIT_RETRIES,
                    )
                    time.sleep(delay)
        except EMBEDDING_HTTP_EXCEPTIONS:
            logger.exception("❌ Exception fetching OpenRouter embeddings")
            return None
        return None


def _rate_limit_delay(response: httpx.Response, attempt: int) -> float:
    """Return a bounded retry delay using the provider's Retry-After hint."""
    retry_after = response.headers.get("Retry-After")
    try:
        if retry_after:
            return min(float(retry_after), OPENROUTER_RATE_LIMIT_MAX_DELAY_SECONDS)
    except (TypeError, ValueError):
        pass
    return min(
        OPENROUTER_RATE_LIMIT_BASE_DELAY_SECONDS * (2**attempt),
        OPENROUTER_RATE_LIMIT_MAX_DELAY_SECONDS,
    )
