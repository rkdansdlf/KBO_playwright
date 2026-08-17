#!/usr/bin/env python3
"""Embedding model x dimension benchmark for the KBO RAG pipeline.

Samples chunks from the pgvector RAG store, embeds a fixed set of KBO
queries with each candidate model (once per model at its native output
dimension), then sweeps locally truncated dimensions to find the best
(model, dimension) trade-off.

Metrics: recall@5, MRR, top-1 per (model, dimension) cell. Dimension
reduction is done locally with L2 re-normalization, so the API is called
once per model.

Usage:
    python scripts/benchmark_embedding_models.py                      # grid sweep
    python scripts/benchmark_embedding_models.py --models voyage,qwen3
    python scripts/benchmark_embedding_models.py --sample 200 --output bench.json
    python scripts/benchmark_embedding_models.py --dry-run            # API 호출 없이 구성 검증만

Environment:
    OPENROUTER_API_KEY (OpenRouter, "sk-or-v1-" prefix) for voyage/qwen3
    PPLX_API_KEY for pplx-embed models
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import sys
from dataclasses import dataclass, field
from http import HTTPStatus
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.vector_engine import get_vector_session

logger = logging.getLogger(__name__)

_HTTP_EXCEPTIONS = (httpx.HTTPError, ValueError, TypeError, RuntimeError, OSError)
_DEFAULT_SAMPLE = 500
_TOP_K = 5
# 소규모 신규 소스가 랜덤 샘플에서 배제되지 않도록 소스별 최소 쿼터 (stratified sampling)
_STRAT_MIN_PER_SOURCE = 25
# 프로바이더별 단일 요청 입력 개수 제한 (Mistral code 3210 대응을 위한 분할 크기)
_EMBED_BATCH_SIZE = 100

# 모델 프로바이더 매핑: model id → provider
_PPLX_PREFIX = "pplx-"
_PROVIDER_OPENROUTER = "openrouter"
_PROVIDER_PERPLEXITY = "perplexity"

# 모델별 API 키 환경변수
_API_KEY_ENV: dict[str, str] = {
    _PROVIDER_OPENROUTER: "OPENROUTER_API_KEY",
    _PROVIDER_PERPLEXITY: "PPLX_API_KEY",
}

# 차원 스윕 후보 (각 모델 네이티브 차원까지)
_DIM_CANDIDATES = (128, 256, 512, 768, 1024, 1536, 2048, 4096, 8192)

# CLI 단축 모델 키 → 모델 ID
_SHORT_KEYS: dict[str, str] = {
    "voyage": "voyageai/voyage-4-lite",
    "qwen3": "qwen/qwen3-embedding-8b",
    "pplx-4b": "pplx-embed-v1-4b",
    "pplx-0.6b": "pplx-embed-v1-0.6b",
}
_DEFAULT_MODEL_KEYS = ("voyage",)


@dataclass
class QuerySpec:
    """A single benchmark query with its expected source table."""

    text: str
    expected_source_table: str


_QUERIES: list[QuerySpec] = [
    QuerySpec("2025년 KBO 타율 1위 선수는 누구야?", "stat_rankings"),
    QuerySpec("김도영은 2025 시즌에 홈런 몇 개를 쳤어?", "player_season_batting"),
    QuerySpec("류현진이 현재 뛰고 있는 팀은 어디야?", "player_basic"),
    QuerySpec("2025년 KIA 타이거즈의 정규시즌 순위는 몇 위였어?", "team_standings_daily"),
    QuerySpec("최근에 선수 이동이나 트레이드 소식 있었어?", "player_movements"),
    QuerySpec("두산 베어스 공식 이벤트에 뭐가 등록되어 있어?", "team_events"),
    QuerySpec("2025년 시즌 승률이 가장 높았던 구단은 어디야?", "team_standings_daily"),
    QuerySpec("2025년 KBO 투수 평균자책점 1위 선수는 누구인가?", "stat_rankings"),
    QuerySpec("지난 시즌 홈런왕이 누구였는지 알려줘.", "stat_rankings"),
    QuerySpec("이번 달에 진행된 경기 결과를 알려줘.", "game"),
    QuerySpec("최근 한화 이글스의 선수 등록 이동은 뭐가 있었지?", "player_movements"),
    QuerySpec("올 시즌 KBO 최고 타자를 알려줘.", "stat_rankings"),
    QuerySpec("KBO 공식 수상 내역 (MVP 등)이 궁금해!", "awards"),
    QuerySpec("KIA 타이거즈의 2025 시즌 팀 순위는 어때?", "team_standings_daily"),
    QuerySpec("한화 이글스의 투수 성적이 궁금해.", "player_season_pitching"),
    QuerySpec("LG 트윈스의 연고지와 홈구장은 어디야?", "teams"),
    QuerySpec("삼성 라이온즈가 우승한 최근 시즌이 있나?", "team_history"),
    QuerySpec("2026년 두산 베어스의 경기 선발 라인업은?", "game_lineups"),
    QuerySpec("KBO 금년 시즌 타점 1위 선수는?", "stat_rankings"),
    QuerySpec("최근 경기에서 가장 극적인 순간이 있었어?", "game_highlights"),
]


@dataclass
class BenchCell:
    """One (model, dim) evaluation result."""

    model_key: str
    model_id: str
    dim: int
    recall_at_k: float = 0.0
    mrr: float = 0.0
    top1_hits: float = 0.0
    n_queries: int = 0
    per_query: list[dict[str, Any]] = field(default_factory=list)


# ─── 임베딩 호출 ───────────────────────────────────────────────────────────────


def _post_embed_batch(
    url: str,
    headers: dict[str, str],
    model_id: str,
    texts: list[str],
) -> list[list[float]]:
    """Embed texts via a single POST, expecting `data[i].embedding`.

    Args:
        url: 임베딩 엔드포인트 URL.
        headers: 요청 헤더.
        model_id: 모델 ID.
        texts: 임베딩할 텍스트 목록.

    Returns:
        Embedding vectors (실패 시 빈 목록).

    """
    with httpx.Client(headers=headers, timeout=120.0) as client:
        res = client.post(url, json={"model": model_id, "input": texts})
        if res.status_code != HTTPStatus.OK:
            logger.warning("Embedding status %s for %s: %s", res.status_code, model_id, res.text[:200])
            return []
    data = res.json()
    records = data.get("data") or []
    if not isinstance(records, list):
        return []
    records.sort(key=lambda x: x.get("index", 0))
    return [item.get("embedding") for item in records]


def _embed_with_batching(
    url: str,
    headers: dict[str, str],
    model_id: str,
    texts: list[str],
) -> list[list[float]]:
    """Embed texts, splitting into requests of at most `_EMBED_BATCH_SIZE` inputs.

    Args:
        url: HTTP 엔드포인트 URL.
        headers: HTTP 요청 헤더.
        model_id: 모델 ID.
        texts: 임베딩할 텍스트 목록.

    Returns:
        임베딩 벡터 목록 (실패 시 빈 목록).

    """
    result: list[list[float]] = []
    for start in range(0, len(texts), _EMBED_BATCH_SIZE):
        chunk = texts[start : start + _EMBED_BATCH_SIZE]
        vectors = _post_embed_batch(url, headers, model_id, chunk)
        if not vectors:
            return []
        result.extend(vectors)
    return result


def _embed_openrouter(api_key: str, model_id: str, texts: list[str]) -> list[list[float]]:
    """Embed a batch of texts via the OpenRouter embeddings endpoint.

    Args:
        api_key: OpenRouter API 키.
        model_id: 모델 ID.
        texts: 임베딩할 텍스트 목록.

    Returns:
        임베딩 벡터 목록 (실패 시 빈 목록).

    """
    url = "https://openrouter.ai/api/v1/embeddings"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    return _embed_with_batching(url, headers, model_id, texts)


def _embed_perplexity(api_key: str, model_id: str, texts: list[str]) -> list[list[float]]:
    """Embed a batch of texts via the Perplexity embeddings endpoint.

    Args:
        api_key: Perplexity API key.
        model_id: 모델 ID.
        texts: 임베딩할 텍스트 목록.

    Returns:
        임베딩 벡터 목록 (실패 시 빈 목록).

    """
    url = "https://api.perplexity.ai/embeddings"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    return _embed_with_batching(url, headers, model_id, texts)


def _embed(provider: str, api_key: str, model_id: str, texts: list[str]) -> list[list[float]]:
    """Dispatch embedding call by provider.

    Args:
        provider: 프로바이더 (openrouter / perplexity).
        api_key: Api Key.
        model_id: 모델 ID.
        texts: 텍스트 목록.

    Returns:
        Embedding vectors.

    """
    if provider == _PROVIDER_PERPLEXITY:
        return _embed_perplexity(api_key, model_id, texts)
    return _embed_openrouter(api_key, model_id, texts)


# ─── 코사인 검색 + 평가 ──────────────────────────────────────────────────────


def _truncate(vec: list[float], dim: int) -> list[float]:
    """Truncate a vector to dim and L2-normalize the slice.

    Args:
        vec: 원본 벡터.
        dim: 목표 차원.

    Returns:
        정규화된 절단 벡터.

    """
    sliced = vec[:dim]
    if len(sliced) < dim:
        sliced = sliced + [0.0] * (dim - len(sliced))
    norm = math.sqrt(sum(x * x for x in sliced)) or 1.0
    return [v / norm for v in sliced]


def _cosine_scores(query_vec: list[float], doc_vecs: list[list[float]]) -> list[float]:
    """Return cosine similarity scores between a query and every doc.

    Args:
        query_vec: 쿼리 벡터.
        doc_vecs: 문서 벡터 목록.

    Returns:
        Similarity scores.

    """
    q_norm = math.sqrt(sum(x * x for x in query_vec)) or 1.0
    scores: list[float] = []
    for doc in doc_vecs:
        d_norm = math.sqrt(sum(x * x for x in doc)) or 1.0
        dot = sum(a * b for a, b in zip(query_vec, doc, strict=False))
        scores.append(dot / (q_norm * d_norm))
    return scores


def _top_k_indices(scores: list[float], k: int) -> list[int]:
    """Return indices of the top-k docs by cosine score.

    Args:
        scores: 유사도 점수 목록.
        k: 상위 k.

    Returns:
        상위 k 인덱스.

    """
    return sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:k]


def _evaluate(
    queries: list[QuerySpec],
    query_vectors: list[list[float]],
    doc_vectors: list[list[float]],
    doc_choices: list[str],
    k: int,
) -> BenchCell:
    """Evaluate recall@k / MRR / top-1 for one (model, dim) cell.

    Args:
        queries: 쿼리 스펙 목록.
        query_vectors: 쿼리 벡터 목록.
        doc_vectors: 문서 벡터 목록.
        doc_choices: 문서 소스 테이블 목록.
        k: 상위 k.

    Returns:
        평가 결과 셀.

    """
    result = BenchCell(model_key="", model_id="", dim=0, n_queries=len(queries))
    hits = 0
    mrr_sum = 0.0
    top1_hits = 0
    for idx, query in enumerate(queries):
        scores = _cosine_scores(query_vectors[idx], doc_vectors)
        ranked = _top_k_indices(scores, k)
        ranked_sources = [doc_choices[i] for i in ranked]
        expected = query.expected_source_table
        try:
            rank = ranked_sources.index(expected) + 1
        except ValueError:
            rank = 0
        if rank:
            hits += 1
            mrr_sum += 1.0 / rank
        if rank == 1:
            top1_hits += 1
        result.per_query.append(
            {
                "query_text": query.text,
                "expected": expected,
                "top_sources": ranked_sources,
                "rank_of_expected": rank,
            }
        )
    result.recall_at_k = hits / len(queries)
    result.mrr = mrr_sum / len(queries)
    result.top1_hits = top1_hits
    return result


# ─── 샘플 로드 ────────────────────────────────────────────────────────────────


def _load_sample_chunks(sample: int, seed: int) -> tuple[list[str], list[str]]:
    """Sample chunk contents and source tables from pgvector rag_chunks.

    Args:
        sample: 샘플링할 청크 수.
        seed: 셔플 시드.

    Returns:
        (내용 목록, 소스 테이블 목록).

    """
    try:
        rng = random.Random(seed)
        with get_vector_session() as vsession:
            rows = vsession.execute(
                text(
                    "SELECT content, source_table FROM rag_chunks "
                    "WHERE embedding IS NOT NULL AND vector_norm(embedding) > 0"
                )
            ).all()
        if not rows:
            return [], []
        by_source: dict[str, list[tuple[str, str]]] = {}
        for content, source in rows:
            by_source.setdefault(source, []).append((content, source))
        selected: list[tuple[str, str]] = []
        remainder: list[tuple[str, str]] = []
        for source in sorted(by_source):
            items = by_source[source]
            selected.extend(items[:_STRAT_MIN_PER_SOURCE])
            remainder.extend(items[_STRAT_MIN_PER_SOURCE:])
        rng.shuffle(remainder)
        selected.extend(remainder[: max(0, sample - len(selected))])
        rng.shuffle(selected)
        selected = selected[:sample]
        return [r[0] for r in selected], [r[1] for r in selected]
    except Exception:  # intentional: sampling is best-effort
        logger.exception("샘플 청크 로드 실패")
        return [], []


# ─── CLI ──────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="KBO 임베딩 모델×차원 벤치마크")
    parser.add_argument("--sample", type=int, default=_DEFAULT_SAMPLE, help="평가에 사용할 청크 수 (기본 500)")
    parser.add_argument("--models", type=str, default=None, help="모델 키 쉼표 목록 (voyage,qwen3,pplx-4b,pplx-0.6b)")
    parser.add_argument("--output", type=str, default=None, help="결과 JSON 파일 경로")
    parser.add_argument("--seed", type=int, default=42, help="샘플 셔플 시드")
    parser.add_argument("--dry-run", action="store_true", help="API 호출 없이 구성 검증만 수행")
    return parser.parse_args()


def _resolve_model_ids(models_arg: str | None) -> list[str]:
    """Resolve the --models argument into a model-id list.

    Args:
        models_arg: 쉼표 구분 모델 키 문자열.

    Returns:
        모델 ID 목록 (미지원 키는 로그 후 제외).

    """
    if not models_arg:
        return [_SHORT_KEYS[key] for key in _DEFAULT_MODEL_KEYS]
    keys = [k.strip() for k in models_arg.split(",") if k.strip()]
    resolved: list[str] = []
    for key in keys:
        if key in _SHORT_KEYS:
            resolved.append(_SHORT_KEYS[key])
        elif "/" in key:
            resolved.append(key)
        else:
            logger.error("알 수 없는 모델 키: %s (voyage,qwen3,pplx-4b,pplx-0.6b 중 선택)", key)
    return resolved


def _bench_model(
    model_id: str,
    query_texts: list[str],
    doc_contents: list[str],
    doc_choices: list[str],
    report: dict[str, Any],
    *,
    dry_run: bool,
) -> None:
    """Run the benchmark for single model and append its cells to the report.

    Args:
        model_id: 벤치 대상 모델 ID.
        query_texts: 쿼리 텍스트 목록.
        doc_contents: 문서 텍스트 목록.
        doc_choices: 문서 소스 테이블 목록.
        report: 결과를 누적할 리포트 dict.
        dry_run: API 호출 없이 구성 검증만 수행할지 여부.

    """
    provider = _PROVIDER_PERPLEXITY if model_id.startswith(_PPLX_PREFIX) else _PROVIDER_OPENROUTER
    api_key_env = _API_KEY_ENV[provider]
    api_key = os.getenv(api_key_env)
    if not api_key:
        logger.warning("SKIP %s: %s 키 미설정", model_id, api_key_env)
        return
    if dry_run:
        logger.info("[dry-run] %s - API 호출 생략", model_id)
        return

    try:
        query_vectors = _embed(provider, api_key, model_id, query_texts)
        doc_vectors = _embed(provider, api_key, model_id, doc_contents)
    except _HTTP_EXCEPTIONS:
        logger.exception("SKIP %s: 임베딩 요청 실패", model_id)
        return
    if len(query_vectors) != len(query_texts) or not doc_vectors or len(doc_vectors) != len(doc_contents):
        logger.warning(
            "SKIP %s: 응답 개수 불일치 (query=%d/%d, doc=%d/%d)",
            model_id,
            len(query_vectors),
            len(query_texts),
            len(doc_vectors),
            len(doc_contents),
        )
        return

    native_dim = len(query_vectors[0]) if query_vectors else 0
    dims = [d for d in _DIM_CANDIDATES if d <= native_dim]
    if native_dim not in dims:
        dims.append(native_dim)
    logger.info("> %s native_dim=%d, dims=%s", model_id, native_dim, dims)

    for dim in dims:
        doc_truncated = [_truncate(v, dim) for v in doc_vectors]
        query_truncated = [_truncate(v, dim) for v in query_vectors]
        cell = _evaluate(_QUERIES, query_truncated, doc_truncated, doc_choices, _TOP_K)
        cell.model_key = model_id
        cell.model_id = model_id
        cell.dim = dim
        report_cell = {
            "model_id": model_id,
            "dim": dim,
            "recall@5": round(cell.recall_at_k, 4),
            "mrr": round(cell.mrr, 4),
            "top1_hits": cell.top1_hits,
            "n_queries": cell.n_queries,
        }
        report["cells"].append(report_cell)
        logger.info(
            "  %-28s dim=%-5d recall@5=%.3f mrr=%.3f top1=%d/%d",
            model_id,
            dim,
            cell.recall_at_k,
            cell.mrr,
            cell.top1_hits,
            cell.n_queries,
        )


def main(argv: list[str] | None = None) -> int:
    """Run the embedding model x dimension benchmark.

    Args:
        argv: CLI 인수.

    Returns:
        Exit code.

    """
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args()
    model_ids = _resolve_model_ids(args.models)
    if not model_ids:
        return 2

    doc_contents, doc_choices = _load_sample_chunks(args.sample, args.seed)
    if not doc_contents:
        logger.error("pgvector에서 샘플을 로드할 수 없습니다")
        return 1
    logger.info("샘플 청크: %d", len(doc_contents))

    query_texts = [q.text for q in _QUERIES]
    report: dict[str, Any] = {"chunks": len(doc_contents), "queries": query_texts, "cells": []}

    for model_id in model_ids:
        _bench_model(model_id, query_texts, doc_contents, doc_choices, report, dry_run=args.dry_run)

    cells = report.get("cells")
    if not cells:
        logger.error("평가된 셀이 없습니다 (API 키/모델 확인)")
        return 2

    best = max(cells, key=lambda c: c["recall@5"])
    report["best"] = best
    logger.info("BEST: %s dim=%d recall@5=%.3f mrr=%.3f", best["model_id"], best["dim"], best["recall@5"], best["mrr"])

    if args.output:
        Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("결과 저장: %s", args.output)
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
