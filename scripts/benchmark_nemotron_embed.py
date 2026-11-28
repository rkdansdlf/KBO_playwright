#!/usr/bin/env python3
"""Nemotron 3.5 Embed 8B vs 1B 간단 비교 테스트 (DB 불필요)"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from http import HTTPStatus
from pathlib import Path
from typing import Any

import httpx

from src.config.env_loader import load_project_env

load_project_env()

logger = logging.getLogger(__name__)

# 테스트용 쿼리 (KBO 도메인 특화)
TEST_QUERIES = [
    "2025년 KBO 타율 1위 선수는 누구야?",
    "김도영은 2025 시즌에 홈런 몇 개를 쳤어?",
    "류현진이 현재 뛰고 있는 팀은 어디야?",
    "2025년 KIA 타이거즈의 정규시즌 순위는 몇 위였어?",
    "최근에 선수 이동이나 트레이드 소식 있었어?",
    "두산 베어스 공식 이벤트에 뭐가 등록되어 있어?",
    "2025년 시즌 승률이 가장 높았던 구단은 어디야?",
    "2025년 KBO 투수 평균자책점 1위 선수는 누구인가?",
]

# 테스트용 문서 (샘플 RAG 청크 시뮬레이션)
TEST_DOCS = [
    ("stat_rankings", "2025년 KBO 타율 1위는 김도영(KIA)입니다. 타율 0.372로 1위를 기록했습니다."),
    ("player_season_batting", "김도영(KIA)은 2025시즌 144경기에서 홈런 38개, 타율 0.372, 타점 112개를 기록했습니다."),
    ("player_basic", "류현진은 2025시즌 한화 이글스 소속으로 KBO 리그에 복귀했습니다."),
    ("team_standings_daily", "2025년 KIA 타이거즈는 정규시즌 1위로 한국시리즈 직행했습니다. 88승 56패 승률 0.611."),
    ("player_movements", "2025년 7월 트레이드 마감일: SSG 랜더스가 투수 김택연을 키움 히어로즈로 보냈습니다."),
    ("team_events", "두산 베어스 2025시즌 공식 이벤트: 홈 개막전 팬 페스티벌, 어린이날 특별 이벤트 등 12개 행사 진행."),
    (
        "team_standings_daily",
        "2025시즌 승률 1위 구단은 KIA 타이거즈(0.611), 2위 삼성 라이온즈(0.583), 3위 LG 트윈스(0.569).",
    ),
    ("stat_rankings", "2025년 KBO 투수 평균자책점 1위는 원태인(삼성)입니다. ERA 2.34로 1위를 기록했습니다."),
]

DOC_SOURCES = [d[0] for d in TEST_DOCS]
DOC_CONTENTS = [d[1] for d in TEST_DOCS]

MODELS = {
    "nemotron-8b": "nvidia/nemotron-3.5-embed-8b:free",
    "nemotron-1b": "nvidia/nemotron-3.5-embed-1b:free",
}


@dataclass
class BenchResult:
    model_key: str
    model_id: str
    native_dim: int = 0
    query_latency_ms: float = 0.0
    doc_latency_ms: float = 0.0
    query_vectors: list[list[float]] = field(default_factory=list)
    doc_vectors: list[list[float]] = field(default_factory=list)
    success: bool = False
    error: str | None = None


def _post_embed_batch(
    url: str,
    headers: dict[str, str],
    model_id: str,
    texts: list[str],
) -> list[list[float]]:
    """OpenRouter 임베딩 엔드포인트 호출"""
    with httpx.Client(headers=headers, timeout=300.0) as client:
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
    batch_size: int = 100,
    max_retries: int = 3,
) -> list[list[float]]:
    """배치 단위로 임베딩 요청 (재시도 포함)"""
    result: list[list[float]] = []
    for start in range(0, len(texts), batch_size):
        chunk = texts[start : start + batch_size]
        vectors = []
        for attempt in range(max_retries):
            vectors = _post_embed_batch(url, headers, model_id, chunk)
            if vectors:
                break
            if attempt < max_retries - 1:
                wait = (attempt + 1) * 5  # 5초, 10초, 15초 대기
                logger.warning("재시도 %d/%d: %s (%.0f초 대기)", attempt + 1, max_retries, model_id, wait)
                time.sleep(wait)
        if not vectors:
            return []
        result.extend(vectors)
    return result


def _cosine_scores(query_vec: list[float], doc_vecs: list[list[float]]) -> list[float]:
    """코사인 유사도 계산"""
    q_norm = sum(x * x for x in query_vec) ** 0.5 or 1.0
    scores = []
    for doc in doc_vecs:
        d_norm = sum(x * x for x in doc) ** 0.5 or 1.0
        # strict=False: nothing here guarantees the query and document vectors
        # have the same length (both come from the same model, but this helper
        # does not check it), so strict=True could not be relied on. A mismatch
        # therefore truncates silently; callers must embed both sides with one model.
        dot = sum(a * b for a, b in zip(query_vec, doc, strict=False))
        scores.append(dot / (q_norm * d_norm))
    return scores


def _top_k_indices(scores: list[float], k: int) -> list[int]:
    return sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:k]


def evaluate_model(model_key: str, model_id: str, api_key: str) -> BenchResult:
    """단일 모델 평가"""
    url = "https://openrouter.ai/api/v1/embeddings"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    query_texts = list(TEST_QUERIES)

    result = BenchResult(model_key=model_key, model_id=model_id)

    # 8B 모델은 더 작은 배치로 시도 (타임아웃 방지)
    batch_size = 4 if "8b" in model_key.lower() else 100

    try:
        # 쿼리 임베딩
        start = time.time()
        query_vectors = _embed_with_batching(url, headers, model_id, query_texts, batch_size=batch_size)
        result.query_latency_ms = (time.time() - start) * 1000

        # 문서 임베딩
        start = time.time()
        doc_vectors = _embed_with_batching(url, headers, model_id, DOC_CONTENTS, batch_size=batch_size)
        result.doc_latency_ms = (time.time() - start) * 1000

        if len(query_vectors) != len(query_texts) or len(doc_vectors) != len(DOC_CONTENTS):
            result.error = f"응답 개수 불일치: query={len(query_vectors)}/{len(query_texts)}, doc={len(doc_vectors)}/{len(DOC_CONTENTS)}"
            return result

        result.native_dim = len(query_vectors[0]) if query_vectors else 0
        result.query_vectors = query_vectors
        result.doc_vectors = doc_vectors
        result.success = True

    except Exception as e:
        result.error = str(e)
        logger.exception("모델 %s 평가 실패", model_id)

    return result


def evaluate_retrieval(result: BenchResult, doc_sources: list[str], k: int = 5) -> dict[str, Any]:
    """검색 품질 평가 (Recall@K, MRR, Top-1)"""
    if not result.success or not result.query_vectors or not result.doc_vectors:
        return {"recall_at_k": 0.0, "mrr": 0.0, "top1_hits": 0, "per_query": []}

    hits = 0
    mrr_sum = 0.0
    top1_hits = 0
    per_query = []

    for idx, query in enumerate(TEST_QUERIES):
        scores = _cosine_scores(result.query_vectors[idx], result.doc_vectors)
        ranked = _top_k_indices(scores, k)
        ranked_sources = [doc_sources[i] for i in ranked]
        expected = TEST_DOCS[idx][0]  # 각 쿼리에 해당하는 예상 소스

        try:
            rank = ranked_sources.index(expected) + 1
        except ValueError:
            rank = 0

        if rank:
            hits += 1
            mrr_sum += 1.0 / rank
        if rank == 1:
            top1_hits += 1

        per_query.append(
            {
                "query": query,
                "expected_source": expected,
                "top_sources": ranked_sources,
                "rank": rank,
            }
        )

    return {
        "recall_at_k": round(hits / len(TEST_QUERIES), 4),
        "mrr": round(mrr_sum / len(TEST_QUERIES), 4),
        "top1_hits": top1_hits,
        "per_query": per_query,
    }


def main():
    load_project_env()
    parser = argparse.ArgumentParser(description="Nemotron 3.5 Embed 8B vs 1B 비교")
    parser.add_argument("--output", type=str, default=None, help="결과 JSON 저장 경로")
    parser.add_argument("--models", type=str, default="nemotron-8b,nemotron-1b", help="테스트할 모델 (콤마 구분)")
    parser.add_argument("--dry-run", action="store_true", help="API 호출 없이 구성만 검증")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        logger.error("OPENROUTER_API_KEY 환경변수 필요 (.env 파일에 설정)")
        return 2

    model_keys = [k.strip() for k in args.models.split(",") if k.strip()]
    selected_models = {k: MODELS[k] for k in model_keys if k in MODELS}

    if not selected_models:
        logger.error("유효한 모델 없음: %s (가능: %s)", model_keys, list(MODELS.keys()))
        return 2

    if args.dry_run:
        for key, mid in selected_models.items():
            logger.info("[dry-run] %s -> %s", key, mid)
        return 0

    print(f"\n{'=' * 70}")
    print("Nemotron 3.5 Embed 비교 테스트")
    print(f"{'=' * 70}")
    print(f"모델: {', '.join(selected_models.values())}")
    print(f"쿼리: {len(TEST_QUERIES)}개, 문서: {len(TEST_DOCS)}개")
    print()

    all_results = {}
    for key, model_id in selected_models.items():
        print(f"\n--- {key} ({model_id}) 테스트 중 ---")
        result = evaluate_model(key, model_id, api_key)

        if result.success:
            retrieval = evaluate_retrieval(result, DOC_SOURCES)
            all_results[key] = {
                "model_id": model_id,
                "native_dim": result.native_dim,
                "query_latency_ms": round(result.query_latency_ms, 1),
                "doc_latency_ms": round(result.doc_latency_ms, 1),
                "total_latency_ms": round(result.query_latency_ms + result.doc_latency_ms, 1),
                "retrieval": retrieval,
            }
            print(f"  차원: {result.native_dim}")
            print(f"  쿼리 임베딩: {result.query_latency_ms:.1f}ms")
            print(f"  문서 임베딩: {result.doc_latency_ms:.1f}ms")
            print(f"  Recall@5: {retrieval['recall_at_k']:.3f}")
            print(f"  MRR: {retrieval['mrr']:.3f}")
            print(f"  Top-1: {retrieval['top1_hits']}/{len(TEST_QUERIES)}")
        else:
            print(f"  실패: {result.error}")
            all_results[key] = {"model_id": model_id, "error": result.error}

    # 비교 요약
    if len(all_results) == 2:
        keys = list(all_results.keys())
        r1, r2 = all_results[keys[0]], all_results[keys[1]]
        if "error" not in r1 and "error" not in r2:
            print(f"\n{'=' * 70}")
            print("비교 요약")
            print(f"{'=' * 70}")
            print(f"{'지표':<25} {keys[0]:<15} {keys[1]:<15} {'차이'}")
            print("-" * 70)
            print(
                f"{'임베딩 차원':<25} {r1['native_dim']:<15} {r2['native_dim']:<15} {r1['native_dim'] - r2['native_dim']:+d}"
            )
            print(
                f"{'총 지연시간(ms)':<25} {r1['total_latency_ms']:<15.1f} {r2['total_latency_ms']:<15.1f} {r1['total_latency_ms'] - r2['total_latency_ms']:+.1f}"
            )
            print(
                f"{'Recall@5':<25} {r1['retrieval']['recall_at_k']:<15.3f} {r2['retrieval']['recall_at_k']:<15.3f} {r1['retrieval']['recall_at_k'] - r2['retrieval']['recall_at_k']:+.3f}"
            )
            print(
                f"{'MRR':<25} {r1['retrieval']['mrr']:<15.3f} {r2['retrieval']['mrr']:<15.3f} {r1['retrieval']['mrr'] - r2['retrieval']['mrr']:+.3f}"
            )
            print(
                f"{'Top-1 적중':<25} {r1['retrieval']['top1_hits']:<15} {r2['retrieval']['top1_hits']:<15} {r1['retrieval']['top1_hits'] - r2['retrieval']['top1_hits']:+d}"
            )

    # 결과 저장
    if args.output:
        Path(args.output).write_text(json.dumps(all_results, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("결과 저장: %s", args.output)
    else:
        print(json.dumps(all_results, ensure_ascii=False, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(main())
