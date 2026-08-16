"""RAG search and hybrid retrieval API router."""

from __future__ import annotations

import logging
import re
import time
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from src.api.auth import get_api_key
from src.api.schemas import HybridSearchResponse, RagAskResponse
from src.db.engine import get_db_session
from src.services.hybrid_retriever import HybridRetriever
from src.services.query_router import QueryRouter, RetrievalRoute
from src.services.structured_retriever import StructuredRetriever

if TYPE_CHECKING:
    from src.services.rag_service import RagService

logger = logging.getLogger(__name__)

_rag_state: dict[str, RagService | None] = {"service": None}


def _get_rag_service() -> RagService:
    """Get or create singleton RagService instance."""
    service = _rag_state["service"]
    if service is None:
        from src.services.rag_service import RagService

        service = RagService()
        _rag_state["service"] = service
    return service


router = APIRouter(tags=["KBO RAG & AI Hybrid Search"])


class RagSearchRequest(BaseModel):
    """RAG 유사도 검색 요청 모델."""

    query: str = Field(..., min_length=1, max_length=500, description="자연어 검색 쿼리")
    top_k: int = Field(default=5, ge=1, le=50, description="반환할 최대 결과 수")
    category: str | None = Field(
        default=None,
        description=("카테고리 필터 (press_release, milestone, futures_schedule, player_splits, stadium_facility)"),
    )
    filters: dict[str, Any] | None = Field(
        default=None,
        description="필터 dict: team_id, season_year, source_table 지원",
    )


@router.post(
    "/api/rag/search",
    dependencies=[Depends(get_api_key)],
    summary="KBO 지식 베이스 의미론적 유사도 검색",
)
@router.post(
    "/api/v1/rag/search",
    dependencies=[Depends(get_api_key)],
    summary="KBO 지식 베이스 의미론적 유사도 검색 (v1)",
)
def rag_search(request: RagSearchRequest) -> dict[str, Any]:
    """KBO 지식 베이스에서 의미적으로 유사한 청크를 검색합니다."""
    try:
        service = _get_rag_service()
        results, timings = service.search(
            query=request.query,
            top_k=request.top_k,
            filters=request.filters or {},
        )
        return {
            "query": request.query,
            "total": len(results),
            "results": [r.to_dict() for r in results],
            "timings": timings,
        }
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except Exception as e:
        logger.exception("RAG search failed")
        raise HTTPException(status_code=500, detail=f"RAG 검색 오류: {e}") from e


@router.post(
    "/api/v1/rag/hybrid-search",
    dependencies=[Depends(get_api_key)],
    response_model=HybridSearchResponse,
    summary="Dense Vector + BM25 RRF 하이브리드 지식 검색",
)
def rag_hybrid_search(request: RagSearchRequest) -> dict[str, Any]:
    """Reciprocal Rank Fusion (RRF) 기반 Dense Vector 및 BM25 키워드 하이브리드 지식 검색을 수행합니다."""
    try:
        with get_db_session() as session:
            retriever = HybridRetriever(session)
            results = retriever.retrieve(
                query=request.query,
                top_k=request.top_k,
                category=request.category,
                filters=request.filters,
            )
            return {
                "query": request.query,
                "total_results": len(results),
                "results": [r.to_dict() for r in results],
                "retrieval": retriever.last_trace,
            }
    except Exception as e:
        logger.exception("Hybrid RAG search failed")
        raise HTTPException(status_code=500, detail="하이브리드 RAG 검색 오류") from e


def _build_rag_answer(
    query: str,
    results: list[Any],
    *,
    analysis: dict[str, Any] | None = None,
    retrieval: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a grounded answer and structured source metadata from hybrid hits."""
    if not results:
        return {
            "query": query,
            "answer": f"'{query}'에 관한 검색 결과를 찾지 못했습니다.",
            "sources": [],
            "chunks": [],
            "chunk_count": 0,
            "analysis": analysis or {},
            "retrieval": retrieval or {},
        }

    query_terms = [term for term in re.findall(r"[\w가-힣]+", query) if len(term) > 1]
    sources: list[dict[str, Any]] = []
    for result in results:
        snippet = result.content[:240]
        for term in query_terms:
            snippet = re.sub(re.escape(term), f"**{term}**", snippet, flags=re.IGNORECASE)
        sources.append(
            {
                "title": result.title,
                "source_url": result.source_url,
                "document_type": result.category,
                "snippet": snippet,
                "meta": result.meta,
                "provenance": result.provenance,
            }
        )

    summaries = [f"- {source['title'] or 'KBO 문서'}: {source['snippet']}" for source in sources[:3]]
    return {
        "query": query,
        "answer": f"검색된 주요 KBO 정보 ({len(results)}건):\n" + "\n".join(summaries),
        "sources": sources,
        "chunks": [result.to_dict() for result in results],
        "chunk_count": len(results),
        "analysis": analysis or {},
        "retrieval": retrieval or {},
    }


@router.post(
    "/api/v1/rag/ask",
    dependencies=[Depends(get_api_key)],
    response_model=RagAskResponse,
    summary="KBO 지식 베이스 질의응답 (Q&A Context & Sources)",
)
def rag_ask(request: RagSearchRequest) -> dict[str, Any]:
    """자연어 질문에 대해 관련 지식 청크를 추출하고 답변 요약 및 소스 URL을 반환합니다."""
    try:
        request_started = time.perf_counter()
        with get_db_session() as session:
            plan = QueryRouter(session).plan(
                request.query,
                category=request.category,
                filters=request.filters,
            )
            structured_results = []
            if plan.route in {RetrievalRoute.STRUCTURED, RetrievalRoute.MIXED}:
                structured_started = time.perf_counter()
                structured_results = StructuredRetriever(session).retrieve(plan, request.top_k)
                structured_ms = round((time.perf_counter() - structured_started) * 1000, 3)
            else:
                structured_ms = 0.0

            retrieval: dict[str, Any]
            if plan.route is RetrievalRoute.STRUCTURED and structured_results:
                results = [result.to_hybrid_result() for result in structured_results]
                retrieval = {
                    "mode": "structured",
                    "route": plan.route.value,
                    "fallback_used": False,
                    "structured_candidates": len(structured_results),
                    "bm25_candidates": 0,
                    "vector_candidates": 0,
                    "rerank_candidates": 0,
                    "returned": len(results),
                    "latency_ms": {
                        "router": plan.timings.get("router", 0.0),
                        "resolver": plan.timings.get("resolver", 0.0),
                        "structured": structured_ms,
                        "bm25": 0.0,
                        "vector": 0.0,
                        "reranker": 0.0,
                        "total": round((time.perf_counter() - request_started) * 1000, 3),
                    },
                }
            else:
                retriever = HybridRetriever(session)
                hybrid_limit = max(request.top_k - len(structured_results), 1)
                hybrid_results = retriever.retrieve(
                    query=request.query,
                    top_k=hybrid_limit if plan.route is RetrievalRoute.MIXED else request.top_k,
                    category=request.category,
                    filters=plan.filters,
                )
                if plan.route is RetrievalRoute.MIXED and structured_results:
                    results = [result.to_hybrid_result() for result in structured_results] + hybrid_results
                    results = results[: request.top_k]
                else:
                    results = hybrid_results
                retrieval = {
                    **retriever.last_trace,
                    "mode": "mixed" if plan.route is RetrievalRoute.MIXED and structured_results else "document",
                    "route": plan.route.value,
                    "fallback_used": plan.route in {RetrievalRoute.STRUCTURED, RetrievalRoute.MIXED}
                    and not structured_results,
                    "structured_candidates": len(structured_results),
                    "structured_fallback": plan.route in {RetrievalRoute.STRUCTURED, RetrievalRoute.MIXED}
                    and not structured_results,
                }
            return _build_rag_answer(
                request.query,
                results,
                analysis=plan.to_analysis(),
                retrieval=retrieval,
            )
    except Exception as e:
        logger.exception("RAG Q&A failed")
        raise HTTPException(status_code=500, detail="RAG 질의응답 오류") from e
