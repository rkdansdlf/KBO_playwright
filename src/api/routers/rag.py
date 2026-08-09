"""RAG search and hybrid retrieval API router."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from src.api.auth import get_api_key
from src.db.engine import get_db_session
from src.services.hybrid_retriever import HybridRetriever

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


router = APIRouter(tags=["RAG Search"])


class RagSearchRequest(BaseModel):
    """RAG 유사도 검색 요청 모델."""

    query: str = Field(..., min_length=1, max_length=500, description="자연어 검색 쿼리")
    top_k: int = Field(default=5, ge=1, le=50, description="반환할 최대 결과 수")
    category: str | None = Field(default=None, description="카테고리 필터")
    filters: dict[str, Any] | None = Field(
        default=None,
        description="필터 dict: team_id, season_year, source_table 지원",
    )


@router.post("/api/rag/search", dependencies=[Depends(get_api_key)])
@router.post("/api/v1/rag/search", dependencies=[Depends(get_api_key)])
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


@router.post("/api/v1/rag/hybrid-search", dependencies=[Depends(get_api_key)])
def rag_hybrid_search(request: RagSearchRequest) -> dict[str, Any]:
    """Reciprocal Rank Fusion (RRF) 기반 하이브리드 지식 검색을 수행합니다."""
    try:
        with get_db_session() as session:
            retriever = HybridRetriever(session)
            results = retriever.retrieve(
                query=request.query,
                top_k=request.top_k,
                category=request.category,
            )
            return {
                "query": request.query,
                "total": len(results),
                "results": [r.to_dict() for r in results],
            }
    except Exception as e:
        logger.exception("Hybrid RAG search failed")
        raise HTTPException(status_code=500, detail=f"하이브리드 RAG 검색 오류: {e}") from e
