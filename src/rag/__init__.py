"""KBO RAG (Retrieval-Augmented Generation) Domain Package."""

from __future__ import annotations

from src.rag.base_retriever import BaseRetriever
from src.rag.dto import (
    RagDocument,
    RagEvaluationMetrics,
    RagEvaluationReport,
    RagGoldenQuery,
    RagLatencyBreakdown,
    RetrievalCandidate,
    RetrievalQuery,
    RetrievalResult,
)
from src.rag.evaluation import RagEvaluator
from src.rag.evaluation_gateway import RagEvaluationGateway
from src.rag.indexer.knowledge_indexer import KnowledgeIndexer
from src.rag.retrievers.hybrid import UnifiedHybridRetriever
from src.rag.retrievers.oracle_dense import OracleDenseRetriever
from src.rag.retrievers.sparse_bm25 import SparseBM25Retriever

__all__ = [
    "BaseRetriever",
    "KnowledgeIndexer",
    "OracleDenseRetriever",
    "RagDocument",
    "RagEvaluationGateway",
    "RagEvaluationMetrics",
    "RagEvaluationReport",
    "RagEvaluator",
    "RagGoldenQuery",
    "RagLatencyBreakdown",
    "RetrievalCandidate",
    "RetrievalQuery",
    "RetrievalResult",
    "SparseBM25Retriever",
    "UnifiedHybridRetriever",
]
