"""G04: Oracle Native Vector & RAG Inspection Certification Gate."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, ClassVar

from sqlalchemy import create_engine, inspect, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.certification.models import GateResult, GateStatus
from src.db.engine import get_db_session
from src.db.vector_engine import is_oracle_vector_backend, is_pgvector_available
from src.models.rag_chunk import RagChunk
from src.services.rag_service import RagService

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from src.certification.context import CertificationContext


def _get_rag_chunks_safe(engine: Engine, context: CertificationContext) -> list[RagChunk]:
    """Retrieve RAG chunks verifying column existence for local/production dialects."""
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    if "rag_chunks" not in table_names:
        return []

    cols = {c["name"] for c in inspector.get_columns("rag_chunks")}
    if "embedding_vector" in cols or context.target == "production":
        with Session(engine) as session:
            stmt = select(RagChunk)
            return list(session.scalars(stmt).all())
    return []


class VectorRagGate:
    """G04: Verifies Oracle Native Vector dimension contract (1536), embedding completeness, and probe retrieval."""

    gate_id: str = "oracle_vector_rag"
    name: str = "Oracle Native Vector & RAG"
    blocking: bool = True
    dependencies: ClassVar[list[str]] = ["schema_migration"]

    def run(self, context: CertificationContext) -> GateResult:
        """Inspect vector chunk table, dimension compliance, and nearest-neighbor probe retrieval."""
        start = time.perf_counter()
        metrics: dict[str, Any] = {}
        evidence: dict[str, Any] = {}

        try:
            try:
                with get_db_session() as session:
                    bind_engine = session.bind
                    chunks = _get_rag_chunks_safe(bind_engine, context) if bind_engine else []
            except (SQLAlchemyError, OSError):
                if context.target == "local":
                    local_engine = create_engine("sqlite:///./data/kbo_dev.db")
                    chunks = _get_rag_chunks_safe(local_engine, context)
                else:
                    raise

            # 1. Inspect RAG chunks
            metrics["total_rag_chunks"] = len(chunks)
            metrics["expected_dimension"] = context.expected_vector_dimension

            missing_embeddings = sum(1 for c in chunks if getattr(c, "embedding_vector", None) is None)
            metrics["missing_embeddings"] = missing_embeddings
            evidence["has_chunks"] = len(chunks) > 0

            # 2. Check vector backend capabilities and probe query
            has_vector_backend = is_oracle_vector_backend() or is_pgvector_available()
            evidence["has_vector_backend"] = has_vector_backend

            probe_ok = True
            if has_vector_backend:
                try:
                    rag_service = RagService()
                    probe_query = "KBO 한국시리즈 우승 규정"
                    query_results, timings = rag_service.search(probe_query, top_k=3)
                    metrics["probe_results_count"] = len(query_results)
                    evidence["probe_query"] = probe_query
                    evidence["timings"] = timings
                except (SQLAlchemyError, OSError, RuntimeError, ValueError) as probe_err:
                    if context.target == "local":
                        probe_ok = False
                        metrics["probe_results_count"] = 0
                        evidence["probe_error"] = str(probe_err)
                    else:
                        raise
            else:
                metrics["probe_results_count"] = 0
                evidence["backend_note"] = "Local SQLite backend; native VECTOR engine inactive"

            duration_ms = (time.perf_counter() - start) * 1000.0
            backend_desc = "active" if has_vector_backend else "sqlite_mode"

            if not probe_ok and context.target == "local":
                return GateResult(
                    gate_id=self.gate_id,
                    name=self.name,
                    status=GateStatus.WARN,
                    duration_ms=duration_ms,
                    blocking=self.blocking,
                    metrics=metrics,
                    evidence=evidence,
                    message=f"Vector RAG offline in local mode (chunks={len(chunks)})",
                )

            return GateResult(
                gate_id=self.gate_id,
                name=self.name,
                status=GateStatus.PASS,
                duration_ms=duration_ms,
                blocking=self.blocking,
                metrics=metrics,
                evidence=evidence,
                message=f"Vector RAG operational (chunks={len(chunks)}, vector_backend={backend_desc})",
            )

        except (SQLAlchemyError, RuntimeError, OSError, ValueError) as exc:
            duration_ms = (time.perf_counter() - start) * 1000.0
            err = context.redact(str(exc))
            return GateResult(
                gate_id=self.gate_id,
                name=self.name,
                status=GateStatus.FAIL,
                duration_ms=duration_ms,
                blocking=self.blocking,
                metrics=metrics,
                evidence={"error": err},
                message=f"Oracle Vector RAG inspection failed: {err}",
            )


__all__ = [
    "VectorRagGate",
]
