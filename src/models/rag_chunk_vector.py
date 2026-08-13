"""pgvector 전용 RagChunk 모델 — 1536차원 Vector 타입으로 임베딩 저장.

메인 SQLite DB의 rag_chunk.py(JSON 임베딩)와 별개로,
로컬 Docker pgvector DB에서만 사용되는 모델입니다.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pgvector.sqlalchemy import Vector
from sqlalchemy import BigInteger, Date, DateTime, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.db.vector_engine import VectorBase
from src.models.base import TimestampMixin


class RagChunkVector(VectorBase, TimestampMixin):
    """pgvector DB에 저장되는 KBO 지식 청크 (1536차원 임베딩 포함).

    source_table + source_row_id 조합으로 중복을 방지하며,
    cosine 유사도 검색(<=>) 연산자를 통해 의미 기반 검색을 지원합니다.
    """

    __tablename__ = "rag_chunks"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # 맥락 메타데이터
    season_year: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="시즌 연도")
    season_id: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="시즌 ID")
    league_type_code: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="리그 레벨 코드 (0=정규시즌)")
    team_id: Mapped[str | None] = mapped_column(String(10), nullable=True, comment="팀 코드")
    player_id: Mapped[str | None] = mapped_column(String(20), nullable=True, comment="선수 ID")
    document_type: Mapped[str | None] = mapped_column(
        String(30), nullable=True, comment="문서 유형 (player_profile/season_batting/...)"
    )
    game_date: Mapped[date | None] = mapped_column(Date, nullable=True, comment="대상 날짜 (경기일/이동일/이벤트일)")
    published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="발행 시각 (뉴스/이벤트)")
    source_url: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="원문 출처 URL")
    language: Mapped[str | None] = mapped_column(String(10), nullable=True, comment="언어 코드 (기본 ko)")

    # 출처 식별
    source_table: Mapped[str] = mapped_column(Text, nullable=False, comment="원본 테이블명 (e.g. player_basic, game)")
    source_row_id: Mapped[str] = mapped_column(Text, nullable=False, comment="원본 레코드 식별자")

    # 청크 내용
    title: Mapped[str | None] = mapped_column(Text, nullable=True, comment="청크 제목")
    content: Mapped[str] = mapped_column(Text, nullable=False, comment="임베딩할 전체 텍스트")

    # 1536차원 벡터 임베딩 (pplx-embed-v1-4b, pgvector Vector 타입)
    embedding: Mapped[list[float] | None] = mapped_column(
        Vector(1536), nullable=True, comment="1536차원 float 임베딩 벡터"
    )

    # 추가 메타데이터
    meta: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default="{}",
        comment="추가 구조화 메타데이터",
    )

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return f"<RagChunkVector(id={self.id}, source='{self.source_table}', title='{self.title}')>"
