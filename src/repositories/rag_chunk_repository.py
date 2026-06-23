"""
Repository for managing RAG chunks in the SQLite/Postgres database.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from src.constants import KST
from src.models.rag_chunk import RagChunk

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class RagChunkRepository:
    """
    Data Access Object (DAO) for managing rag_chunks.
    """

    def upsert_chunks(self, session: Session, chunks: list[dict[str, Any]]) -> int:
        """
        Saves or updates RAG chunks using a clean, database-agnostic query-and-upsert approach.
        """
        upserted_count = 0
        now = datetime.now(KST)

        for chunk_data in chunks:
            title = chunk_data.get("title", "")
            content = chunk_data.get("content", "")
            meta = chunk_data.get("meta", {})
            embedding = chunk_data.get("embedding")

            source_table = meta.get("category", "unknown")
            source_row_id = meta.get("source_row_id", "")

            season_year = meta.get("season_year")
            season_id = meta.get("season_id")
            league_type_code = meta.get("league_type_code")
            team_id = meta.get("team_id")
            player_id = meta.get("player_id")

            # Check if chunk exists by source_table & source_row_id
            stmt = select(RagChunk).where(
                RagChunk.source_table == source_table,
                RagChunk.source_row_id == source_row_id,
            )
            existing_chunk = session.scalar(stmt)

            if existing_chunk:
                # Update fields
                existing_chunk.title = title
                existing_chunk.content = content
                existing_chunk.embedding = embedding
                existing_chunk.meta = meta
                existing_chunk.season_year = season_year
                existing_chunk.season_id = season_id
                existing_chunk.league_type_code = league_type_code
                existing_chunk.team_id = team_id
                existing_chunk.player_id = player_id
                existing_chunk.updated_at = now
            else:
                # Insert new chunk
                new_chunk = RagChunk(
                    title=title,
                    content=content,
                    source_table=source_table,
                    source_row_id=source_row_id,
                    embedding=embedding,
                    meta=meta,
                    season_year=season_year,
                    season_id=season_id,
                    league_type_code=league_type_code,
                    team_id=team_id,
                    player_id=player_id,
                    created_at=now,
                    updated_at=now,
                )
                session.add(new_chunk)

            upserted_count += 1
            if upserted_count % 100 == 0:
                session.commit()

        session.commit()
        return upserted_count
