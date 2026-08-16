"""Repository for managing RAG chunks in the Oracle primary database."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from src.constants import KST
from src.models.rag_chunk import RagChunk
from src.services.rag_index_identity import ACTIVE_INDEX_STATUS, chunk_content_hash, current_index_version

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class RagChunkRepository:
    """Data Access Object (DAO) for managing rag_chunks."""

    def upsert_chunks(self, session: Session, chunks: list[dict[str, Any]], *, commit: bool = True) -> int:
        """Save or updates RAG chunks using a clean, database-agnostic query-and-upsert approach.

        Args:
            session: Session.
            chunks: Chunks.
            commit: Commit the session after upserting chunks.

        """
        upserted_count = 0

        now = datetime.now(KST)

        for chunk_data in chunks:
            title = chunk_data.get("title", "")
            content = chunk_data.get("content", "")
            meta = dict(chunk_data.get("meta") or {})
            embedding = chunk_data.get("embedding")

            for meta_key in ("document_type", "source_url", "language", "game_date"):
                if chunk_data.get(meta_key) is not None:
                    meta_value = chunk_data[meta_key]
                    if hasattr(meta_value, "isoformat"):
                        meta_value = meta_value.isoformat()
                    meta.setdefault(meta_key, meta_value)

            source_table = chunk_data.get("source_table", meta.get("source_table", meta.get("category", "unknown")))
            source_row_id = chunk_data.get("source_row_id", meta.get("source_row_id", ""))
            content_hash = chunk_data.get("content_hash") or chunk_content_hash(title, content)
            index_version = chunk_data.get("index_version") or current_index_version()
            index_status = chunk_data.get("index_status") or ACTIVE_INDEX_STATUS

            season_year = chunk_data.get("season_year", meta.get("season_year"))
            season_id = chunk_data.get("season_id", meta.get("season_id"))
            league_type_code = chunk_data.get("league_type_code", meta.get("league_type_code"))
            team_id = chunk_data.get("team_id", meta.get("team_id"))
            player_id = chunk_data.get("player_id", meta.get("player_id"))

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
                existing_chunk.content_hash = content_hash
                existing_chunk.index_version = index_version
                existing_chunk.index_status = index_status
                existing_chunk.indexed_at = now
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
                    content_hash=content_hash,
                    index_version=index_version,
                    index_status=index_status,
                    indexed_at=now,
                    created_at=now,
                    updated_at=now,
                )
                session.add(new_chunk)

            upserted_count += 1
            if commit and upserted_count % 100 == 0:
                session.commit()

        if commit:
            session.commit()
        return upserted_count
