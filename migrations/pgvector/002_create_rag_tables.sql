-- 002_create_rag_tables.sql
-- RAG 청크 및 임베딩 캐시 테이블 생성 (1536차원 Vector 타입)
-- 실행 대상: 로컬 Docker pgvector (kbo_rag DB)
-- 사전 조건: 001_enable_pgvector_extension.sql 실행 완료

-- ─── rag_chunks: KBO 지식 청크 + 1536차원 임베딩 ──────────────────────────────
CREATE TABLE IF NOT EXISTS rag_chunks (
    id          BIGSERIAL PRIMARY KEY,
    season_year INTEGER,
    season_id   INTEGER,
    league_type_code INTEGER,
    team_id     VARCHAR(10),
    player_id   VARCHAR(20),
    source_table TEXT    NOT NULL,
    source_row_id TEXT   NOT NULL,
    title       TEXT,
    content     TEXT    NOT NULL,
    embedding   vector(1536),
    meta        JSONB   NOT NULL DEFAULT '{}',
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 중복 방지: source_table + source_row_id 조합 유일
CREATE UNIQUE INDEX IF NOT EXISTS uq_rag_chunks_source
    ON rag_chunks (source_table, source_row_id);

-- 필터링용 인덱스
CREATE INDEX IF NOT EXISTS idx_rag_chunks_team_id     ON rag_chunks (team_id);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_player_id   ON rag_chunks (player_id);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_season_year ON rag_chunks (season_year);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_source_table ON rag_chunks (source_table);

-- IVFFlat 코사인 유사도 인덱스 (100개 리스트, 데이터 충분 시 활성화)
-- 주의: 10,000행 이상일 때 성능 효과. 소량 데이터는 flat scan이 더 빠름.
-- CREATE INDEX IF NOT EXISTS idx_rag_chunks_embedding_ivfflat
--     ON rag_chunks USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- ─── embedding_cache: SHA-256 해시 기반 임베딩 캐시 ─────────────────────────
CREATE TABLE IF NOT EXISTS embedding_cache (
    text_hash   VARCHAR(64)  NOT NULL,
    model_name  VARCHAR(100) NOT NULL,
    embedding   vector(1536) NOT NULL,
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (text_hash, model_name)
);
