-- 071_rag_query_events.sql
-- PostgreSQL 070과 같은 계약이다. SQLite는 init_db의 create_all로도 이 표를 만들지만
-- 체인을 그대로 두면 PG 서버 없이 컬럼·인덱스 계약을 검사할 수 있다.
-- 원문 질의는 저장하지 않고 sha256과 길이만 남긴다.

CREATE TABLE IF NOT EXISTS rag_query_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    query_hash CHAR(64) NOT NULL,
    query_chars INTEGER NOT NULL DEFAULT 0,
    route VARCHAR(16) NOT NULL,
    intent VARCHAR(32),
    outcome VARCHAR(32) NOT NULL,
    source_table VARCHAR(64),
    result_count INTEGER NOT NULL DEFAULT 0,
    structured_candidates INTEGER NOT NULL DEFAULT 0,
    fallback_used BOOLEAN NOT NULL DEFAULT 0,
    latency_ms REAL
);

CREATE INDEX IF NOT EXISTS idx_rag_query_events_created_at ON rag_query_events (created_at);

CREATE INDEX IF NOT EXISTS idx_rag_query_events_outcome_created_at ON rag_query_events (outcome, created_at);
