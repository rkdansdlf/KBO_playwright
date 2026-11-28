-- 070_rag_query_events.sql
-- L3 실사용 질의 분포 측정을 위한 append-only 원장.
--
-- 원문 질의는 저장하지 않는다. 공백을 정규화한 문자열의 sha256과 길이만 남기므로
-- 이 표는 사용자 입력을 담는 저장소가 아니라 라우팅 관측치만 담는 저장소다.
-- 표본이 쌓이면 kbo rag traffic 이 route/outcome 분포를 읽는다.
--
-- 신규 표이므로 CREATE TABLE IF NOT EXISTS로 충분하며 기존 표를 건드리지 않는다.
-- 인덱스 이름은 SQLite 체인과 동일하게 유지한다.

CREATE TABLE IF NOT EXISTS rag_query_events (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    query_hash CHAR(64) NOT NULL,
    query_chars INTEGER NOT NULL DEFAULT 0,
    route VARCHAR(16) NOT NULL,
    intent VARCHAR(32),
    outcome VARCHAR(32) NOT NULL,
    source_table VARCHAR(64),
    result_count INTEGER NOT NULL DEFAULT 0,
    structured_candidates INTEGER NOT NULL DEFAULT 0,
    fallback_used BOOLEAN NOT NULL DEFAULT false,
    latency_ms DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_rag_query_events_created_at ON rag_query_events (created_at);

CREATE INDEX IF NOT EXISTS idx_rag_query_events_outcome_created_at ON rag_query_events (outcome, created_at);
