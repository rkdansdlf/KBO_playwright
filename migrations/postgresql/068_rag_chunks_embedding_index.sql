-- 067_rag_chunks_embedding_index.sql
-- rag_chunks.embedding에 근사 코사인 인덱스를 만든다.
--
-- 왜 필요한가: 인덱스가 없으면 `ORDER BY embedding <=> query`가 37만 행 ×
-- 1536차원을 전부 훑는다. 2026-10-29 실측으로 `rag_chunks`는 **377,342행 ·
-- 8,629MB**였고 단순 `count(*)` 하나가 **232초**를 썼다 — 검색은 그보다 오래
-- 걸렸고, RAG 조회가 실용적 시간 안에 끝나지 않았다.
--
-- `migrations/pgvector/006_embedding_ivfflat_index.sql`이 같은 인덱스를 만들지만
-- 그 체인에는 러너가 없다(003이 겪은 것과 같은 이유 — 그 파일 주석은 "로컬
-- Docker pgvector (kbo_rag DB)"를 실행 대상으로 적고 있다). 004·005가 만드는
-- 컬럼들은 이미 DB에 있었지만 003의 컬럼과 006의 인덱스는 없었다.
--
-- lists 선택: 006은 100을 썼다. pgvector의 권장은 rows/1000(≈377)이고
-- `sqrt(rows)`(≈614)가 상한이다. 100은 재현율이 낮아질 수 있어 400으로 둔다 —
-- 질의가 IVF 리스트 일부만 방문하므로 리스트가 너무 적으면 정답을 놓친다.
-- 재현율은 `python3 -m src.cli.rag.evaluate_rag_retrieval`로 확인해야 한다.
--
-- 일반 CREATE INDEX로 만든다(러너가 트랜잭션 안에서 실행하므로 CONCURRENTLY를
-- 쓸 수 없다). 빌드 동안 쓰기가 잠기므로 **저트래픽 시간에 적용할 것**.

CREATE INDEX IF NOT EXISTS idx_rag_chunks_embedding_ivfflat
    ON rag_chunks
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 400);
