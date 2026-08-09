-- 001_enable_pgvector_extension.sql
-- pgvector 확장 활성화 — Vector 타입과 코사인 유사도 연산자 (<=>)를 사용하기 위해 필요
-- 실행 대상: 로컬 Docker pgvector (kbo_rag DB)

CREATE EXTENSION IF NOT EXISTS vector;
