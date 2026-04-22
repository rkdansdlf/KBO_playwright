# 데이터 무결성 개선 로그 (Data Integrity Log)

## [2026-04-19] 대규모 데이터 무결성 보정 및 자동 감시 체계 구축

### 1. 개요
시스템 내 장기간 누적된 고아 데이터(Orphaned Stats)와 식별 불가 선수(NULL Player ID) 문제를 전수 조사하고, 이를 복구 및 자동화된 감시 체계로 전환한 프로젝트입니다.

### 2. 주요 작업 내용

#### **가. 고아 데이터(Orphaned Data) 100% 복구**
*   **대상:** `game_batting_stats`, `game_pitching_stats`, `game_play_by_play` 등 상세 테이블
*   **성과:** 부모(`game`) 정보가 없던 **약 7,400여 건**의 경기 메타데이터를 KBO 공식 사이트로부터 역추적하여 복구 완료.
*   **특이사항:** 우천 취소 등으로 데이터가 없는 경기는 `CANCELED` 상태로 기록하여 중복 수집 시도 방지.

#### **나. 선수 식별 번호(Player ID) 전수 보정**
*   **대상:** 모든 스탯 테이블 내 NULL `player_id` 행
*   **성과:** **총 15,138건**의 누락된 ID를 이름/팀/연도 기반의 `PlayerIdResolver` 로직을 통해 모두 채워 넣음.
*   **팀 코드 표준화:** 옛 현대 유니콘스 코드(`HD`)를 공식 코드(`HU`)로 통합 변환 (1.4만여 행).

#### **다. 시스템 구조 및 방어 체계 강화**
*   **스키마 개선:** SQLAlchemy 모델에 `ForeignKey` 제약 조건을 도입하여 데이터 간 물리적 연결성 강제.
*   **파서 고도화:** `GameDetailCrawler`에 선수 ID 누락 시 자동 역추적(Fallback) 로직 통합.
*   **성능 최적화:** SQLite 대량 작업 시 `database is locked` 에러 방지를 위한 타임아웃(30s) 및 커넥션 튜닝.

#### **라. 정기 품질 모니터링 자동화**
*   **GitHub Actions:** 매일 새벽 4시(KST) OCI DB의 무결성을 검사하는 `quality_check.yml` 워크플로우 구축.
*   **Quality Gate:** 고아 데이터, NULL ID, 상태 지연 경기 등을 감시하고 Slack으로 즉시 알림 전송.

### 3. 최종 상태
*   **Local/OCI 데이터 일치성(Parity):** 100% 달성
*   **무결성 결함 수치:** 0 (Quality Gate PASS)
*   **Baseline 업데이트:** 향후 운영 기준이 될 새로운 데이터 품질 Baseline 수립 완료.

---
*기록자: Gemini CLI (Interactive Engineering Mode)*
