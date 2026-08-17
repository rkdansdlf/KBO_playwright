# Transaction Ownership Contract

> P0.5 문서 — 2026-08-17 갱신

## 1. 목적

Repository 계층의 트랜잭션 소유권 규칙을 고정하고, 이를 실행 가능한 증거
(정적 감사 + 동적 테스트)로 검증하기 위한 문서입니다.

핵심 규칙:

> **Repository는 데이터를 읽고 쓰지만 transaction을 끝내지 않는다.
> Service/Caller가 transaction을 시작하고 끝낸다.**

## 2. Repository Contract

1. Repository는 Session을 생성하지 않는다.
2. Repository는 Session을 close하지 않는다.
3. Repository는 commit하지 않는다.
4. Repository는 rollback하지 않는다.
5. 필요하면 flush는 할 수 있다.
6. 함수형 API는 `session: Session | None = None`을 허용하며,
   `None`일 경우 `get_db_session()` / `get_rag_index_session()` 폴백으로
   트랜잭션 범위를 생성한다 (Compatibility Facade).

`flush()`는 허용한다. INSERT 직후 DB-generated PK가 필요하거나 후속 쿼리에
반영해야 하는 경우 repository 레벨에서 flush하는 것이 합리적이기 때문이다.

### Compatibility Facade Contract

계약 1과 6은 개념적으로 다른 두 계층이다:

- **Core Contract**: 주입받은 Session만 사용. commit/rollback/close/session 생성 금지.
- **Compatibility Facade Contract**: `session=None` 허용. session이 없을 경우
  `get_db_session()`으로 transaction scope를 생성하고 내부 core 로직을 호출.

```python
def _save_game(session: Session, game: GameData) -> None:
    ...  # core: session 사용, commit 없음

def save_game(game: GameData, session: Session | None = None) -> None:
    if session is not None:
        _save_game(session, game)
        return
    with get_db_session() as local_session:  # facade: 자동 commit/rollback
        _save_game(local_session, game)
```

`get_db_session()`은 `src/db/engine.py`에서 다음과 같이 동작한다:

```python
session = SessionLocal()
try:
    yield session
    session.commit()       # 성공 시 자동 commit
except DB_SESSION_EXCEPTIONS:
    session.rollback()     # 실패 시 자동 rollback
    raise
finally:
    session.close()
```

## 3. 현재 상태 (검증 기준 2026-08-17)

### 전수 감사 결과

`src/repositories/` 46개 파일을 AST 기반으로 전수 검사한 결과:

- **Repository core path의 직접 commit/rollback/session 생성: 0건**
- **Compatibility facade(session-optional 폴백): 8개 파일**

| 파일 | facade 폴백 | 비고 |
|---|---|---|
| `game_save.py` | `get_db_session` | save_* 시리즈 |
| `game_relay.py` | `get_db_session` | relay 저장/복구 |
| `game_status.py` | `get_db_session` | 상태 갱신 |
| `safe_batting_repository.py` | `get_db_session` | 타격 통계 |
| `player_season_pitching_repository.py` | `get_db_session` | 투수 통계 |
| `player_basic_repository.py` | `get_db_session` | 선수 기본 정보 |
| `relay_repository.py` | `get_db_session` | relay 조회 |
| `rag_chunk_repository.py` | `get_rag_index_session` | RAG chunk upsert |

### 정확한 표현 (이전 보고서 수정 사항)

- ~~"commit 29건, rollback 24건, SessionLocal 41건, 총 94건 제거"~~ →
  git 이력으로 재현 불가한 수치이므로 **삭제**. 현재는 "전수 감사 후
  strict-core 위반을 제거했다"는 표현을 사용한다.
- ~~"commit 0건"~~ → "Repository core path의 직접 commit/rollback/session
  생성 위반 0건. Compatibility-managed path(계약 6)의 lifecycle 제어는
  명시적 예외로 유지"가 정확한 표현이다.

## 4. 정적 감사 스크립트

```bash
python3 scripts/audit_transaction_ownership.py          # src + scripts 스캔
python3 scripts/audit_transaction_ownership.py -v       # OK/CALLER 포함 출력
```

- 스캔 대상: `src/` + `scripts/` 전체 Python 파일
- 감지 마커: `.commit()`, `.rollback()`, `.close()`, `.begin()`,
  `SessionLocal(`, `Session(`, `sessionmaker(`, `get_db_session(`,
  `get_rag_index_session(`
- 분류:
  - `OK` — 마커 없음
  - `COMPAT_FACADE` — session-optional 폴백 또는 allowlist 파일
  - `CALLER` — 서비스/CLI/스크립트 (트랜잭션 소유자, 허용)
  - `STRICT_VIOLATION` — allowlist 밖 repository의 lifecycle 제어
- exit code: `0` = 계약 충족, `1` = 위반 발견, `2` = 사용 오류

### Allowlist

`ALLOWED_COMPATIBILITY_FACADES` — 현재 어떤 파일도 commit/rollback을
수행하지 않지만, 회귀 방지 목적으로 유지한다:

```python
ALLOWED_COMPATIBILITY_FACADES = {
    "game_save.py",
    "game_relay.py",
    "game_status.py",
    "safe_batting_repository.py",
    "player_season_pitching_repository.py",
    "rag_chunk_repository.py",
}
```

## 5. 동적 테스트 매핑

| 검증 항목 | 테스트 | 위치 |
|---|---|---|
| 개별 repo가 caller session을 commit하지 않음 | `test_team_repo_does_not_commit` 등 | `tests/repositories/test_repository_contract.py` |
| 복수 repo가 한 session 공유 | `test_multiple_repos_share_session` | `tests/repositories/test_repository_contract.py` |
| 실패 시 전체 rollback (repo 수준) | `test_rollback_on_failure_reverts_all` | `tests/repositories/test_repository_contract.py` |
| UPSERT 멱등성 | `test_*_upsert_idempotent` | `tests/repositories/test_repository_contract.py` |
| **session=None 성공 경로 자동 commit** | `test_compat_api_without_session_commits_on_success` | `tests/repositories/test_repository_compat_transaction.py` |
| **session=None 실패 경로 자동 rollback** | `test_compat_api_without_session_rolls_back_on_failure` | `tests/repositories/test_repository_compat_transaction.py` |
| **SQLAlchemyError 시 False 반환 + rollback** | `test_compat_api_without_session_sqlalchemy_error_returns_false` | `tests/repositories/test_repository_compat_transaction.py` |
| **Service → multi-table 원자 저장 (성공)** | `test_collection_service_commits_multiple_tables_atomically` | `tests/integration/test_transaction_atomicity_e2e.py` |
| **Service → multi-table 원자 rollback (실패)** | `test_collection_service_rolls_back_all_tables_on_failure` | `tests/integration/test_transaction_atomicity_e2e.py` |
| **Service 재실행 멱등성** | `test_collection_service_idempotent_after_commit` | `tests/integration/test_transaction_atomicity_e2e.py` |

compat/e2e 테스트는 mock session이 아니라 **실제 SQLite DB 결과**를 검사한다
(`sqlite://` + `StaticPool`으로 세션 간 DB 공유, `src.db.engine.SessionLocal`
패치로 실제 `get_db_session()` 경로를 실행).

## 6. P0.5 완료 기준

- [x] write-path inventory script 존재 (`scripts/audit_transaction_ownership.py`)
- [x] inventory script exit 0 (현재 트리)
- [x] 모든 repository lifecycle 예외가 명시적 allowlist에 있음 (현재 0건)
- [x] session=None 성공 경로 commit 테스트
- [x] session=None 실패 경로 rollback 테스트
- [x] 실제 Service → multi-repository → DB 성공 E2E
- [x] 동일 경로 중간 실패 시 전체 rollback E2E
- [ ] PostgreSQL transaction semantics integration (P0.6, 별도 진행)
- [x] 기존 repository tests 전체 통과
- [x] ruff 통과
- [x] 재현 불가능한 "94건 제거" 수치 삭제
- [x] "commit 0건" → strict-core/compatibility 예외 기준으로 정확히 표현

## 7. 이후 참고: 호출자 commit 누락 위험

Repository 내부 commit 제거 후 위험은 "caller가 commit을 잊는 것"으로
이동했다. 호출부는 `with get_db_session() as session:` (자동 commit) 또는
service 계층의 `sessionmaker.begin()`을 사용해야 한다. 새 write path를
추가할 때는 반드시 다음을 확인한다:

1. repository 호출이 session을 받는지 (자체 생성 금지)
2. 트랜잭션 범위가 service/caller에 있는지
3. 대량 작업은 transaction unit(경기/배치 단위)별로 commit되는지
   (한 거대 트랜잭션으로 만들지 않기)
