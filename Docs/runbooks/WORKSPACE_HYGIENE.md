# Workspace Hygiene Rules

작업 산출물이 저장소 밖(상위 폴더)으로 새는 것을 막기 위한 경로 계약과 worktree 규칙.
2026-08-23 대정리(`data/archive/workspace_cleanup_20260823/MANIFEST.md`)의 재발 방지 계획.

## 경로 계약 (Path Contract)

모든 크롤링·분석·검증 산출물은 **반드시 저장소 내부** 아래 경로에 생성한다.

| 산출물 | 저장 위치 | 비고 |
| --- | --- | --- |
| 런타임 로그 | `logs/` | gitignore 적용. 대형 로그는 주기 아카이브 |
| 원본 스냅샷 / 대용량 덤프 | `data/archive/<작업명>_<날짜>/` | gitignore 적용. NDJSON 등은 tar.gz 압축 후 원본 삭제 |
| 디버그 스크린샷/HTML | `debug_shots/`, 루트 `*_debug.*` | 조사 스크립트가 자동 재생성 |
| 임시 조사 스크립트 | `scratch/`, `scripts/investigations/` | `scripts/investigations/`는 ruff 스코프 제외 |
| 리포트 생성물 | `reports/` | gitignore 적용 (`/reports/`) |
| DB / 검증용 파일 | `data/` 하위 | 루트에 `*.db` 생성 금지 |

**금지**: 저장소 밖(`/Users/mac/project/` 등 상위 폴더)에 patch, dump, manifest, blob_hash, ndjson 파일 생성.
도구가 상대경로(`..`)로 쓰는 경우 저장소 내부 절대경로로 지정할 것.

## Git Worktree 규칙

1. 생성: `git worktree add ../<이름> <branch>` — 이름은 목적을 알 수 있게 (`kbo-rag-recovery` 참고).
2. 작업 종료 시:
   - 미반영 변경이 있으면 먼저 패치 백업: `git -C <worktree> diff HEAD > data/archive/<폴더>/<이름>.patch`
   - `git worktree remove --force <path>` → `git worktree prune`
3. worktree를 `rm -rf`로만 지우면 `.git/worktrees/`에 유령 메타데이터가 남는다 — 반드시 `remove` 사용.
4. 병합 완료된 작업 브랜치는 정리: `git branch -d <branch>`.

## 로그 관리

- `logs/scheduler.launchd.err.log`는 운영 로그로 유지 (lock-health가 말미 8MB만 스캔).
- 16MB 초과 시 트림 (말미 보존, 헤드는 gzip 아카이브):
  ```bash
  python3 -m scripts.maintenance.trim_scheduler_log --file logs/scheduler.launchd.err.log --keep 16M
  ```
  launchd가 fd를 유지하므로 파일 교체 없이 in-place 절단한다. 아카이브 위치: `data/archive/logs/`.
- 세대가 끝난 마이그레이션/실험 로그(예: Oracle 동기화 시대)는 `data/archive/`로 이관.

## 월간 점검 체크리스트

- [ ] `du -sh /Users/mac/project/*` — 저장소 밖 잔재 없는지
- [ ] `git worktree list` — 불필요한 worktree 없는지
- [ ] `du -sh logs/ data/` — 급증 여부
- [ ] `python3 -m scripts.maintenance.trim_scheduler_log --file logs/scheduler.launchd.err.log --keep 16M --dry-run` — 16MB 초과 시 실제 트림 실행
- [ ] `ls data/archive/` — 오래된 작업 폴더 압축/정리
