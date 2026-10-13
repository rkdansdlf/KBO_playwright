# Windows DB 호스트 복구 런북 (ch806-08)

> 이 문서는 **Windows 서버 앞(또는 사내망 PC)에서** 실행할 절차다.
> KBO_playwright/Mac 쪽에서는 이 서버로 들어갈 원격 채널(SSH·RDP·WinRM)이 모두 닫혀 있어 대신 실행할 수 없다.
> Mac 쪽 후속 절차는 §7에 있다.

## 1. 대상과 현재 증상

| 항목 | 값 |
|---|---|
| 호스트 | `ch806-08` (Windows) |
| 네트워크 주소 | Tailscale `100.81.73.13` |
| 역할 | KBO_playwright 운영 PostgreSQL (`DATABASE_URL=postgresql://<user>@100.81.73.13:5432`) — BEGA 백엔드와 공유 |
| 서버 앞 접근 | RDP(3389)·SSH(22)·WinRM(5985/5986) **무응답** → 사내망 PC의 RDP 또는 물리 접근 필요 |

**2026-10-05 Mac 관측 기준**

| 포트 | 상태 |
|---|---|
| 5432 (PostgreSQL) | **무응답 (SYN timeout, RST 아님)** |
| 3389 (RDP) / 22 (SSH) / 5985·5986 (WinRM) | 무응답 |
| 445 (SMB) / 135 (RPC) | **OPEN** |
| Tailscale ping | **정상 (29ms)** |

해석: 네트워크 계층(Tailscale)은 정상이고, **Windows 쪽 방화벽 DROP 또는 PostgreSQL 서비스 미기동**이다.
`RST`가 아니라 timeout이라는 점은 "서비스가 죽어서 거부"보다 **방화벽이 먼저 버리는 경우**와 일치한다
(둘 다 가능하므로 §3에서 서비스와 방화벽을 함께 확인한다).

## 2. 빠른 복구 카드 (관리자 PowerShell, 위에서부터)

```powershell
# 1) 서비스 상태
Get-Service *postgres* | Format-Table Name, Status, StartType -AutoSize

# 2) 내려가 있으면 시작 + 자동 시작 고정
Get-Service *postgres* | Start-Service
Get-Service *postgres* | Set-Service -StartupType Automatic

# 3) 5432 LISTEN 확인 (0.0.0.0/[::] 이면 바인딩 정상)
Get-NetTCPConnection -LocalPort 5432 -State Listen -ErrorAction SilentlyContinue |
    Select-Object LocalAddress, LocalPort, OwningProcess

# 4) Tailscale 인터페이스가 Public으로 분류됐으면 Private로 (사설 규칙이 다시 적용됨)
Get-NetConnectionProfile |
    Where-Object { $_.InterfaceAlias -like "*Tailscale*" } |
    Set-NetConnectionProfile -NetworkCategory Private

# 5) 그래도 원격에서 안 되면: Tailscale 대역만 허용하는 인바운드 규칙 추가
New-NetFirewallRule -DisplayName "PostgreSQL 5432 (Tailscale only)" `
    -Direction Inbound -Protocol TCP -LocalPort 5432 -Action Allow `
    -RemoteAddress 100.64.0.0/10
```

> 더 좁게 열려면 `-RemoteAddress 100.103.157.51` (KBO_playwright가 도는 Mac 한 대만).
> 다른 tailnet 기기가 이 DB를 쓰지 않는다면 이쪽이 최소 권한이다.

## 3. 진단 상세 (결과별 분기)

### 3-1. 서비스

```powershell
Get-Service *postgres* | Format-Table Name, Status, StartType, DisplayName -AutoSize
```

- `Running` → §3-2로
- `Stopped` → `Start-Service`; 실패하면 로그/디스크 확인
  ```powershell
  Get-ChildItem "C:\Program Files\PostgreSQL" -Recurse -Filter "postgresql*.log" -ErrorAction SilentlyContinue |
      Sort-Object LastWriteTime -Descending | Select-Object -First 3 FullName, LastWriteTime
  Get-Content "<위 경로>" -Tail 50
  Get-PSDrive C | Select-Object Used, Free
  ```
- 서비스 이름은 버전마다 다르다(`postgresql-x64-16` 등). 와일드카드 결과의 `Name`을 그대로 사용한다.

### 3-2. 바인딩·로컬 도달성

```powershell
netstat -ano | findstr :5432
Test-NetConnection -ComputerName 127.0.0.1 -Port 5432
Test-NetConnection -ComputerName 100.81.73.13 -Port 5432
```

| 결과 | 진단 |
|---|---|
| 출력 없음 | LISTEN 안 함 → §3-1(서비스) 또는 `listen_addresses` 문제(§3-5) |
| `0.0.0.0`/`[::]` LISTEN + 로컬 실패 | 서비스 기동 직후가 아니면 로그 확인 |
| `127.0.0.1`만 LISTEN | Tailscale IP에 바인딩 안 됨 → §3-5 |
| 로컬(127.0.0.1)은 성공, Tailscale IP 실패 | **방화벽이 원격만 차단** → §3-3 |

### 3-3. 방화벽 프로필

```powershell
Get-NetConnectionProfile | Format-Table InterfaceAlias, InterfaceDescription, NetworkCategory -AutoSize
Get-NetFirewallProfile      | Format-Table Name, Enabled, DefaultInboundAction -AutoSize
```

- **Tailscale 어댑터가 `Public`이면 그것이 원인**이다. Windows 방화벽은 프로필별로 규칙을 적용하므로,
  사설/도메인 전용 규칙이 Public 프로필에는 적용되지 않는다 → §2-4로 재분류.

### 3-4. 5432 인바운드 규칙

```powershell
Get-NetFirewallPortFilter | Where-Object LocalPort -eq 5432 |
    ForEach-Object { Get-NetFirewallRule -AssociatedNetFirewallPortFilter $_ } |
    Select-Object DisplayName, Enabled, Profile, Action, Direction | Format-Table -AutoSize
```

- 규칙이 없거나 `Profile`이 제한적이면 §2-5로 추가.

### 3-5. `listen_addresses` / `pg_hba.conf` (127.0.0.1만 LISTEN일 때)

```powershell
Get-ChildItem "C:\Program Files\PostgreSQL" -Recurse -Filter postgresql.conf -ErrorAction SilentlyContinue |
    Select-Object FullName
Select-String -Path "<postgresql.conf 경로>" -Pattern "^#?\s*listen_addresses"
```

```powershell
$conf = "<postgresql.conf 경로>"
Copy-Item $conf "$conf.bak-20261005"
(Get-Content $conf) -replace "^#?\s*listen_addresses\s*=.*", "listen_addresses = '*'" | Set-Content $conf

# 같은 data 디렉터리의 pg_hba.conf 끝에 Tailscale 대역 추가
Add-Content "<pg_hba.conf 경로>" "`nhost    all    all    100.64.0.0/10    scram-sha-256"

Get-Service *postgres* | Restart-Service
```

### 3-6. 이력·이벤트·구성 (원인 기록용)

```powershell
Get-HotFix | Sort-Object InstalledOn -Descending | Select-Object -First 5 HotFixID, InstalledOn
Get-EventLog -LogName Application -Source *postgre* -Newest 20
tailscale status
tailscale ip -4
tailscale version
```

## 4. 검증 (서버에서)

```powershell
Get-NetTCPConnection -LocalPort 5432 -State Listen | Select-Object LocalAddress, LocalPort
Test-NetConnection -ComputerName 100.81.73.13 -Port 5432   # TcpTestSucceeded : True 가 목표
```

## 5. 재발 방지

```powershell
# 자동 시작 확인
Get-Service *postgres* | Set-Service -StartupType Automatic
Get-Service *postgres*, Tailscale | Format-Table Name, Status, StartType

# 서비스 복구 옵션: 60초 후 자동 재시작 (서비스명은 실측값으로)
sc.exe qfailure "postgresql-x64-16"
sc.exe failure  "postgresql-x64-16" reset=86400 actions=restart/60000/restart/60000
```

체크리스트:
- [ ] 서비스 `Automatic` + 복구 옵션 등록
- [ ] 방화벽 규칙은 **Tailscale 대역(또는 Mac 단일 IP)으로 제한** — 전체 공개 금지
- [ ] Windows Update 직후에는 `Get-NetConnectionProfile`로 Tailscale 프로필 재확인
- [ ] Tailscale 자동 시작 확인 (`Get-Service Tailscale`)

## 6. 하지 말 것

- `-Profile Any -RemoteAddress Any`로 5432를 인터넷에 공개하지 않는다 (Tailscale 대역/Mac IP로 제한).
- 서비스 계정·비밀번호를 임의로 바꾸지 않는다 (BEGA 백엔드와 공유 중).
- PostgreSQL 재시작은 현재 연결을 끊는다(현재는 끊을 연결이 없다).

## 7. 복구 후 (Mac/KBO_playwright 쪽에서 실행)

```bash
# 1) 도달성
nc -z -G 3 100.81.73.13 5432

# 2) 마이그레이션 체인 확인 (read-only)
PGCONNECT_TIMEOUT=5 venv/bin/python -m src.cli.apply_postgres_migrations --check

# 3) 통과하면 Phase G 재개(G1 → G4~G7): DLQ 통계/스냅샷 검증/원장 census
```

- 스케줄러는 별도로 꺼져 있다(`~/Library/LaunchAgents/com.kbo-playwright.scheduler.plist.disabled`).
  **DB 복구 확인 후** 이 Mac에서 스크립트 하나로 재개한다:
  ```bash
  bash scripts/restore_scheduler_launchd.sh --dry-run   # 예정 동작 확인 (부작용 없음)
  bash scripts/restore_scheduler_launchd.sh             # 복구 + 검증 (원샷)
  ```
  - 스크립트는 repo 템플릿(`scripts/launchd/`)과 `scripts/install_scheduler_launchd.sh`를 재사용하고,
    `state = running`·`scheduler.py` 프로세스·`Registered job: crawl_daily_games` 로그까지 확인한 뒤에만
    `.disabled` 복사본을 정리한다. 이미 로드돼 있으면 아무 것도 하지 않는다(멱등).
  - DB가 아직 불통이면 경고만 출력하고 진행한다 — fail-fast 게이트가 DB 잡을 조용히 건너뛴다.
  > `com.kbo.daily_ingest`·`com.kbo.monthly_embed_upgrade`는 다른 프로젝트(KBO_platform/bega_AI) 소유이므로 손대지 않는다.
  > 복구 스크립트도 스케줄러 라벨만 건드린다.

## 8. 서버 접근자가 기록해 줄 것

- §2·§3 명령의 출력 원문(특히 `Get-Service`, `Get-NetConnectionProfile`, 5432 규칙 조회)
- 최근 Windows Update 5건과 Tailscale 버전
- 변경한 설정(파일 백업 경로 포함)
- 재발 시각과 그때의 방화벽 프로필

## 9. 복구 결정 기록 (2026-10-12 ~ 10-13)

Windows `kbo_postgres`(bega_prod) 장애 복구에서 내린 결정과 근거. 각 결정은 복구 과정에서 실제로 확인한 사실을 바탕으로 한다.

### 9.1 원본 보존 (가장 먼저 한 일)

- 손상된 볼륨 `kbo_playwright_postgres-data`를 호스트 사본 `D:\kbo_backup\volume_copy_20261013`로 보존했다.
- 검증: `global/pg_control` md5 `3fcacd03…`, 3,351개 파일, `pg_wal` 63개가 원본과 일치.
- 이 사본은 장애 직전 상태이므로 원인 분석과 롤백에 사용한다. 분석이 끝나기 전에는 삭제하지 않는다.

### 9.2 복구 방식: pg_resetwal (사본 볼륨에서만)

- 원인: `pg_control`이 `4/7AFE2D60` 체크포인트를 가리켰으나, 그 WAL 세그먼트가 없어 기동이 PANIC으로 실패했다.
- 읽을 수 있는 WAL은 `5/B6000000` ~ `5/B6F5E4B0`이었다. 이 구간에 체크포인트 `5/B6F0BF48`이 있었지만, 공식 도구로는 `pg_control`을 그 위치로 되돌릴 수 없었다.
- 선택: `pg_resetwal -f -x 40000`을 복구 사본 볼륨(`kbo_pg_recover`)에만 실행했다. 원본 볼륨에는 쓰지 않았다.
- `-x 40000`이 필요한 이유: `pg_control`의 NextXID가 `36811`로 남으면 WAL 마지막 XID `38752`보다 작아서, 재임베딩 행이 보이지 않는다(행 수 0처럼 보임). 실제 데이터 파일 손실은 아니었다.
- 교훈: `pg_resetwal`은 `-x`를 반드시 WAL 최대 XID보다 크게 지정하고, 기동 후 `txid_current()`와 행의 `xmin`을 대조한다.

### 9.3 손실 범위 (확인된 것과 미확인인 것)

- 확인됨: 버려진 WAL 구간은 `5/B6F5E4B0` 이후이며, 2026-10-12 19:00~19:05 UTC의 커밋 약 100건 규모다.
- 확인됨: WAL 분석으로 장애 손실에 의한 `game` 행 삭제는 0건이다.
- 미확인: 위 커밋 100건이 어떤 테이블의 어떤 행인지. 애플리케이션 로그로 확인이 필요하다.

### 9.4 백업

- 복구 직후 운영 DB를 `pg_dump -Fc`로 백업했다: `D:\kbo_backup\bega_prod_after_recovery_20261013.dump`, 약 1.65 GiB.
- sha256: `4398455c7e5763f917ff8b3c38672e4b459a895484efbc568b20e3e54d18c11e`
- 검증: `pg_restore -l` 정상, 목록 1,394개 항목.

### 9.5 `rag_chunk_terms` 미복원

- 결정: 옛 term 테이블을 복원하지 않는다(선택지 A).
- 근거: 옛 term 테이블의 chunk id 222,987개 중 약 78%(174,552개)가 재임베딩 `rag_chunks`(583,265행)에 없다. 존재하는 행만 남기면 4,287,429행 중 814,605행(약 19%)만 남는다.
- 사용처: 이 테이블은 Oracle 경로의 희소 검색에서만 쓰인다. PostgreSQL 경로는 `content_tsv` 전문 검색을 쓴다. 현재 운영은 `RAG_BACKEND=oracle`이라 서비스 영향은 없다.
- 후속: PostgreSQL 경로를 운영에 올릴 때 현재 corpus에서 재생성해야 한다.
- 참고: 결정 보조로 외부 판단 모델(OpenRouter 경유)에 요약을 보냈고 선택지 A(확률 0.76)가 나왔다. 최종 판단은 운영 책임자가 했다.

### 9.6 데이터 차이 (`game`)

- 덤프에 있고 운영에 없는 행: 7개(`game_id`: 19950620LTOB0, 19950621LTOB0, 19950620LGTP0, 19950620HTSL0, 19950621HTSL0, 19950620SSHH0, 19950621SSHH0). 모두 시즌 199500, 상태 `CANCELLED`. 10-10, 10-11, 10-12 덤프 세 개 모두 504행에 포함돼 있었고, 운영은 497행이다. 삭제 시점은 10-12 덤프 이후 복구 구간 안으로 보인다. 자식 행(라인업, 이닝, 타격)은 덤프에서도 0건이었다.
- 행위자: 미확인. 원장에 healer, cleanup, 1995 관련 실행 기록은 0건이고, `correction_audit_trail`도 0건이다. 장애 중 블라인드 구간이나 복구 중 직접 SQL 실행이 가능성으로 남아 있지만 근거는 없다.
- 운영에만 있는 행: 2개(`597889`, `597890`, 예정 경기). 덤프 이후 추가된 행이다.
- 덤프 대비 순감소: 27,393 → 27,388 (-7 + 2 = -5). 다른 행 삭제는 확인되지 않았다.
- 같은 구간의 전체 상태 변화: `CANCELLED` 9,547 → 6,933 (-2,614), `UNRESOLVED_MISSING` 241 → 2,848 (+2,607). 운영 값은 2026-10-13 조회로 확인했다. 기준 값은 장애 구간 이전 스냅샷이다(재조사 보고 기준). 변경 원인은 미확인이다. 앞서 추정했던 `auto_healer`는 실행 기록이 없어서 철회한다.

### 9.7 인덱스, 콜레이션, 검증

- HNSW 인덱스: 보류. PostgreSQL 경로를 활성화할 때 생성한다.
- 콜레이션: `template1`의 기록 버전을 2.41에서 2.36으로 갱신했다(2026-10-13). 운영 DB(`bega_prod` 등)는 기록과 실측이 모두 2.36이므로 REINDEX는 하지 않았다.
- 검증(2026-10-13): btree 인덱스 347개 `bt_index_check`(heapallindexed) 통과, 기본 테이블 96개 `verify_heapam` 손상 행 0건.
- 공개 API(`api.begabaseball.xyz`): 2026-10-13 조회 기준 `/api/stadiums`, `/api/leaderboard`, `/api/home/bootstrap`, `/api/games/past` 모두 200이다. 같은 날 다른 경로로 확인한 보고에서 401이 있었으나 URL이 달라 이 항목의 재현 대상에서 제외했다.

### 9.8 정리

- 제거함: `pg_scratch` 컨테이너와 `kbo_pg_scratch` 볼륨(검증용 복원 DB), `kbo_postgres_plain16_bak` 컨테이너.
- 제거 이유(후자): 중지된 상태였지만 운영 볼륨 `kbo_playwright_postgres-data`를 마운트하고 있어, 기동되면 같은 데이터에 두 번째 PostgreSQL이 붙을 수 있었다.
- 보류: `kbo_pg_recover` 볼륨은 롤백용으로 안정화 확인 뒤 제거한다.
- 보존: `D:\kbo_backup`의 모든 덤프, `volume_copy_20261013`.

### 9.9 미해결

- 장애 근본 원인: `pg_control`이 최신 체크포인트를 따라가지 않은 이유, Docker Desktop과 디스크 상태.
- `game` 7행 삭제의 행위자와, 같은 구간의 `CANCELLED` → `UNRESOLVED_MISSING` 2,607행 변경의 원인. 두 건 모두 실행 기록으로 확인해야 한다.
- "4,996행 감소": 저장소, 실행 로그, 데이터 파일(2026-10-01 이후 텍스트)에서 근거를 찾지 못했다. 측정 쿼리와 기준 시점이 제시되지 않아 재현하지 못했다. 덤프 대비 실측은 `game` 순감소 5행(27,393 → 27,388), `CANCELLED` 2,614행 감소다. 이 항목은 출처 없음으로 종결한다.
- 백엔드 쓰기 여부: VM 컨테이너 `ubuntu-backend-1`은 장애 구간에도 실행 중이었다(2026-10-09 11:30 UTC 시작 이후 재시작 없음). 컨테이너 로그는 2026-10-12 23:33 UTC 이후만 보존되어 장애 구간(19:00~19:06 UTC)의 쓰기 요청은 확인할 수 없다. 장애로 버려진 쓰기는 크롤러 소유의 야구 데이터(`game`, `stat_rankings` 등)이고, CLAUDE.md의 데이터 소유권 정의상 백엔드는 야구 데이터를 읽기만 한다. 따라서 백엔드 쓰기는 해당 없다고 판단한다. 이 판단은 정책 근거에 의존하며 로그로 입증되지는 않았다. 로컬 `kbo_api_server`는 DB 오류 없이 `/health` 200이다.
