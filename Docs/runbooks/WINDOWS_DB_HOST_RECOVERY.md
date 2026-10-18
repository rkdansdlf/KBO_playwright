# Windows DB 호스트 복구 런북 (ch806-08)

> 이 문서는 **Windows 서버 앞(또는 사내망 PC)에서** 실행할 절차다.
> 원격(Mac 등 다른 tailnet 기기)에서는 이 서버로 들어갈 원격 채널(SSH·RDP·WinRM)이 모두 닫혀 있어 대신 실행할 수 없다(2026-10 관측).
>
> **크롤러 스케줄러도 이 Windows 서버에서 돈다**(2026-10-18 운영자 확인). PostgreSQL만 Windows에 있고 스케줄러는 Mac(launchd)에서 돈다는 이전 설명은 더 이상 맞지 않다.
> 스케줄러 실행·재시작·로그 위치·작업 스케줄러 등록은 [`WINDOWS_SCHEDULER.md`](WINDOWS_SCHEDULER.md)를 본다.
> DB 복구 후 스케줄러 확인 절차는 §7에 있다. §9 이후는 Mac에서 관측·작업하던 시기의 기록이라 "Mac" 표현을 그대로 둔다.

## 1. 대상과 현재 증상

| 항목 | 값 |
|---|---|
| 호스트 | `ch806-08` (Windows) |
| 네트워크 주소 | Tailscale `100.81.73.13` |
| 역할 | KBO_playwright 운영 PostgreSQL (`DATABASE_URL=postgresql://<user>@100.81.73.13:5432`) — BEGA 백엔드와 공유 |
| 역할 (스케줄러) | 크롤러 스케줄러 `scripts/scheduler.py` — Windows 작업 스케줄러로 상주 ([`WINDOWS_SCHEDULER.md`](WINDOWS_SCHEDULER.md)) |
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

> 스케줄러는 같은 서버에서 붙으므로, 원격 허용이 필요한 것은 이 DB를 쓰는 다른 tailnet 기기뿐이다(BEGA 백엔드 등).
> 더 좁게 열려면 그 기기들의 IP만 `-RemoteAddress`에 넣는다. 이것이 최소 권한이다.
> (이전에는 스케줄러가 돌던 Mac `100.103.157.51` 한 대만 열었다. 지금은 그 Mac이 DB에 붙을 일이 없다면 넣지 않는다.)
> 스케줄러의 `DATABASE_URL`이 이 서버의 Tailscale 주소(`100.81.73.13`)를 쓴다면, 규칙을 좁힌 뒤 스케줄러 연결도 함께 확인한다(§7).

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
- [ ] 방화벽 규칙은 **Tailscale 대역(또는 이 DB를 쓰는 기기 IP)으로 제한** — 전체 공개 금지
- [ ] Windows Update 직후에는 `Get-NetConnectionProfile`로 Tailscale 프로필 재확인
- [ ] Tailscale 자동 시작 확인 (`Get-Service Tailscale`)
- [ ] 스케줄러 작업이 부팅 후 자동으로 뜨는지 확인 ([`WINDOWS_SCHEDULER.md`](WINDOWS_SCHEDULER.md) §4·§6)

## 6. 하지 말 것

- `-Profile Any -RemoteAddress Any`로 5432를 인터넷에 공개하지 않는다 (Tailscale 대역/필요한 기기 IP로 제한).
- 서비스 계정·비밀번호를 임의로 바꾸지 않는다 (BEGA 백엔드와 공유 중).
- PostgreSQL 재시작은 현재 연결을 끊는다. 같은 서버의 스케줄러 연결도 끊기므로, 가능하면 진행 중인 DB 잡이 없을 때 한다(`logs\scheduler.log` 끝을 먼저 본다).
- Mac의 launchd 스케줄러를 다시 켜지 않는다. Windows 스케줄러와 함께 돌면 같은 DB에 스케줄러 두 개가 쓴다.

## 7. 복구 후 (Windows 서버에서 실행)

```powershell
cd "<프로젝트 경로>"

# 1) 도달성 (스케줄러의 DATABASE_URL이 가리키는 주소로)
Test-NetConnection -ComputerName 100.81.73.13 -Port 5432

# 2) 마이그레이션 체인 확인 (read-only)
$env:PGCONNECT_TIMEOUT = "5"
& "<파이썬 경로>" -m src.cli.apply_postgres_migrations --check

# 3) 통과하면 Phase G 재개(G1 → G4~G7): DLQ 통계/스냅샷 검증/원장 census
```

- `<프로젝트 경로>`, `<파이썬 경로>`는 [`WINDOWS_SCHEDULER.md`](WINDOWS_SCHEDULER.md) 첫 표의 값이다.
- **스케줄러는 DB가 끊겨도 내려가지 않는다.** DB 잡마다 fail-fast 게이트가 먼저 DB를 확인하고, 안 되면 `Job ... skipped: database unreachable`를 남기고 건너뛴다. DB가 돌아오면 다음 실행부터 다시 돈다. 그래서 DB 복구만으로는 보통 재시작이 필요 없다.
- DB 복구 후 [`WINDOWS_SCHEDULER.md`](WINDOWS_SCHEDULER.md) §6으로 스케줄러를 확인한다.
  - 작업이 `Running`이고 프로세스가 하나인지
  - `logs\scheduler.log`에서 `skipped: database unreachable`가 멈추고 잡이 정상 완료되는지
- 서버 재부팅으로 스케줄러가 안 떠 있으면 `Start-ScheduledTask -TaskName "<작업 이름>"`으로 시작한다. 재시작 절차는 `WINDOWS_SCHEDULER.md` §5를 따른다.

### 7-1. 이전 방식: Mac launchd 스케줄러 재개 (사용하지 않음)

스케줄러가 Mac에서 돌던 시기의 절차다. **지금은 실행하지 않는다.** 실행하면 Windows 스케줄러와 동시에 돌게 된다. 기록용으로만 남긴다.

- 당시 스케줄러는 Mac에서 꺼 두었다(`~/Library/LaunchAgents/com.kbo-playwright.scheduler.plist.disabled`). DB 복구를 확인한 뒤 `bash scripts/restore_scheduler_launchd.sh --dry-run`으로 확인하고 `bash scripts/restore_scheduler_launchd.sh`로 재개했다.
- 그 스크립트는 repo 템플릿(`scripts/launchd/`)과 `scripts/install_scheduler_launchd.sh`를 재사용했다. `state = running`, `scheduler.py` 프로세스, `Registered job: crawl_daily_games` 로그를 확인한 뒤에만 `.disabled` 복사본을 정리했다.
- `com.kbo.daily_ingest`·`com.kbo.monthly_embed_upgrade`는 다른 프로젝트(KBO_platform/bega_AI) 소유라서 손대지 않았다.

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

### 9.9 재조사 결론 (2026-10-14)

`game` 상태 변경과 7행 삭제는 코드와 운영 DB를 대조해 원인을 좁혔다. **상태 변경의 작성자는 여전히 호출 로그로 확정되지 않았고**, 아래는 원인이 될 수 있는 경로와 그것이 왜 반복되면 안 되는지에 대한 기록이다.

#### 상태 변경은 "실패한 수집이 결과를 덮어쓴" 것이었다

`updated_at`으로 실행 시각을 복원했다(운영 DB 타임존 `Etc/UTC` 확인, KST 오독 배제). 두 번의 스윕이 날짜순으로 진행된 것이 드러난다.

| 실행 시각 (UTC) | 처리 범위 | 행 수 | 결과 상태 |
|---|---|---|---|
| 2026-09-23 07:52~07:54 | 1982-03-27 → 1982-10-14 | 241 | `CANCELLED` |
| 2026-09-23 08:17~08:26 | 1995-06-24 → 2000-10-13 | 2,848 | `UNRESOLVED_MISSING` |
| 2026-10-12 18:12 → 19:02 | 1982-03-27 → 1995-06-23 | — | `CANCELLED` |

현재 상태는 `game_date` 1995-06-23/24을 경계로 나뉜다. 10-12 실행은 19:02:40(1995-06-23)에서 멈췄고, 그 다음 날짜를 처리하기 전에 장애가 났다.

원인은 `_apply_detail_failure_fallback`(`src/cli/pipelines/run_daily_update.py`)의 보호 집합이 `CANCELLED`/`POSTPONED`뿐이었다는 점이다. 08-26 `apply_season_to_db`가 1983~2000을 `COMPLETED`로 적어놨고, detail 수집 실패 시 과거 날짜는 `UNRESOLVED_MISSING`으로 떨어지는데 `COMPLETED`는 보호 대상이 아니었다. **경기를 취소한 것이 아니라, 이미 끝난 경기에서 "몸체가 없다"는 사실만으로 결과를 버렸다.** `COMPLETED`와 `DRAW`를 보호 집합에 넣었다.

수렴하지 않았던 이유도 여기에 있다. `_find_detail_gaps`는 `COMPLETED`/`DRAW` 중 batting·pitching이 없는 날짜를 백필 대상으로 잡는다. 1983~2000은 본문이 한 건도 없어(측정: 이닝·라인업·타구·투구 전부 0행) 전부 대상이 되었고, 각 실행이 상태를 덮어써 다음 스캔에서 빠졌다. **데이터를 못 모아서 회수가 아니라 상태를 지워서 "진행"한 형태**다.

#### 7행 삭제: 고아 정리가 아니다

`game_metadata` 7행이 **전부 살아 있었다.** 부모를 `ON DELETE CASCADE`로 지웠다면 자식은 따라 사라진다. 따라서 삭제는 `game`만 겨냥했다.

`game`을 삭제하는 코드는 저장소에 두 곳뿐이다 — `scripts/historical/apply_season_to_db.py`(연도 전체 삭제 후 재삽입)와 `src/cli/sync/sync_sqlite_to_postgres.py`(테이블 전체 truncate). `src/maintenance/orchestrator.py`의 orphan 정리는 자식행만 지우고 `game`을 건드리지 않는다. 어느 것도 이틀치 7행을 지우지 않고, 해당 구간 실행 기록도 없다.

두 날짜 전체(1995-06-20 4경기, 06-21 3경기)가 통째로 사라졌고, answer set 504경기와 19개 시즌 전수 대조에서 **1995년 7건만이 유일한 불일치**였다. `scripts/maintenance/restore_deleted_game_rows`로 복원했다(`CANCELLED`, 10-12 덤프 기록값). 재실행 시 `missing=0`으로 멱등하다.

#### 남는 불확실성

- **"2,848행은 원래 `CANCELLED`였다"는 전제가 아직 서 있지 않다.** `data/archive/status_1983_2000_backup_20261012_202403.json`(10-12 20:24 KST)은 1983~2000 전부를 `CANCELLED`로 기록하지만, 운영 DB의 같은 행들은 `updated_at`이 09-23이고 값이 `UNRESOLVED_MISSING`다. 둘이 동시에 참일 수 없다. 덤프를 별도 DB로 복원해 대조해야 기준값이 정해진다.
- 09-23 08:17~08:26 구간의 주체는 확정되지 않았다. 초당 약 5행의 속도는 날짜별 CLI 호출이 아니라 단일 프로세스가 행을 순회한 형태인데, 해당 경로가 아직 특정되지 않았다.
- 장애 근본 원인: `pg_control`이 최신 체크포인트를 따라가지 않은 이유, Docker Desktop과 디스크 상태.

### 9.10 재발 방지 (2026-10-14)

- `src/config/historical_backfill_policy.py` + `Docs/config/historical_backfill_dates.yaml`: 과거 시즌의 상태 쓰기는 커밋된 승인 목록에 없으면 거부된다. 목록 밖 날짜가 필요하면 설정 파일을 고쳐 커밋하는 것이 승인 행위이며, 플래그로 우회할 수 없다.
- 거부해도 실행은 죽지 않는다. 전이는 계산·보고만 하고 쓰지 않는다 — 과거 날짜에서 review/story 배치가 함께 실패하지 않도록.
- `refresh_game_status_for_date`는 실제로 쓴 전이마다 `correction_audit_trail`에 1행을 남기며, 근거 5플래그와 경과일수, 호출자 이름을 함께 기록한다. 매니페스트에는 상태 전이 집계가 들어간다(전 2,848건 스윕은 매니페스트가 정상 실행처럼 보였다).
- `_find_detail_gaps`/`_find_pbp_gaps`는 `SOURCE_LIMITED_SEASON_FLOOR`(2001) 이전을 제외한다.

### 9.11 DB 접속 불가 재발 (2026-10-14 16:2x UTC 관측)

`game` 7행 복원을 마친 뒤 다음 작업(D7 RAG 정체성 백필 드라이런)을 시도하다 연결이 서지 않았다.

| 확인 항목 | 결과 |
|---|---|
| TCP 5432 | **연결 성공** |
| ICMP `ping 100.81.73.13` | 2/2 응답, 평균 11.5ms |
| psycopg2 연결 | **`server closed the connection unexpectedly`** (3회 연속 재현) |
| 서버 서비스 상태 | **관측 불가** — §1대로 Mac에서 RDP·SSH·WinRM이 모두 닫혀 있다 |

TCP가 열려 있으면서도 시작 핸드셰이크 직후 서버가 연결을 끊는 것은 §1 표의 증상과 같은 계열이다. 네트워크가 아니라 **PostgreSQL 서비스 상태** 문제로 보고 §2의 빠른 복구 카드를 따른다.

이 장애가 막은 것:

- D7 `backfill_rag_index_identity` 드라이런(58만 행 대상) — **읽기 전용이므로 손실 없음**
- 1982 병합 적용(`merge_1982_duplicate_games --apply`) — 드라이런은 이미 확인됨(241쌍, 필드 641개 이동, 불일치 0)
- §9.9의 덤프 재조회와 D3·D6 — 원래부터 운영자 조치 대기 상태였다

### 9.12 남은 작업과 그 대기 사유 (2026-10-15 KST 기준)

DB가 복구되면 순서대로 진행한다. **아무 것도 자동 복구하지 않는다.**

| # | 작업 | 대기 사유 | 선행 조건 |
|---|---|---|---|
| 1 | D3 — 2,848행 격리 표시 | 기준값 미확정 | 10-12 덤프를 별도 DB로 복원해 `1983-2000` 상태 대조(§9.9의 모순 해소) |
| 2 | D6 — `stat_rankings` 재생성 | 차분 시즌 미확정 | 덤프 기준 `SELECT season, COUNT(*) ... GROUP BY 1` 확보 후 **차분 시즌만** 재생성 |
| 3 | D7 — `index_version`/`indexed_at` 백필 | 호스트 환경변수 미확인 | Windows의 `RAG_INDEX_VERSION` 확인 (없으면 `rag-v1`, `indexed_at=2026-10-12`) |
| 4 | D4 — 1982 병합 적용 | 게임 ID 규칙 미확정 | `apply_season_to_db.py:31-38`의 MBC→MB 축약이 중복 원인이라는 주석이 미검증 — ID 생성 규칙을 먼저 확정 |
| 5 | 상태 변경의 행위자 규명 | 로그 부재 | `game` 7행 삭제 주체, 09-23 08:17~08:26 실행 주체 |

각 항목의 명령과 가드는 저장소에 준비되어 있다.

```bash
# 3. RAG 정체성 백필 (읽기 전용 드라이런 먼저)
venv/bin/python -m src.cli.backfill.backfill_rag_index_identity \
  --skip-vector --json --index-version rag-v1 --indexed-at 2026-10-12
KBO_ALLOW_HISTORICAL_RESTORE=1 venv/bin/python -m scripts.maintenance.restore_deleted_game_rows --year 1995 --apply

# 4. 1982 병합 (검증 후 삭제, 스냅샷 자동 기록)
KBO_ALLOW_1982_DEDUP=1 venv/bin/python -m scripts.maintenance.merge_1982_duplicate_games --apply
```


Mac 로컬 스케줄러는 `com.kbo-playwright.scheduler.plist.disabled` 등 3개 모두 비활성이므로, 이 Mac에서 죽은 DB를 향해 재시도하는 프로세스는 없다.
(2026-10-15 당시 기록이다. 지금 스케줄러는 Windows 서버에서 돈다 — [`WINDOWS_SCHEDULER.md`](WINDOWS_SCHEDULER.md).)


- "4,996행 감소": 저장소, 실행 로그, 데이터 파일(2026-10-01 이후 텍스트)에서 근거를 찾지 못했다. 측정 쿼리와 기준 시점이 제시되지 않아 재현하지 못했다. 덤프 대비 실측은 `game` 순감소 5행(27,393 → 27,388), `CANCELLED` 2,614행 감소다. 이 항목은 출처 없음으로 종결한다.
- 백엔드 쓰기 여부: VM 컨테이너 `ubuntu-backend-1`은 장애 구간에도 실행 중이었다(2026-10-09 11:30 UTC 시작 이후 재시작 없음). 컨테이너 로그는 2026-10-12 23:33 UTC 이후만 보존되어 장애 구간(19:00~19:06 UTC)의 쓰기 요청은 확인할 수 없다. 장애로 버려진 쓰기는 크롤러 소유의 야구 데이터(`game`, `stat_rankings` 등)이고, CLAUDE.md의 데이터 소유권 정의상 백엔드는 야구 데이터를 읽기만 한다. 따라서 백엔드 쓰기는 해당 없다고 판단한다. 이 판단은 정책 근거에 의존하며 로그로 입증되지는 않았다. 로컬 `kbo_api_server`는 DB 오류 없이 `/health` 200이다.

---

## 10. 1982 중복 재발 구조 (2026-10-16 규명)

### 10.1 결론: 두 가지 game_id 규약이 동시에 살아 있었다

1982 시즌 241경기가 **두 개의 `game_id` 로 각각 존재**했다. 날짜·홈팀·원정팀·점수·승자가 모두 같고 `game_id` 만 달랐다.

| 형태 | 예시 | 규약 | 근거 파일 |
| --- | --- | --- | --- |
| 정본 | `19820327MBSS0` | **홈 먼저** (`game_id[8:12] == home+away`, 241/241 일치) | `data/archives/{year}_answer_set_final.json` |
| 파생 | `19820327SSMB0` | **원정 먼저** | `scripts/historical/apply_season_to_db.py::build_standard_game_id` |

`build_standard_game_id(date, home_team, away_team)` 는 `{date}{away}{home}{dh}` 다. answer set 이 저장하는 `game_id` 필드는 홈-먼저라, **ID를 다시 계산하는 도구는 정상 행을 "누락"으로 판단하고 반대 방향의 행을 새로 만든다.** 로컬 SQLite(`data/kbo_dev.db`)는 파생 형태만 241행 갖고 있어 두 규약이 갈라져 있다는 사실 자체를 눈으로 볼 수 있다.

D4 병합(`scripts/maintenance/merge_1982_duplicate_games.py`)은 정본 쪽을 primary 로 남기고 파생 241행을 지웠다.

### 10.2 지워진 241행이 되살아온 경위

병합 직후 1982는 241행이었으나 약 7시간 뒤 다시 482행이 되었다. **병합이 되돌린 것이 아니다** — 근거는 다음과 같다.

- `merge_season()` 은 `flush -> verify -> delete -> commit` 단일 트랜잭션(`:313~:326`)이다. 롤백이었다면 `primary` 의 병합값도 함께 사라져야 하는데 `winning_score` 240/241과 `updated_at=2026-10-15 23:56:22`(병합 시각)가 남아 있다.
- `pg_stat_user_tables` 누적치(`stats_reset = NULL`): `game` 은 `ins=241 / upd=5,629 / del=241 / live=27,907`. 삽입 241건과 삭제 241건이 **정확히 1회씩**이고, upd 5,629 = 병합 UPDATE 241 + 상태 정정 UPDATE 5,388.
- 되살아난 241행은 `xmin = 47316` 단일 트랜잭션이며 그 트랜잭션은 **`game` 테이블에만** 241행을 썼다. 다중 테이블 동기화가 아니다.
- 삽입 행은 `created_at=2026-08-26 13:09:38`(과거값), `updated_at=2026-09-23 07:52:46.007817`(마이크로초까지 보존), `is_primary=FALSE`, `game_status=CANCELLED` 를 그대로 갖고 있었다. 덤프나 백업에서 복원된 값이며, ORM 기본값(now())으로는 나올 수 없다.

`pg_stat` 기준으로 이 구간 이후 재삽입은 없다(스케줄러 트랜잭션은 `notification_deliveries` 로 별도 확인). **반복 작업이 아니라 1회성 삽입**이며, 실행 주체는 이 시점부터 미확정이다. 서버 로그는 호스트에 있어 이 Mac에서 확인할 수 없다(§5).

`track_commit_timestamp` 가 꺼져 있어 `xmin` 으로는 커밋 시각을 직접 얻을 수 없고 상대 순서만 판정된다. 다음 occurrence 에 대비해 켜둘 것을 권고한다.

### 10.3 회수와 재발 방지

1. **회수**: `is_primary IS FALSE` 인 241행을 삭제했다. 자식 참조는 21개 테이블 중 `correction_audit_trail` 241행(소프트 참조)뿐이고 본문·메타데이터는 0행이었다. 되돌림은 `data/archive/1982_dup_retire_rollback_20261016_203051.sql`(22컬럼 전체 INSERT)로 검증했다 — 실행하면 27,666 -> 27,907, `is_primary=FALSE` 241행으로 정확히 복원됨을 트랜잭션 롤백으로 실증했다.
2. **게이트**: `game_duplicate_identifiers` 추가. 키는 `(game_date, home_team, away_team, game_id 마지막 문자)` 다. 마지막 문자를 넣는 이유는 실제 더블헤더가 1,000개나 있기 때문이다 — 같은 날짜·홈·원정을 공유하는 1,000개 그룹은 전부 정당한 더블헤더(`20100518LTHT0` / `20100518LTHT1`)라 날짜와 팀만 키로 쓰면 1,000건이 오탐이 된다. 단위는 **쌍**이라 개수가 기준선과 비교 가능하다.
3. **미해결 잔여분**: 게이트가 22그룹을 보고한다 — 2010년 16쌍(같은 일정이 `season_id` 169과 170으로 갈라져 점수·본문 모두 없음), 2014년 6쌍(2014 한국시리즈가 본문 있는 `NXSS0` 과 본문 없는 `WO*` 로 중복). **2014년 6쌍은 §11 의 상태 정정 대상에 포함되어 `UNRESOLVED_MISSING` 으로 바뀌었으나, 본문이 형제 ID 에 있으므로 정상 분류가 아니라 삭제 대상이다.**

## 11. `CANCELLED` + 점수 행의 분류 (2026-10-16)

`CANCELLED` 이면서 양쪽 점수를 갖는 행은 "경기가 열리지 않았다"는 뜻이 될 수 없다. 점수가 있다는 것은 결과가 있다는 뜻이다. 이 행들은 전부 **본문이 없다**는 공통점이 있었다.

| 대상 | 행수 | 처리 |
| --- | --- | --- |
| pre-2001 | 5,388 | `UNRESOLVED_MISSING`. 되돌림 `data/archive/pre2001_cancelled_rollback_20261016_161453.sql` |
| 2001+ | 50 | `UNRESOLVED_MISSING`. 되돌림 `data/archive/modern_cancelled_rollback_20261016_202937.sql` |

2001+ 50건의 정체: 2011·2012·2026 올스타전, 2011·2014·2018 한국시리즈, 2024 WBC(12개국). `xmin` 이 2166~2168이고 `updated_at` 이 2026-08-15 18:02이므로 **09-23/10-12 스윕이 덮은 것이 아니라 2026-08-15 대량 적재 때부터 `CANCELLED` 로 들어온 행**이다. 48건에 대해 "스윕이 덮었다"는 이전 판단은 철회한다.

**게이트 결과**: `game_cancelled_with_scores` 50 -> 0. 게이트가 조용해진 것은 정보를 잃은 것이 아니라 분류가 일관된 때문이다 — 정보성 게이트 `game_result_without_body` 가 8,288을 계속 보고한다.

### 11.1 상태 분리의 근거

`game_status` 는 "경기 결과"와 "본문 존재"를 겸용하고 있었다. 분리했다.

- **`game_cancelled_with_scores` (fail)** — 소스 제한이 아니라 우리 쪽 오류다. 어느 세대든 점수를 가진 행은 결과이므로 취소될 수 없다. 연대 조건을 넣지 않는 이유: 이 검사가 존재하게 만든 실패(7,995행)는 전부 2001년 이전에 발생했고, 연대로 한정하면 같은 스윕이 재발해도 조용히 통과한다. `is_primary` 로 한정하는 것은 은퇴한 중복 행을 보고하지 않기 위함이며, 그 행도 실제 점수를 갖는다.
- **`game_result_without_body` (warn)** — 나무위키가 1982~2000 결과를만 제공하고, 2011~2026 의 특수 경기 50건은 박스스코어 자체를 수집한 적이 없다. 소스 제한이라 실패가 아니다. 네 테이블(`player_game_batting`/`game_lineups`/`game_inning_scores`/`game_batting_stats`) 중 하나라도 있으면 본문이 있는 것으로 본다 — 라인업만 있는 경기를 "본문 없음"으로 보고하면 없는 공백이 보고된다. **연대 조건을 넣지 않는다**: 그랬다면 50건은 `UNRESOLVED_MISSING` 로 바뀐 순간 보고에서 빠지고, 게이트는 그것이 지켜야 할 행을 잃는다.

## 12. 2026-10-16 작업 중 발견한 별개 문제 (미해결)

### 12.1 두 규칙이 서로 모순된다

`src/repositories/game_status.py` 에 두 개입이 동시에 있고 순서가 문제다.

| 규칙 | 위치 | 내용 |
| --- | --- | --- |
| 하드 바닥 | `game_status.py` pre-2001 가드 | `dt.year < 2001` 이면 **무조건 거부** |
| 정책 승인 | `historical_backfill_policy.py::status_write_allowed` | `approved_dates` 에 있으면 **허용** |

하드 바닥이 정책 검사보다 먼저 실행되므로 `approved_dates={"19950624"}` 로 1995년 날짜를 승인해도 정책까지 도달하지 못한다. `Docs/config/historical_backfill_dates.yaml` 의 승인 기반 쓰기 정책과 정면으로 부딪힌다. 어느 쪽이 우선인지는 issue #32 작업의 결정 사항이며 **이 문서 작성 시점까지 미해결**이다.

### 12.2 Oracle 벡터 테스트 10건

`AGENTS.md` 는 이제 Oracle 다이얼렉트를 `OracleEmbeddingColumnUnsupportedError` 로 **거부**한다고 명시한다. `tests/sync/test_oracle_writer_vector.py` 10건은 Oracle 네이티브 VECTOR 바인딩이 동작한다고 가정하므로 거절된 계약과 어긋난다. 테스트가 옛 계약을 유지하고 있다.

### 12.3 작업 트리에 병합 충돌이 남아 있었다

`src/repositories/game_status.py` 에 `<<<<<<<` / `>>>>>>>` 3곳과 `UU`(unmerged) 인덱스 상태가 있었고, 진행 중인 병합도 스태시도 아니었다(`.git/MERGE_HEAD`·`git stash list` 모두 없음). **10개 테스트가 import 실패로 죽어 있었다.**

해결 근거:

- import 충돌은 **양쪽 다 필요하다** — 본문이 `AUTO_HEAL_HISTORY_FLOOR_YEAR`(126·359행)와 `HistoricalBackfillPolicy`(195행)·`load_historical_backfill_policy`(406행)를 모두 쓴다.
- 코드 충돌 2곳은 **stashed 쪽이 유일하게 성립**한다 — 공통 꼬리가 `allowed`·`skip_reason`·`write`·`transitions`를 쓰는데 이 변수들을 정의하는 쪽이 stashed 뿐이다. `_EvaluationContext`·`_collect_transitions`·`_record_transition_audits` 정의도 존재한다.
- pre-2001 거부 경로가 **4개 키만** 반환하고 있었다(`_empty_status_result` 의 12개 키와 불일치). `skip_reason` 인자를 추가해 세 거부 경로의 형태를 통일했다.

## 13. 히어로즈 경기 중복 1,161쌍 (2026-10-17 규명·정리 완료)

### 13.1 게이트가 22쌍만 보고한 이유

`game_duplicate_identifiers` 는 팀 **컬럼**(`home_team`/`away_team`)으로 그룹핑했다. 같은 경기인데 컬럼 표기가 다르면 서로 다른 그룹이 된다.

```
20100310DBNX0   home=KH(키움)   ← 게이트가 못 봄
20100310OBNX0   home=NX(넥센)   ← KH ≠ NX 라서 다른 그룹
```

`NX`(넥센 2010~2018)·`WO`(우리 2008~2009)·`KH`(키움 2019~)는 **모두 프랜차이즈 11** 이다. 정본 매핑(`src/utils/team_history.py`)으로 코드를 프랜차이즈로 접으면 실제 중복은 **1,161쌍**이었다 — 게이트가 보던 22쌍의 53분의 1.

| 연도 | 중복 쌍 |
| --- | --- |
| 2010 | 32 |
| 2011~2018 | 141 · 140 · 137 · 145 · 140 · 143 · 139 · 144 |

다른 세대(1982~2009, 2019~2026)는 0.

### 13.2 저장된 `franchise_id` 컬럼을 쓰지 않은 이유

`home_franchise_id`/`away_franchise_id` 는 이 검사 기준으로 쓸 수 없다. **NULL 165행**(1982년 81 · 2024년 38 · 2010년 21 …)이고, 오배정도 있다 — `20100310KIAHH0` 의 `away_franchise_id=11` 은 히어로즈 프랜차이즈인데 그 행의 `away_team` 은 `KIA` 다. 로더가 실제로 적은 값은 **코드**이므로 정규화 대상도 코드다.

### 13.3 생존 행 규칙

세 무리가 형태가 달라도 규칙은 하나다: **본문 행이 가장 많은 쪽을 남기고**, 같으면 점수를 가진 쪽, 그마저 같으면 먼저 적재된 쪽(`xmin`·`created_at`).

| 무리 | 쌍 | 양쪽 상태 | 결정 근거 |
| --- | --- | --- | --- |
| 2011~2018 히어로즈 | 1,121 | 한쪽에 본문·점수, 다른 쪽엔 `game_metadata` 1행뿐 | 본문 보유 |
| 2010 코드 차이 | 32 | 양쪽 다 점수·본문 없음 | 선적재 = 시대 정본 코드(`KIA`/`DB`, 레거시는 `HT`/`OB`) |
| 2014 한국시리즈 + 2018 | 8 | **양쪽 다 본문** | 큰 쪽이 진부분집합(예: 561행 대 478행) |

2010 의 32쌍은 두 신호(선적재 순서 · 시대 코드)가 **32/32 일치**하므로 순서 하나에 기대지 않는다. 규칙은 1,161쌍 전부에서 무승부 없이 결정된다.

`EA`/`WE`(올스타전 양쪽)는 프랜차이즈에 대응하지 않아 각자 다른 키를 유지한다. 같은 키로 접으면 올스타전 경기가 그날 클럽 경기와 충돌한다.

### 13.4 실행 결과와 되돌림

- `game` **27,666 → 26,505** (1,161행 삭제), 자식 5,561행 동반 삭제.
- 본문 테이블 행 수는 보존됨(오빔 없음), **고아 행 0**, 게이트 **0**.
- 되돌림: `data/archive/duplicate_games_rollback_20261017_005004.sql` (자식 먼저 → 부모 순, 22컬럼 전체 INSERT). 스냅샷: 같은 시각 `_snapshot_*.json`.

정리 도구: `scripts/maintenance/dedupe_duplicate_games.py` (드라이런 기본, `--apply` + `KBO_ALLOW_DUPLICATE_RETIRE=1` 이중 가드).

### 13.5 되돌릴 때의 주의

본문이 양쪽에 있던 8쌍은 지운 쪽이 **진부분집합**이었다. 본문이 없는 쪽(1,121쌍 + 32쌍)은 자식도 `game_metadata` 뿐이라 되돌리면 중복이 그대로 돌아온다. 되돌릴 이유가 생기면 `game_id` 로 어느 행을 되살릴지 먼저 정할 것 — 스냅샷에 양쪽 좌표가 모두 있다.

## 14. 전 컨테이너 PostgreSQL 무응답 (2026-10-17 관측)

§13의 중복 정리를 마친 뒤(되돌림 스냅샷 00:50) 다음 조회에서 **세 컨테이너가 동시에**
응답하지 않았다.

| 확인 항목 | 결과 |
| --- | --- |
| ICMP `ping 100.81.73.13` | 3/3 응답, 평균 9.6ms |
| Tailscale | `active; direct`, tx/rx 계속 증가 |
| TCP 5432 / 5435 / 55433 | **모두 연결 성공**(SYN 응답) |
| psycopg2 (5432, `connect_timeout=10` × 3) | **모두 `timeout expired`** |
| psycopg2 (5435, 55433) | **역시 timeout** |

### 14.1 §9.11과 무엇이 다른가

§9.11은 TCP 성공 + `server closed the connection unexpectedly`(서버가 즉시 끊음)였다.
지금은 TCP 성공 + **응답 없음**이다. TCP가 열린 채 PG가 침묵하는 형태는
**docker-proxy가 포워딩을 유지하는데 백엔드 컨테이너가 서 있지 않은** 경우와 일치한다.

결정적 차이는 **55433까지 timeout**이라는 점이다. 10-16에는 55433이
`password authentication failed`였으므로 그때는 **서버가 살아 있었다**. 지금은 별개
프로젝트 컨테이너까지 침묵하므로, 개별 서비스 정지보다 **호스트 전역**(Docker 데몬 또는
재부팅·절전) 가능성이 높다.

### 14.2 선행 작업과의 인과 가능성 (미확정)

§13에서 `game` 1,161행 + 자식 5,561행을 삭제했다. 대량 DELETE 뒤 autovacuum이 돌면
I/O가 포화되어 응답이 매달릴 수 있다. 다만 그것만으로 **별개 프로젝트 컨테이너까지**
멈추는 것은 설명되지 않으므로 인과는 **미확정**으로 둔다.

### 14.3 이 장애가 막은 것

- D7 `backfill_rag_index_identity` (§9.12 3번)
- 1982 병합 적용(§10)
- 폐기 인덱스의 커버리지 갭 조회 — `build_rag_index` 대상 17종에 게임별 기록
  (`game_batting_stats`·`game_inning_scores`·`game_pitching_stats`·`game_summary`·
  `game_metadata`·`game_flow_summary`·`kbo_seasons`)이 **없음**을 확인했고, 이들을
  대상에 추가할지 판단하려면 도메인 표본 조회가 필요하다

전부 읽기이거나 승인 대기였으므로 **손실은 없다**.

### 14.4 조치

§1대로 Mac에서 RDP·SSH·WinRM이 닫혀 있어 서버 앞에서만 가능하다. §2의 빠른 복구 카드를
따르되 **세 컨테이너가 함께 죽었으므로 Docker 데몬 상태부터** 본다.
