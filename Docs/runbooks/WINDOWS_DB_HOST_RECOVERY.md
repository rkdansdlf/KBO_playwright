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
