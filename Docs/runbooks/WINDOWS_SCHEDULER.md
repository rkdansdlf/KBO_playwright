# Windows 서버 스케줄러 운영 런북

> 크롤러 스케줄러(`scripts/scheduler.py`)는 **Windows 서버**에서 돌린다. PostgreSQL이 있는 서버와 같은 서버다(2026-10-18 운영자 확인).
> Mac(launchd)에서 돌리던 내용은 맨 아래 [§9 이전 방식](#9-이전-방식-mac--launchd-더-이상-사용하지-않음)에 따로 모아 두었다.
> DB(PostgreSQL) 자체의 장애 복구는 [`WINDOWS_DB_HOST_RECOVERY.md`](WINDOWS_DB_HOST_RECOVERY.md)를 본다.

`<...>`로 적은 값은 저장소에 없는 서버별 값이다. 실제 서버에서 확인해 채운다.

| 자리표시자 | 뜻 | 확인 방법 |
|---|---|---|
| `<프로젝트 경로>` | 서버에 clone한 KBO_playwright 루트 (예: `C:\...\KBO_playwright`) | 그 폴더에 `scripts\scheduler.py`와 `.env`가 있어야 한다 |
| `<파이썬 경로>` | 스케줄러를 돌리는 가상환경의 `python.exe` | 저장소 문서에는 uv 기반 `.venv\Scripts\python.exe`가 나온다(`STADIUM_MASTER_DATA.md`). 실제로 쓰는 venv를 확인한다 |
| `<작업 이름>` | 작업 스케줄러에 등록한 작업 이름 | `Get-ScheduledTask -TaskName '*KBO*'` |
| `<실행 계정>` | 작업을 실행하는 Windows 계정 | Playwright 브라우저를 설치한 계정과 같아야 한다(§2) |

## 1. 구성 요약

| 항목 | 값 | 근거 |
|---|---|---|
| 진입점 | `scripts\scheduler.py` (APScheduler `BlockingScheduler`, 상주 프로세스) | `scripts/scheduler.py` → `src/scheduler/registry.py::main` |
| 권장 인자 | `--no-startup-run` (기동 시 놓친 날짜 보충 실행을 끈다) | 이전 launchd 템플릿과 같은 인자. 끄지 않으면 `STARTUP_RUN`(기본 `1`)에 따라 기동 직후 `backfill_missed_daily_crawls`가 돈다 |
| 작업 디렉터리 | **반드시 `<프로젝트 경로>`** | 로그 경로가 현재 디렉터리 기준 상대 경로 `logs/scheduler.log`다(`src/scheduler/config.py`). `.env`는 프로젝트 루트에서 읽는다 |
| 설정 | `<프로젝트 경로>\.env` (`DATABASE_URL` 등) | `src/scheduler/config.py`의 `load_project_env(PROJECT_ROOT / ".env")` |
| DB | PostgreSQL. `DATABASE_URL`에 keepalive 파라미터가 있어야 한다 | `AGENTS.md` "Current Database Contract" |
| 단일 실행 보장 | PID 파일 `data\locks\scheduler.pid` (`SCHEDULER_PID_FILE`로 변경 가능) | `src/scheduler/locks.py`. Windows에서는 `OpenProcess`로 PID 생존을 확인한다 |
| 메트릭 | `http://127.0.0.1:<PROMETHEUS_PORT>/metrics` (기본 `8000`) | `registry.py::main`의 `start_metrics_server(_env_int("PROMETHEUS_PORT", 8000))` |

Oracle은 쓰지 않는다. 스케줄러용 Oracle 설정(지갑, `TNS_ADMIN` 등)을 새로 넣지 않는다.

## 2. 사전 준비 (최초 1회)

PowerShell에서 `<실행 계정>`으로 실행한다.

```powershell
cd "<프로젝트 경로>"

# 의존성 (pyproject.toml이 단일 기준)
& "<파이썬 경로>" -m pip install -e ".[dev]"

# Playwright 브라우저. 기본 설치 위치가 사용자 프로필 아래(%LOCALAPPDATA%\ms-playwright)라서
# 작업 스케줄러의 <실행 계정>과 같은 계정으로 설치해야 한다.
& "<파이썬 경로>" -m playwright install chromium

# .env 확인 (값은 출력하지 않는다)
Test-Path .\.env
Select-String -Path .\.env -Pattern '^DATABASE_URL=' -Quiet
```

- `.env`의 `DATABASE_URL`이 이 서버의 PostgreSQL을 가리키는지 확인한다. 같은 서버이므로 `127.0.0.1`을 쓸지, 지금처럼 Tailscale 주소를 쓸지는 현재 `.env` 값을 따른다(**운영자 확인 필요**). 어느 쪽이든 keepalive 파라미터는 빼지 않는다.
- PostgreSQL은 Docker 컨테이너(`kbo_postgres`)로 돌고 있다(`WINDOWS_DB_HOST_RECOVERY.md` §9). Docker Desktop이 로그온 전에 뜨는지, 로그온 후에야 뜨는지 확인한다(**운영자 확인 필요**). 로그온 후에 뜬다면 아래 트리거를 "로그온 시"로 바꾸거나 지연을 늘린다.

## 3. 수동 실행 (점검·디버깅용)

```powershell
cd "<프로젝트 경로>"

# 상주 실행 (콘솔에 로그가 보인다. Ctrl+C로 정상 종료 = SIGINT 핸들러가 진행 중 잡을 기다린다)
& "<파이썬 경로>" scripts\scheduler.py --no-startup-run

# 잡 하나만 즉시 실행하고 끝내기 (상주 스케줄러와 별개, PID 가드를 거치지 않는다)
& "<파이썬 경로>" scripts\scheduler.py --run-once               # 일일 업데이트
& "<파이썬 경로>" scripts\scheduler.py --run-pregame-once       # 경기 전 갱신
& "<파이썬 경로>" scripts\scheduler.py --run-integrity-check-once
& "<파이썬 경로>" scripts\scheduler.py --run-auto-heal-once
& "<파이썬 경로>" scripts\scheduler.py --run-retire-once --limit 10
```

작업 스케줄러로 이미 상주 중이면 상주 실행을 하나 더 띄우지 않는다. 두 번째 프로세스는 `Another scheduler instance (PID ...) is already running.`를 남기고 종료한다.

## 4. 작업 스케줄러 등록

관리자 PowerShell에서 실행한다. 이미 같은 이름이 있으면 먼저 §6으로 상태를 확인한다.

```powershell
$taskName = "<작업 이름>"
$root     = "<프로젝트 경로>"
$python   = "<파이썬 경로>"

# -WorkingDirectory는 필수: logs\scheduler.log가 이 폴더 기준으로 생긴다
$action = New-ScheduledTaskAction -Execute $python `
    -Argument "scripts\scheduler.py --no-startup-run" `
    -WorkingDirectory $root

# 부팅 시 시작. DB 컨테이너가 먼저 뜨도록 지연을 둔다(값은 서버에 맞게 조정).
$trigger = New-ScheduledTaskTrigger -AtStartup
$trigger.Delay = "PT2M"

# 로그온 여부와 관계없이 실행(S4U = 비밀번호 저장 안 함). 로컬 파일·로컬 TCP만 쓰므로 S4U로 충분하다.
$principal = New-ScheduledTaskPrincipal -UserId "<실행 계정>" -LogonType S4U -RunLevel Limited

# -MultipleInstances IgnoreNew: 이미 돌고 있으면 새로 띄우지 않는다
# -ExecutionTimeLimit 0: 필수. 기본값(72시간)이면 상주 프로세스가 사흘 뒤 강제 종료된다
# (백틱 줄 이음 뒤에는 주석을 붙일 수 없어서 설명을 위에 모았다)
$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
    -Principal $principal -Settings $settings `
    -Description "KBO_playwright scheduler (scripts\scheduler.py)"

Start-ScheduledTask -TaskName $taskName
```

GUI(`taskschd.msc`)로 등록한다면 같은 값을 넣는다.

| 탭 | 설정 |
|---|---|
| 일반 | "사용자의 로그온 여부에 관계없이 실행", 계정 = `<실행 계정>` |
| 트리거 | "시작할 때", 지연 2분(조정 가능) |
| 동작 | 프로그램 = `<파이썬 경로>`, 인수 = `scripts\scheduler.py --no-startup-run`, **시작 위치 = `<프로젝트 경로>`** |
| 설정 | "작업이 이미 실행 중이면: 새 인스턴스를 시작하지 않음", **"다음 시간 이상 실행되면 작업 중지" 해제**, 실패 시 1분 간격 3회 다시 시작 |

주의:

- "실패 시 다시 시작"은 프로세스가 비정상 종료하는 모든 경우를 잡아 주지 않을 수 있다. 정기적으로 §6의 상태 확인을 한다. 스케줄러 자체의 정지 감지는 메트릭(`monitoring/prometheus/` 알림 규칙)이 맡는다.
- 표준 출력/오류는 작업 스케줄러가 저장하지 않는다. 로그는 §7의 `logs\scheduler.log`가 기준이다. `cmd /c ... 2>> 파일`로 오류 출력을 따로 받으려면, 주간 로그 트림 잡이 `logs\scheduler.launchd.err.log`만 자른다는 점(§7)을 감안한다. 다른 이름의 파일은 저절로 줄지 않는다.

## 5. 재시작 (잡 추가·코드 갱신 후)

새 잡이나 코드 변경은 **재시작해야 반영**된다.

```powershell
$taskName = "<작업 이름>"

# 1) 진행 중인 잡이 없는지 로그 끝을 본다 (특히 03:00 crawl_daily_games, 경기 시간대 live refresh)
Get-Content "<프로젝트 경로>\logs\scheduler.log" -Tail 30

# 2) 중지
Stop-ScheduledTask -TaskName $taskName

# 3) 남은 스케줄러 프로세스가 없는지 확인 (아래 "주의" 참고)
Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" |
    Where-Object CommandLine -like '*scheduler.py*' |
    Select-Object ProcessId, ParentProcessId, CommandLine
# 남아 있으면: Stop-Process -Id <PID>

# 4) 시작 후 확인 (§6)
Start-ScheduledTask -TaskName $taskName
```

주의:

- Windows에서는 작업 중지(`Stop-ScheduledTask`, 작업 관리자 "작업 끝내기")가 SIGTERM을 보내지 않고 프로세스를 바로 끝낸다. 진행 중이던 잡은 끊기고 `data\locks\scheduler.pid`가 남는다. 남은 PID 파일은 다음 기동 때 `Removing stale scheduler PID file`로 자동 정리되고, 끊긴 잡의 tier 락(`data\locks\*.lock`)은 네 개 모두 `ForceProcessLock`이라, 주인 PID가 죽어 있으면 다음 획득 때 정리된다(`src/scheduler/locks.py`, `src/utils/lock.py`). 끊기면 안 되는 작업 중이면 끝날 때까지 기다린다.
- venv의 `python.exe`는 실제 인터프리터를 자식 프로세스로 띄우는 런처일 수 있다. 그래서 3)에서 부모·자식 두 줄이 보일 수 있고, 중지 후 자식이 남아 있을 수도 있다. 남은 프로세스를 정리하지 않고 시작하면 새 프로세스가 PID 가드에 막혀 바로 종료된다.
- 정상 종료(진행 중 잡을 기다림)가 꼭 필요하면 작업을 중지·비활성화한 뒤 §3처럼 콘솔에서 띄워 Ctrl+C로 끈다.

## 6. 상태 확인

```powershell
cd "<프로젝트 경로>"

# 작업 상태: State = Running 이어야 한다. LastTaskResult 267009(0x41301)은 "실행 중"이다.
Get-ScheduledTask -TaskName "<작업 이름>" | Select-Object TaskName, State
Get-ScheduledTaskInfo -TaskName "<작업 이름>" | Select-Object LastRunTime, LastTaskResult, NextRunTime

# 프로세스 (스케줄러는 하나만 떠 있어야 한다. 런처+자식 한 쌍은 하나로 본다)
Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" |
    Where-Object CommandLine -like '*scheduler.py*' |
    Select-Object ProcessId, ParentProcessId, CreationDate

# PID 파일
Get-Content .\data\locks\scheduler.pid

# 기동 로그: 아래 문자열이 차례로 보이면 정상 기동이다
Select-String -Path .\logs\scheduler.log -Pattern `
    'Resolved database target', 'Database connectivity check passed', `
    'Registered job: crawl_daily_games', 'KBO Crawler Scheduler Started' |
    Select-Object -Last 8

# 메트릭 엔드포인트
(Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8000/metrics).StatusCode   # PROMETHEUS_PORT를 바꿨다면 그 포트

# 락 진단 (읽기 전용, exit 0 = 정상)
& "<파이썬 경로>" scripts\diagnose_scheduler_locks.py -v

# P1/P2 잡 락 상태 (logs\scheduler.log를 우선 스캔)
& "<파이썬 경로>" scripts\check_p1p2_lock_health.py
```

- `Database target ... did not answer SELECT 1`이 보이면 DB가 아직 안 떴거나 끊긴 것이다. 스케줄러는 계속 돌고, DB 잡은 fail-fast 게이트가 건너뛴다. DB 쪽은 `WINDOWS_DB_HOST_RECOVERY.md` §2부터 본다.
- `Failed to start Prometheus HTTP server on port 8000`이 보이면 포트가 이미 쓰이고 있다. 스케줄러는 계속 돌지만 메트릭이 비므로 `.env`의 `PROMETHEUS_PORT`를 바꾸고 재시작한다.
- `diagnose_scheduler_locks.py`의 중복 프로세스 검사는 `pgrep`을 쓰므로 Windows에서는 항상 비어 있다. 락 파일·PID 파일 검사만 유효하고, 중복 프로세스는 위의 `Win32_Process` 조회로 본다.

## 7. 로그와 상태 파일 위치

모두 `<프로젝트 경로>` 기준 상대 경로다.

| 경로 | 내용 |
|---|---|
| `logs\scheduler.log` | **운영 로그(기준)**. `RotatingFileHandler`, 10MB × 5개(`scheduler.log.1` ~ `.5`), UTF-8 |
| `data\locks\scheduler.pid` | 상주 스케줄러 PID |
| `data\locks\*.lock` | tier 락(`daily_update`, `live_refresh`, `maintenance`, `sqlite_writer`) |
| `data\last_runs\p1p2_data.json` | `crawl_p1p2_data_job` 실행 마커(`check_p1p2_lock_health.py --require-run`이 읽는다) |
| `data\archive\logs\` | 로그 트림 아카이브 |
| 이벤트 뷰어 `Microsoft-Windows-TaskScheduler/Operational` | 작업 시작·종료·실패 기록. 작업 스케줄러에서 "모든 작업 기록 사용"이 켜져 있어야 남는다 |

```powershell
Get-Content .\logs\scheduler.log -Tail 100
Select-String -Path .\logs\scheduler.log* -Pattern 'ERROR|Traceback' | Select-Object -Last 20
Get-WinEvent -LogName Microsoft-Windows-TaskScheduler/Operational -MaxEvents 20 |
    Where-Object Message -like '*<작업 이름>*' | Select-Object TimeCreated, Id, Message
```

- 로그 시각은 서버 로컬 시각으로 찍힌다. 서버 시간대가 KST가 아니면 다른 문서의 KST 시각과 맞춰 읽는다.
- 주간 잡 `trim_scheduler_logs_weekly`는 `logs\scheduler.launchd.err.log`만 자른다(launchd 시절 이름). Windows 구성에서는 이 파일이 없어 `No scheduler log file to trim`이 남는 것이 정상이다. `scheduler.log`는 회전으로 크기가 제한된다.
- 다른 문서에 나오는 `logs/scheduler.launchd.err.log` 기반 `grep` 명령은 Windows에서 `Select-String -Path .\logs\scheduler.log* -Pattern '...'`로 바꿔 실행한다.

## 8. 자주 생기는 문제

| 증상 | 확인 | 조치 |
|---|---|---|
| 작업은 Running인데 `logs\scheduler.log`가 안 생김 | 동작의 "시작 위치" | `<프로젝트 경로>`로 지정하고 재시작. 엉뚱한 폴더에 `logs\`가 생겼을 수 있다 |
| 기동 직후 종료, 로그에 `Another scheduler instance` | §6 프로세스 조회 | 남은 프로세스를 정리하거나, 이미 하나가 정상이라면 그대로 둔다 |
| 사흘쯤 지나 스케줄러가 멈춤 | 작업 설정의 실행 시간 제한 | `ExecutionTimeLimit`을 0(제한 없음)으로 다시 등록 |
| Playwright `Executable doesn't exist` | 브라우저 설치 계정 | `<실행 계정>`으로 `playwright install chromium` 재실행 |
| DB 잡이 계속 skip | 로그의 `Job ... skipped: database unreachable` | `WINDOWS_DB_HOST_RECOVERY.md` |

## 9. 이전 방식: Mac + launchd (더 이상 사용하지 않음)

2026-10 중순까지는 스케줄러가 Mac에서 launchd 사용자 에이전트(`com.kbo-playwright.scheduler`)로 돌았고, Windows 서버에는 PostgreSQL만 있었다. **지금은 쓰지 않는다.** 과거 기록과 다른 문서의 옛 명령을 읽을 때 참고용으로만 남긴다.

| 항목 | 이전 값 |
|---|---|
| 서비스 템플릿 | `scripts/launchd/com.kbo-playwright.scheduler.plist` (`RunAtLoad`, `KeepAlive`) |
| 설치 / 복구 스크립트 | `scripts/install_scheduler_launchd.sh`, `scripts/restore_scheduler_launchd.sh` |
| 재시작 | `launchctl kickstart -k gui/$(id -u)/com.kbo-playwright.scheduler` |
| 로그 | `logs/scheduler.launchd.out.log`, `logs/scheduler.launchd.err.log` (launchd가 stdout/stderr를 받아 적던 파일) |
| 비활성화 상태 | `~/Library/LaunchAgents/com.kbo-playwright.scheduler.plist.disabled` |

- Mac에서 launchd 스케줄러를 다시 켜지 않는다. Windows 스케줄러와 동시에 돌면 같은 DB에 두 스케줄러가 쓰게 된다. PostgreSQL advisory 락이 로컬 파일 락으로 떨어지는 상황에서는 서로를 막지 못한다(`AGENTS.md`, "Operate a single scheduler host").
- 위 스크립트와 plist는 테스트(`tests/scripts/test_restore_scheduler_launchd.py`)가 참조하므로 이번 문서 정리에서는 지우지 않았다.
