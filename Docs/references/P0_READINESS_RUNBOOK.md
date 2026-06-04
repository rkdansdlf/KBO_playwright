# P0 경기 데이터 운영 게이트

P0 readiness는 경기 전, 경기 중, 경기 후 파이프라인이 운영 가능한 상태인지 한 번에 확인하는 점검 결과다. 직관 이벤트와 티켓 가격 같은 P0 비경기 데이터는 `monitor_data_freshness`의 DataSource/table freshness로 별도 점검한다.

## 점검 대상

- `schedule`: 일정, 팀, 상태, 시작 시간, 구장
- `pregame`: 선발투수, 프리뷰 요약, 선발 라인업
- `live`: 라이브 상태 경기의 현재 스코어, relay/PBP, 진행 이닝
- `postgame`: 최종 스코어, 박스스코어, 이닝 스코어, 투수 결정 기록
- `relay`: `game_events` 또는 `game_play_by_play`
- `roster`: `team_daily_roster`, `roster_transactions`
- `broadcast`: TV/라디오 중계 행
- `oci`: OCI sync 준비 상태와 publish skip 사유

## 수동 확인

```bash
python3 -m src.cli.check_data_status --p0 --date 20260601 --lookahead-days 7 --json
```

기본적으로 `--date` 기준 최근 7일과 다음 1일을 본다. `--json`은 자동화에서 쓰기 좋은 `p0_readiness` 객체만 출력한다.

최근 3일 dry-run 점검:

```bash
python3 - <<'PY'
from datetime import datetime, timedelta

from src.db.engine import SessionLocal
from src.services.p0_readiness import build_p0_readiness, format_p0_readiness_summary

today = datetime.now().date()
dates = [(today - timedelta(days=offset)).strftime("%Y%m%d") for offset in (2, 1, 0)]

with SessionLocal() as session:
    for day in dates:
        report = build_p0_readiness(session, target_date=day, lookback_days=0, lookahead_days=0)
        print(day, format_p0_readiness_summary(report))
PY
```

## 일일 운영 연결

`python3 -m src.cli.run_daily_update --date YYYYMMDD`는 `logs/daily_update_summary/YYYYMMDD.json`에 `p0_readiness` 섹션을 함께 기록한다. 또한 기본 실행에서는 P0 비경기 데이터인 구단 이벤트/뉴스와 티켓 가격/오픈 규칙도 저장하고, 결과를 `stability.p0_non_game`에 남긴다.

P0 backfill처럼 이미 대상 경기 데이터를 보강했고 summary/OCI publish만 확인해야 하는 scoped 실행에서는 비-P0 병목을 명시적으로 제외할 수 있다.

```bash
python3 -m src.cli.run_daily_update --date YYYYMMDD --sync --skip-auto-healer --skip-season-stats --skip-oci-supporting-sync
```

- `--skip-season-stats`: 누적 시즌 타격/투수 웹 크롤링을 건너뛴다. P0 경기 상세, relay, roster, broadcast readiness 계산에는 영향을 주지 않는다.
- `--skip-oci-supporting-sync`: 대상 경기 publish 후 연간 standings/matchup/ranking/season stats/daily roster 전체 OCI sync를 건너뛴다. summary의 `p0_readiness.oci.skip_counts.oci_supporting_sync_skipped`에 남는다.
- `--skip-p0-non-game`: 과거 날짜 scoped backfill에서 현재 기준 이벤트/티켓 소스를 다시 크롤링하지 않는다. P0 경기 readiness 계산에는 영향을 주지 않는다.

스케줄러의 03:00 daily job은 기본적으로 full finalize를 실행한다. 운영 복구일 또는 비-P0 병목 회피가 필요한 날에는 다음 env로 scoped P0 모드를 켠다.

```bash
DAILY_SKIP_SEASON_STATS=1
DAILY_SKIP_OCI_SUPPORTING_SYNC=1
```

이 env는 `run_daily_update`에 각각 `--skip-season-stats`, `--skip-oci-supporting-sync`를 전달한다. full finalize로 복귀하려면 두 값을 unset 또는 `0`으로 둔다.

스케줄러의 06:20 P0 non-game job은 `crawl_p0_data --type all --save --days 3 --season <current_year>`를 실행한다. 07:00 freshness monitor는 전날 경기 기준 critical P0 failure와 함께 `team_events`, `roster_transactions`, `ticket_prices`, `ticket_open_rules` table freshness를 Slack/로그 알림 대상에 포함한다.

당일 roster OCI 재시도는 날짜 스코프를 명시한다. 날짜를 생략하면 기존 호환성을 위해 전체 `team_daily_roster`를 동기화한다.

```bash
python3 -m src.cli.sync_oci --daily-roster --roster-date YYYYMMDD
```

release gate:

```bash
./scripts/verification/crawler_stability_gate.sh
```

live smoke는 운영 승인 시에만 실행한다.

```bash
KBO_LIVE_SMOKE=1 python3 -m src.cli.crawler_live_smoke --allow-network --date YYYYMMDD
```

## 실패 해석

- `critical`: 완료 경기의 최종 스코어, 박스스코어, relay 같은 핵심 데이터가 비어 있어 사용자 응답 품질에 직접 영향이 있다.
- `warning`: 발표 전 선발 라인업, 중계 정보, roster snapshot처럼 지연 또는 원천 누락 가능성이 있는 데이터다.
- `broadcast_not_announced`: 예정 경기의 방송 정보가 아직 발표되지 않은 상태다.
- `broadcast_source_unavailable`: 과거/완료 경기의 방송 원천이 현재 수집 가능한 KBO schedule payload에 없어서 명시 skip으로 남긴 상태다.
- `stability.quality_gates.non_p0_failure_counts`: 누적 시즌 스탯 또는 OCI 전체 parity처럼 P0 경기 publish와 분리된 품질 게이트 실패다. P0 critical이 0이면 경기 데이터 publish는 계속 진행하고, 해당 사유를 별도 복구 작업으로 다룬다.

P0 완료 기준은 critical failure 0건이다. warning은 `source_url`, 원천 수집 시각, 재시도 여부를 확인해 운영 판단으로 처리한다.
