# AI 전용 데이터 뷰 (`v_ai_game_context`) 활용 가이드

이 문서는 AI 서비스 및 분석 대시보드에서 `v_ai_game_context` 뷰를 활용하여 경기 맥락 데이터를 추출하는 방법을 설명합니다.

## 1. 개요

`v_ai_game_context`는 경기 기본 정보와 함께 AI 분석에 핵심적인 **프리뷰(전력 분석)** 및 **리뷰(승부처 분석)** 데이터를 JSON 형식으로 통합하여 제공합니다.

- **프리뷰 데이터:** 선발 투수 기록, 팀 최근 10경기 흐름, 맞대결 전적, 최근 타격/투구 지표.
- **리뷰 데이터:** WPA(Win Probability Added) 기반 결정적 순간(Crucial Moments).

## 2. 테이블 구조

| 컬럼명 | 타입 | 설명 |
| :--- | :--- | :--- |
| `game_date` | DATE | 경기 날짜 |
| `game_id` | VARCHAR | 경기 고유 ID (예: 20250412SKLG0) |
| `home_team` | VARCHAR | 홈 팀 코드 |
| `away_team` | VARCHAR | 원정 팀 코드 |
| `home_score` | INT | 홈 팀 득점 |
| `away_score` | INT | 원정 팀 득점 |
| `game_status` | VARCHAR | 경기 상태 (SCHEDULED, COMPLETED, CANCELLED 등) |
| `preview_data` | JSON | **[AI 핵심]** 경기 전 분석용 풍부한 맥락 데이터 |
| `review_wpa_data` | JSON | **[AI 핵심]** 경기 종료 후 주요 승부처 데이터 |

## 3. 주요 활용 사례 및 쿼리 예시

### A. 특정 경기의 AI 분석용 통합 데이터 조회
경기 전/후에 필요한 모든 데이터를 한 번에 가져와서 LLM 프롬프트에 주입할 때 사용합니다.

```sql
SELECT 
    game_id,
    game_status,
    preview_data,
    review_wpa_data
FROM v_ai_game_context
WHERE game_id = '20250412SKLG0';
```

### B. 특정 팀의 최근 흐름 및 맥락 분석
특정 팀(예: 'LG')의 최근 경기 결과와 당시의 팀 지표(Metrics)를 분석할 때 유용합니다.

```sql
SELECT 
    game_date,
    away_team,
    home_team,
    preview_data->'away_recent_l10'->>'l10_text' as away_l10,
    preview_data->'home_recent_l10'->>'l10_text' as home_l10,
    preview_data->'away_metrics'->>'avg' as away_recent_avg,
    preview_data->'away_metrics'->>'bullpen_era' as away_recent_bp_era
FROM v_ai_game_context
WHERE (home_team = 'LG' OR away_team = 'LG')
  AND game_status = 'COMPLETED'
ORDER BY game_date DESC
LIMIT 5;
```

### C. 어제 경기의 주요 승부처(하이라이트) 요약
어제 진행된 모든 경기의 주요 승부처 리스트를 가져와 요약 뉴스를 생성할 때 사용합니다.

```sql
SELECT 
    game_id,
    review_wpa_data->'crucial_moments' as moments
FROM v_ai_game_context
WHERE game_date = CURRENT_DATE - 1
  AND review_wpa_data IS NOT NULL;
```

## 4. 데이터 파싱 가이드 (Python 예시)

`preview_data`와 `review_wpa_data`는 JSON 타입이므로 Python에서 다음과 같이 쉽게 활용할 수 있습니다.

```python
import json

# DB에서 가져온 row 데이터라고 가정
row = {
    "game_id": "20250412SKLG0",
    "preview_data": {
        "away_metrics": {"avg": 0.275, "bullpen_era": 3.45},
        "matchup_h2h": {"summary_text": "3승 1패 0무 (LG 우세)"}
    },
    "review_wpa_data": {
        "crucial_moments": [
            {"inning": "9회말", "description": "홍길동 끝내기 안타", "wpa": 0.45}
        ]
    }
}

# 선발 투수 정보 접근
h2h_text = row['preview_data']['matchup_h2h']['summary_text']
print(f"상대 전적: {h2h_text}")

# 승부처 리스트 반복
for moment in row['review_wpa_data']['crucial_moments']:
    print(f"[{moment['inning']}] {moment['description']} (WPA: {moment['wpa']})")
```

## 5. 주의 사항

1. **데이터 업데이트 타이밍:** `preview_data`는 경기 시작 약 1~2시간 전(선발 투수 확정 후)에 생성되며, `review_wpa_data`는 경기 종료 및 문자중계 수집 완료 후 생성됩니다.
2. **NULL 처리:** 경기 전에는 `review_wpa_data`가 NULL일 수 있으며, 비정상적으로 종료되거나 데이터가 없는 경우 `preview_data` 내부 필드들이 비어있을 수 있으므로 방어적 프로그래밍이 필요합니다.
