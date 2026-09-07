# Gate R2: Limited Live Relay Smoke Certification (Remediated)

## 1. Overview & Operational Scope
- **Gate**: `GATE_R2_LIMITED_LIVE_RELAY_SMOKE_REMEDIATED`
- **Operational Label**: 종료된 역사 경기의 라이브 엔드포인트 read-only smoke
- **Target Game**: `20240930NCHT0` (KBO) / `20240930NCHT02024` (Naver)
  - **Date**: 2024-09-30 (2024 Regular season finale, Gwangju-Kia Champions Field)
  - **Matchup**: NC Dinos (5) at KIA Tigers (10)
  - **Status**: `COMPLETED`
  - **Comparison Scope**: Inning 9 top (`TERMINAL_HALF_INNING`)
- **Overall Verdict**: **`PASS — LIMITED HISTORICAL DUAL-SOURCE LIVE-ENDPOINT SMOKE`**
- **Primary Operational Goal**: Validate live remote response structures of KBO and Naver text relay endpoints under strict, auditable request budgets without mutating local storage or connecting to production databases.

### Explicit Scope Boundaries
> **Certified Scope**: 한 개의 종료된 역사 경기, 9회초 종료 구간에 대한 KBO·Naver 라이브 엔드포인트 read-only 교차 검증 통과 (`PASS_DUAL_SOURCE_CANONICAL_MATCH`).
>
> **Explicitly NOT Certified in Gate R2**:
> - 진행 중 경기의 실시간 장기 polling (`NOT_TESTED`)
> - 30개 크롤러 전체의 라이브 동작 (`NOT_TESTED`)
> - 정규시즌 외 모든 경기 유형의 URL 라이브 계약 (정규시즌만 `LIVE_VERIFIED`, 포스트시즌/퓨처스는 `UNIT_VERIFIED`)
> - 전체 경기(1~9회)의 KBO↔Naver 패리티 (9회초 5개 이벤트에 한정)
> - 라이브 이벤트의 운영 DB 영속화 (`NO-GO`)
> - Scheduler crash/restart 복구 (Gate R4A에서 검증)
> - Oracle 또는 production 동작 (`STRICT NO-GO`)

---

## 2. Key Remediation Pillars (Gate R2-R1 ~ R2-R5)

### 1. R2-R1: KBO Canonical URL Contract with Provenance
- Resolved via immutable `KboRelayTarget` (`src/utils/kbo_relay_target.py`).
- Enforces `resolved_from` provenance (`verified_target_fixture`).
- Canonical URL: `https://www.koreabaseball.com/Game/LiveText.aspx?leagueId=1&seriesId=0&gameId=20240930NCHT0&gyear=2024`.
- Single URL generation path across the repository. Fails closed with `R2_TARGET_METADATA_UNRESOLVED` when evidence is missing.
- **Verification Levels**:
  - 2024 정규시즌 Target URL: **`LIVE_VERIFIED`** (actual KBO LiveText response received)
  - 포스트시즌 및 퓨처스리그 URL 매핑: **`UNIT_VERIFIED`** (`tests/test_kbo_relay_target.py` 11 tests passed)
  - 수동 URL 조립 방지 정적 검사: **`PASS`** (`tests/test_no_manual_livetext_urls.py` passed)

### 2. R2-R2: Dual-Source Live Relay Smoke Verified (Completed Historical Game)
- Target: Completed historical match `20240930NCHT0` (top of 9th).
- Discovery probe explicitly separated and recorded in `discovery-probe-ledger.jsonl` (`DISCOVERY_PROBE NON_CERTIFYING`).
- Both live endpoints reached and parsed under approved budget constraints:
  - Poll count: KBO 1, Naver 1 (limit: $\le 3$)
  - Auto-retries: 0 (limit: $\le 1$)
  - Concurrency: 1
  - DB mutations: 0 (SHA-256 unchanged)
  - Unexpected outbound hosts: 0

### 3. R2-R3: Exhaustive Raw Data Reconciliation (Both KBO DOM and Naver Options)
- **Naver Options Reconciliation**:
  $$\text{Raw Options (39)} = \text{Events (5)} + \text{Commentary (31)} + \text{Headers (3)} + \text{Duplicates (0)} + \text{Unclassified (0)}$$
  - Verified `unclassified_rows = 0`, `reconciliation_complete = true`.
  - **Count Delta Notice**: 이전 탐색에서 관측된 37개에서 39개로 변경된 사유는 `PARSER_SCOPE_CORRECTION` (8개 릴레이 그룹 전체에 대한 완전 파싱 및 옵션 분류 적용)입니다.
- **KBO Leaf DOM Nodes Reconciliation**:
  $$\text{Raw Leaf DOM Nodes (41)} = \text{Events (5)} + \text{Commentary (29)} + \text{Headers (7)} + \text{Structural (0)} + \text{Unclassified (0)}$$
  - 41개의 비어 있지 않은 leaf DOM node를 분류했고, 그중 5개를 normalized baseball event로 판정.
  - Verified `unclassified_nodes = 0`, `reconciliation_complete = true`.

### 4. R2-R4: Multi-Dimensional Cross-Source Match Audit
- **Primary Classification**: **`MATCHED_CANONICAL_EXACT: 5`**
  - 원문 문자열의 byte-for-byte 일치가 아니라, 각 소스 parser를 거쳐 23개 canonical 필드로 정규화된 이벤트 간의 완전 일치를 의미합니다 (`PASS_DUAL_SOURCE_CANONICAL_MATCH`).
  - `KBO_ONLY`: 0, `NAVER_ONLY`: 0, `AMBIGUOUS`: 0.
- **Secondary Attributes**:
  - `ORDER_DIFFERENCE`: 0, `GRANULARITY_DIFFERENCE`: 0, `CORRECTION_CANDIDATE`: 0, `FALSE_MERGE_REVIEW_REQUIRED`: 0.
- **Match Scope**:
  - `relay.r2.cross_source.one_to_one_canonical_match.five_events.v1`: **`PASS`** (5개 1:1 매칭 그룹 `MG-0001`~`MG-0005`)
  - `1:N / N:1 live matching`: **`NOT_TESTED`** (해당 표본에서는 1:1로 수렴)
- **23-Field Provenance Matrix**:
  - `field-provenance-matrix.json`에 6개 분류(`SOURCE_DERIVED`, `CALCULATED`, `DEFAULT_FILLED`, `NULL_NOT_AVAILABLE`, `NOT_APPLICABLE`, `INVALID`)로 전수 기록.
  - WPA/WE 필드는 내부 계산 엔진 기반 `CALCULATED`로 분류되어 소스 간 독립 일치가 아닌 동일 계산식 일치임을 명시.
- **Domain Invariants**:
  - `domain-invariant-results.json`: 아웃 전이(0→1→2→2→3), 점수 일관성(5:10), 베이스 상태, 승리확률(0.0~1.0 및 연속성), 종료 조건 등 7개 검사 모두 위반 0건 (`relay.r2.domain_invariants.one_terminal_half_inning.v1 = PASS`).

### 5. R2-R5: Code→Evidence Strict Commit Isolation & Provenance
- `C_R2_CODE`: `e3371ca70abfc0186bee1df4bb30eae87553c157` (Tree SHA: `0b02b7d22fa43abeef9916229b6304890a5517a0`)
- `C_R2_EVIDENCE`: `e0eb51dc48671752b04f7623ca397fe6f6c9d78e`
- `C_R2_EVIDENCE^ == C_R2_CODE` (완전한 직계 부모 확인)
- `git diff --name-status C_R2_CODE..C_R2_EVIDENCE` $\to$ Strictly touches only `Docs/certification/phase-106/gate-106f-r2-live-relay/`.
- Dynamic file tracking: 24 payload files + 2 integrity files (total 26 files) under `SHA256SUMS`.
- **Superseded Evidence Record**: 이전 비대칭/미완성 증적(`b637...`)은 `SUPERSEDED` (사유: `KBO canonical URL contract was incomplete`)로 공식 대체되었습니다.

---

## 3. Network Architecture & Traffic Separation

| 네트워크 구분 | 대상 호스트 | 요청 수 | 판정 |
| :--- | :--- | :---: | :---: |
| **Transmitted (Allowed)** | `www.koreabaseball.com`, `api-gw.sports.naver.com`, `6ptotvmi5753.edge.naverncp.com` | 15 | **PASS** |
| **Attempted (Observed)** | `cdnjs.cloudflare.com` (정적 라이브러리) | 1 | **PASS** |
| **Blocked (Filtered)** | 트래커, 광고, 폰트, 이미지 등 비인가 리소스 | 30 | **PASS** |
| **Unapproved Hosts Transmitted** | 없음 | 0 | **PASS** |

---

## 4. Evidence Artifacts Inventory (26 Files)

| File | Purpose / Contents | Checksum Verification |
| :--- | :--- | :---: |
| `README.md` | Gate R2-R formal audit report and operational summary | **OK** |
| `live-relay-plan.json` | Execution limits, observed budget, and persistence blockade | **OK** |
| `target-identity.json` | Game identity contract for `20240930NCHT0` | **OK** |
| `tested-code-manifest.json` | Commit SHA, tree SHA, runner SHA, superseded record | **OK** |
| `discovery-probe-ledger.jsonl` | Discovery probe logs marked `DISCOVERY_PROBE NON_CERTIFYING` | **OK** |
| `network-request-ledger.jsonl` | Allowed, blocked, and observed network traffic ledger | **OK** |
| `browser-console-ledger.jsonl` | Browser console logs during KBO Playwright navigation | **OK** |
| `pageerror-ledger.jsonl` | Zero browser page errors | **OK** |
| `response-manifest.json` | HTTP status codes, latencies, payloads, observed hosts | **OK** |
| `kbo-raw-response-manifest.json` | KBO response payload hash and 41 extracted spans | **OK** |
| `naver-raw-response-manifest.json` | Naver response payload hash, 8 groups, 39 options, delta record | **OK** |
| `kbo-normalized-events.jsonl` | 5 normalized events from KBO LiveText | **OK** |
| `naver-normalized-events.jsonl` | 5 normalized events from Naver Sports API | **OK** |
| `kbo-dom-node-classification-ledger.json` | Reconciles 41 KBO DOM nodes (5 events + 29 comm + 7 hdr, 0 unclassified) | **OK** |
| `naver-option-classification-ledger.json` | Reconciles 39 Naver options (5 events + 31 comm + 3 hdr, 0 unclassified) | **OK** |
| `cross-source-comparison.json` | Multi-dimensional cross-source match summary (5 canonical exact matches) | **OK** |
| `cross-source-match-groups.jsonl` | 5 exact match groups (`MG-0001` ~ `MG-0005`, `MATCHED_CANONICAL_EXACT`) | **OK** |
| `field-provenance-matrix.json` | 23 canonical fields evaluated with strict provenance categories | **OK** |
| `domain-invariant-results.json` | 0 failures on outs, scores, base states, WE continuity/bounds | **OK** |
| `semantic-match-review.jsonl` | Event-by-event classification review ledger (`MATCHED_CANONICAL_EXACT`) | **OK** |
| `protected-db-before-after.json` | SQLite DB SHA-256 bit-level identical (zero mutations) | **OK** |
| `raw-run-output.txt` | Complete stdout/stderr log of the runner execution | **OK** |
| `git-status-before.txt` | Git working tree status before run | **OK** |
| `git-status-after.txt` | Git working tree status after run | **OK** |
| `SHA256SUMS` | SHA-256 hashes of all 24 payload files | **OK** |
| `checksum-verification.txt` | Independent `shasum -a 256 -c` verification output (all 24 OK) | **OK** |
