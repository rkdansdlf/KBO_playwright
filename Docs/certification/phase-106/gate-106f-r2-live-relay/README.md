# Gate R2: Limited Live Relay Smoke Certification

## 1. Overview & Operational Context
- **Gate**: `GATE_R2_LIMITED_LIVE_RELAY_SMOKE`
- **Target Game**: `20240930NCHT0` (KBO) / `20240930NCHT02024` (Naver)
  - **Date**: 2024-09-30 (Regular season finale, Gwangju-Kia Champions Field)
  - **Matchup**: NC Dinos (5) at KIA Tigers (10)
  - **Status**: `COMPLETED`
- **Primary Operational Goal**: Validate live remote response structures of KBO and Naver text relay endpoints under strict, auditable request budgets without mutating local storage or connecting to production databases.

---

## 2. Operational Constraints & Budget Caps
| Dimension | Enforced Limit | Observed Execution | Compliance Status |
| :--- | :---: | :---: | :---: |
| **Target Game Count** | Exactly 1 | 1 | **PASS** |
| **KBO Top-Level Polls** | Max 3 | 2 (Scoreboard Warmup + LiveText) | **PASS** |
| **Naver Top-Level Polls** | Max 3 | 1 (Terminal Game Relay API) | **PASS** |
| **Concurrency** | Strictly 1 | 1 | **PASS** |
| **Max Auto-Retry** | 1 per source | 0 | **PASS** |
| **Allowed External Hosts** | Pre-declared Whitelist | Whitelist Only (0 unexpected) | **PASS** |
| **Browser Resource Blocking** | Images, Fonts, Media, Trackers | Enforced via Playwright Route Interception | **PASS** |
| **DB Persistence** | Strictly 0 writes / Session blocked | 0 writes (SHA-256 bit-level invariant verified) | **PASS** |
| **Production / Oracle Access** | Strictly Prohibited (NO-GO) | 0 connections | **PASS** |

---

## 3. Findings & Source Observations
1. **KBO Track (`www.koreabaseball.com`)**:
   - URL: `https://www.koreabaseball.com/Game/LiveText.aspx?gameId=20240930NCHT0&gyear=2024`
   - **Observation**: Navigates with scoreboard session warmup, then redirects to `https://www.koreabaseball.com/Error/Error.html?aspxerrorpath=/Game/LiveText.aspx`.
   - **Classification**: `R2_BLOCKED_SOURCE_UNAVAILABLE`.
   - **Context**: KBO official website deprecated the historical standalone `LiveText.aspx` page in favor of the GameCenter web/mobile platform. As prescribed by Section 6/7 of the Gate R2 contract, this is treated as `SOURCE_UNAVAILABLE` rather than a code defect.
2. **Naver Track (`api-gw.sports.naver.com`)**:
   - URL: `https://api-gw.sports.naver.com/schedule/games/20240930NCHT02024/relay`
   - **Observation**: Responds with `HTTP 200 OK`, valid JSON containing `textRelayData`, 8 text relay groups, lineups, inning scores, and 37 play options.
   - **Classification**: `SUCCESS`.
   - **Normalization**: Parsed through `RelayCrawler._parse_naver_payload` and normalized into the canonical 23-field schema in `naver-normalized-events.jsonl`.
3. **Rate Limit Signals**:
   - Neither source returned `HTTP 403`, `HTTP 429`, or CAPTCHA/bot challenges under the enforced polite throttling (`rate_limit_signal_observation: NO_RATE_LIMIT_SIGNAL`).

---

## 4. Evidence Artifacts Directory Inventory
All files are located in `Docs/certification/phase-106/gate-106f-r2-live-relay/`:
- `target-identity.json`: Pre-declared game identity contract.
- `live-relay-plan.json`: Execution parameters, limits, and runtime observation metadata.
- `response-manifest.json`: Combined response metadata, status codes, and network summaries.
- `kbo-raw-response-manifest.json`: Sanitized KBO response SHA-256 and redirection metadata.
- `naver-raw-response-manifest.json`: Sanitized Naver payload SHA-256, latency, and group counts.
- `kbo-normalized-events.jsonl`: Normalized 23-field event stream for KBO (0 events due to redirection).
- `naver-normalized-events.jsonl`: Normalized 23-field event stream for Naver.
- `cross-source-comparison.json`: 7-category taxonomy comparison summary.
- `semantic-match-review.jsonl`: Line-by-line event classification ledger.
- `network-request-ledger.jsonl`: Comprehensive log of all intercepted and permitted outbound HTTP requests.
- `browser-console-ledger.jsonl`: Browser console messages emitted during KBO navigation.
- `pageerror-ledger.jsonl`: Page errors observed in browser context.
- `protected-db-before-after.json`: Bit-level SHA-256 and sidecar check proving zero DB writes.
- `tested-code-manifest.json`: Immutable commit SHA, tree SHA, python runtime, and verdict.
- `git-status-before.txt`: Working tree status prior to smoke execution.
- `git-status-after.txt`: Working tree status after smoke execution.
- `SHA256SUMS`: Checksums for all generated artifacts.
- `checksum-verification.txt`: Independent `/sbin/sha256sum -c` output.
