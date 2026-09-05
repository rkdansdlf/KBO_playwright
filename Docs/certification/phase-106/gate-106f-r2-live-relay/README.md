# Gate R2: Limited Live Relay Smoke Certification (Remediated)

## 1. Overview & Operational Context
- **Gate**: `GATE_R2_LIMITED_LIVE_RELAY_SMOKE_REMEDIATED`
- **Operational Label**: 종료된 역사 경기의 라이브 엔드포인트 read-only smoke
- **Target Game**: `20240930NCHT0` (KBO) / `20240930NCHT02024` (Naver)
  - **Date**: 2024-09-30 (2024 Regular season finale, Gwangju-Kia Champions Field)
  - **Matchup**: NC (5) at KIA (10)
  - **Status**: `COMPLETED`
  - **Comparison Scope**: Inning 9 top (`TERMINAL_HALF_INNING`)
- **Primary Operational Goal**: Validate live remote response structures of KBO and Naver text relay endpoints under strict, auditable request budgets without mutating local storage or connecting to production databases.

---

## 2. Key Remediation Pillars (Gate R2-R1 ~ R2-R5)
1. **R2-R1: KBO Canonical URL Contract with Provenance**:
   - Resolved via immutable `KboRelayTarget` (`src/utils/kbo_relay_target.py`).
   - Provenance: `verified_target_fixture`.
   - Canonical URL: `https://www.koreabaseball.com/Game/LiveText.aspx?leagueId=1&seriesId=0&gameId=20240930NCHT0&gyear=2024`.
   - Enforced single path of URL generation across the entire codebase. Prohibited global hardcoded `seriesId=0`.
2. **R2-R2: Dual-Source Live Relay Smoke Verified (Completed Game Smoke)**:
   - Target: Completed historical match `20240930NCHT0` (top of 9th).
   - Discovery probe explicitly recorded in `discovery-probe-ledger.jsonl` (`DISCOVERY_PROBE NON_CERTIFYING`).
   - Both live endpoints reached and parsed under approved budget constraints (1 poll each, 0 DB mutations, 1 concurrency).
3. **R2-R3: Exhaustive Raw Data Reconciliation (Both KBO DOM and Naver Options)**:
   - **Naver Options Equation**:
     $$\text{Raw Options} = \text{Events} (5) + \text{Commentary} (31) + \text{Headers} (3) + \text{Duplicates} (0) + \text{Unclassified} (0)$$
     Verified `unclassified_rows = 0`.
   - **KBO DOM Nodes Equation**:
     $$\text{Raw Leaf DOM Nodes} = \text{Events} (5) + \text{Commentary} (29) + \text{Headers} (7) + \text{Structural} (0) + \text{Unclassified} (0)$$
     Verified `unclassified_nodes = 0`.
4. **R2-R4: Real Dual-Source Match Grouping & Invariant Checks**:
   - 1:1 Match groups formed with primary class `MATCHED_EXACT: 5`, `kbo_only: 0`, `naver_only: 0`.
   - 23-field provenance matrix audited in `field-provenance-matrix.json`.
   - Baseball domain invariants: outs monotonic $(0 \to 1 \to 2 \to 2 \to 3)$, score consistency, WE continuity $\to$ 0 failures.
5. **R2-R5: Code \to Evidence Strict Commit Isolation & Dynamic File Management**:
   - `C_R2_CODE` $\to$ certifying run in clean state $\to$ `C_R2_EVIDENCE`.
   - Dynamic tracking of all evidence payload files under `SHA256SUMS`.
