# Gate 106D: Controlled Live Read-Only Smoke Certification

**Phase**: Phase 106D
**Execution Timestamp**: 2026-09-01T03:35:00+09:00
**Scope**: 2 KBO Browser Targets + 1 Secondary Wikipedia HTTP Target
**Isolation Policy**: STRICT READ-ONLY | ZERO Database Persistence | ZERO Oracle/Production DML
**Protected DB SHA-256**: `f7a7c122ce9656de47957ebfca662d736418fc4ca7e8f0d2255690a1f64bbe30` (100% Unchanged)

---

## 1. Summary of Executed Targets

| Target ID | Protocol | Live Target URL | Observed Metric | Status |
| :--- | :---: | :--- | :---: | :---: |
| `player-search-pagination-contract` | Playwright Browser DOM | `https://www.koreabaseball.com/Player/Search.aspx?searchWord=%25` | 20 player rows parsed, next button DOM element verified | **PASS** |
| `player-stats-basic2-headers` | Playwright Browser DOM | `https://www.koreabaseball.com/Record/Player/HitterBasic/Basic2.aspx` | 11/11 Basic2 headers matched (`BB`, `IBB`, `SO`, `OPS`, etc.) | **PASS** |
| `wikipedia-awards-live` | HTTP HTML via httpx (Secondary Authority) | `https://ko.wikipedia.org/wiki/KBO_MVP` | 495 award records parsed across 6 categories | **PASS** |

---

## 2. Network & Browser Audit

- **Top-Level Navigations**: 2 (Budget limit: 3)
- **Total Outbound Requests**: 87 (Allowed: 52, Blocked by Policy: 35)
- **Unexpected Hosts**: 0 (Observed hosts: `www.koreabaseball.com`, `6ptotvmi5753.edge.naverncp.com`, `ko.wikipedia.org`, `www.googletagmanager.com`)
- **Browser Page Errors**: 0 errors on KBO target origin
- **Chrome WebUI Warnings**: Classified as `BROWSER_INTERNAL_WEBUI_WARNING` (gate impact: NONE)
- **Database Persistence**: 0 writes (all parsed payloads evaluated in-memory and discarded)
