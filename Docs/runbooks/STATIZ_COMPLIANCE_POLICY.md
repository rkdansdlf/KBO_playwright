# STATIZ Compliance & Data Collection Policy

## 1. Background & Legal Context

On **April 2, 2026**, STATIZ published an explicit terms of service notice prohibiting all automated web crawling and scraping:
> **"모든 크롤링(Crawling) 행위 금지"** (Prohibition of all automated crawling activities)

Under repository policy and intellectual property compliance standards:
- STATIZ is **NOT** an authoritative primary source for KBO data. KBO official channels (`koreabaseball.com`) serve as the authoritative system of record.
- Automated data extraction from STATIZ without explicit written permission or a valid commercial/academic licensing contract is strictly prohibited.
- Technical scraping barriers and legal risks necessitate a fail-closed governance model.

## 2. Fail-Closed Policy Enforcement

To ensure that no crawler worker or CI job accidentally accesses STATIZ:

1. **Default Exclusion**:
   - `ExternalStatsCrawler` defaults exclusively to `providers=("fangraphs",)`.
   - STATIZ is completely bypassed in standard scheduled pipelines and DAG tasks.

2. **Dual-Key Safety Gate**:
   STATIZ collection can ONLY be initiated when **BOTH** conditions are satisfied in the runtime environment:
   - `KBO_ALLOW_STATIZ_CRAWL=1`: Explicit operator opt-in.
   - `STATIZ_LICENSE_AGREEMENT_REF=<ID>`: An auditable reference identifier to a verified written licensing agreement or contract (e.g., `LIC-STATIZ-2026-001` or internal legal ticket reference).

   If `KBO_ALLOW_STATIZ_CRAWL=1` is present but `STATIZ_LICENSE_AGREEMENT_REF` is absent or whitespace, execution **fails closed** with a compliance error.

3. **Test Fixtures & Local Mocking**:
   - In automated unit/integration tests, `allow_statiz=True` may be passed directly to `ExternalStatsCrawler.crawl(...)` alongside mock HTTP fixtures (`AsyncMock` returning offline HTML).
   - Live network requests to `statiz.co.kr` are blocked in CI via `test_playwright_blocking.py` and environment isolation.

## 3. Operational Checklist for Operators

Before enabling STATIZ data collection:
1. [ ] Confirm that a written contract, data partnership agreement, or written license from the STATIZ site operator is signed and archived.
2. [ ] Record the agreement identifier in repository credentials or secret management (e.g., `STATIZ_LICENSE_AGREEMENT_REF=CONTRACT-YYYYMMDD-REV`).
3. [ ] Set `KBO_ALLOW_STATIZ_CRAWL=1` in the deployment environment.
4. [ ] Configure conservative rate limits (minimum 2.0s delay) in `RequestPolicyConfig` to respect host resources.
