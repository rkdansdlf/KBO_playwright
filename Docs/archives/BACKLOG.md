# Backlog
(All items completed)

## Adaptive Rate-Limiting & User-Agent Rotation [DONE]
- **Goal**: prevent throttling/blocks while crawling hitter/pitcher/relay flows (`src/crawlers/*.py`).
- **Status**: IMPLEMENTED. Shared `AsyncThrottle` service added with jitter and env var support. UA rotation added to `AsyncPlaywrightPool`.

## Robots.txt & Compliance Checks [DONE]
- **Goal**: ensure long-running crawls remain within koreabaseball.com's published rules.
- **Status**: IMPLEMENTED. Automated fetching, parsing, and snapshot saving of `robots.txt`. All crawlers now check compliance before navigation. Documented in `README.md`.
