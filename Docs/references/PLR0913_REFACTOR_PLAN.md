# PLR0913 Refactor Plan

Last updated: 2026-07-06

**Status: COMPLETE** — `PLR0913` is enabled in `pyproject.toml` select and all `src/` violations have been eliminated (0 violations).

## Final Summary

| Phase | Scope | Violations |
| --- | --- | --- |
| Initial baseline | `src/` crawlers, repositories, services | 120 |
| Batch 1 | CLI/utility + `RequestPolicy`, `AsyncPlaywrightPool`, `write_refresh_manifest`, `derive_stable_game_status` | 109 |
| Batch 2 | `game_save.py`, `game_helpers.py` payload/context objects | ~90 |
| Batch 3 | `game_detail_crawler.py` Playwright context helpers | ~76 |
| Batch 4 | `relay_crawler.py`, `game_relay.py`, `relay_recovery_service.py` | ~64 |
| Batch 5 | `game_collection_service.py` request/result dataclasses | ~52 |
| Phase 15 | `team_stat_aggregator.py` → `TeamAggregationQuery` dataclass | ~41 |
| Phase 16 | `aggregators/`, `crawlers/`, `relay_validation/` PLR0913 cleanup | 109 → 0 |
| Phase 16-4 | `game_deduplication_service.py` → `_CandidateQuery` dataclass | **0** |

## Approach

All refactors followed the strategy documented in the original plan:
- Public CLI/crawler entrypoints: kept signatures, added targeted `# noqa: PLR0913` only where external callers relied on keyword args.
- Internal helpers: extracted dataclass/context objects (`TeamAggregationQuery`, `_CandidateQuery`, etc.).
- Repository save helpers: accept typed payload dict/model objects instead of many scalar fields.
- Service orchestration helpers: introduced `Options`/`Context` dataclasses.

## Verification

- `ruff check --select PLR0913 src/` = 0 violations
- `ruff check src/ tests/ scripts/` = 0 errors
- `pytest` = 8547+ passed in recent full-suite runs
