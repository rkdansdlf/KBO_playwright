# PLR0913 Refactor Plan

Last updated: 2026-06-23

`PLR0913` (`too-many-arguments`) is not enabled in the default Ruff profile yet. The initial baseline was 120 violations in `src/`; after the first utility/repository context-object pass, the current baseline is 109. Because many affected functions are crawler, repository, and service interfaces with broad call-site usage, this rule should be introduced through staged refactors rather than blanket signature changes.

## Baseline

Top files by violation count:

| Count | File | Primary pattern |
| ---: | --- | --- |
| 11 | `src/crawlers/game_detail_crawler.py` | Playwright helper payload/context functions |
| 10 | `src/repositories/game_save.py` | save helpers with explicit stat fields |
| 10 | `src/services/relay_recovery_service.py` | relay recovery orchestration helpers |
| 8 | `src/repositories/game_helpers.py` | game/detail persistence helpers |
| 6 | `src/crawlers/relay_crawler.py` | relay extraction/build helpers |
| 6 | `src/repositories/game_relay.py` | relay/PBP repository helpers |
| 5 | `src/services/game_collection_service.py` | collection result/context helpers |

## Refactor Strategy

Use the smallest compatible change per function category:

| Category | Preferred approach | Notes |
| --- | --- | --- |
| Public CLI/crawler entrypoints | Keep signature, add targeted `# noqa: PLR0913` only if external callers rely on keyword args | Do not break CLI smoke tests or documented examples |
| Internal helper functions | Extract a dataclass/context object | Good fit for repeated groups such as `page`, `year`, `series_info`, `policy` |
| Repository save helpers | Accept typed payload dict/model object instead of many scalar fields | Preserve existing repository API until all callers migrate |
| Service orchestration helpers | Introduce `Options`/`Context` dataclass | Avoid deeply nested tuples or opaque dicts |
| Small 6-arg functions | Prefer one local helper extraction over new names if readability improves | Do not refactor purely to satisfy the rule when the call is clearer as-is |

## Batch Order

1. CLI and utility functions with 6 arguments: low-risk cleanup or targeted compatibility `noqa`. Started: `RequestPolicy`, `AsyncPlaywrightPool`, `write_refresh_manifest`, and `derive_stable_game_status` now use config/evidence objects while keeping keyword compatibility.
2. `game_save.py` and `game_helpers.py`: introduce payload/context objects and update tests. Started: canonical game-id, field-change, derived-status, and detail-save helpers are context-based; record replacement and pregame/snapshot helpers remain.
3. `game_detail_crawler.py`: group Playwright crawl context and parsed payload state.
4. `relay_crawler.py`, `game_relay.py`, `relay_recovery_service.py`: share relay context/value objects.
5. `game_collection_service.py`: replace large argument groups with collection request/result dataclasses.

## Verification

For each batch:

- `venv/bin/ruff check --select PLR0913 src/<changed files>`
- Targeted pytest for affected crawler/repository/service tests
- `venv/bin/ruff check src/ tests/ scripts/`
- Full pytest before enabling `PLR0913` in `pyproject.toml`

## Enablement Criteria

Enable `PLR0913` in `pyproject.toml` only when:

- Remaining violations are either eliminated or explicitly justified with targeted `# noqa: PLR0913`.
- Default `ruff check src/ tests/ scripts/` remains 0 errors.
- Full pytest passes.
