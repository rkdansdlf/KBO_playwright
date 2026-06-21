# Blocked Source Registry

Last checked: 2026-06-19

These sources were previously stale because the original URLs were blocked, had TLS issues, or no longer resolved. They should not be treated as parser regressions until the access path below has been checked.

| Source key | Domain | Current issue | Verified behavior | Fallback / next action |
| --- | --- | --- | --- | --- |
| `doosan_bears_events` | event | Python TLS verification fails for Doosan public site | httpx may fail with `CERTIFICATE_VERIFY_FAILED`; Playwright fallback succeeds | Refresh via `refresh_source_snapshots`; Naver notice search remains operational fallback |
| `doosan_bears_ticket` | ticket | Python TLS verification fails and ticket platform is external | httpx may fail with `CERTIFICATE_VERIFY_FAILED`; Playwright fallback succeeds | Refresh via `refresh_source_snapshots`; investigate Interpark endpoint for structured ticket prices |
| `nc_dinos_events` | event | Previous probe returned 403 from old access path | Current base URL fetch succeeds with httpx | Refresh via `refresh_source_snapshots`; NC Naver search query is available for notices |
| `nc_dinos_ticket` | ticket | Old ticket URL returned 404/403 | Current base URL fetch succeeds with httpx | Use KBO ticket map/base page for source freshness; structured ticket prices require NC-specific parser |
| `nc_dinos_food_seat` | food | Old `/stadium/food` URL returned 404 | Updated to `/dinos/stadium.do`, which succeeds with httpx | Static Changwon food seed remains fallback for menu-level details |

Operational guidance:

- Prefer `python3 -m src.cli.refresh_source_snapshots --source-key <key>` to re-check a source before changing crawler selectors.
- If httpx fails but Playwright succeeds, treat the issue as access/TLS friction rather than selector drift.
- If both httpx and Playwright fail, classify the source as access blocked and avoid assigning it to parser maintenance.
