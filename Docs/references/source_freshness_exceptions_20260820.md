# Source Freshness Exceptions - 2026-08-20

## Current Census

- Active sources: 40.
- Refresh run: 40 sources selected at 2026-08-20 22:23 local time with `--max-hours 48`.
- Refresh result: 30 saved successfully; 10 remained failed.
- Current stale sources: 0 under the 48-hour diagnostic threshold.
- Current never-crawled sources: 10.
- No failed source was marked successful and no synthetic snapshot was written.

## Never-Crawled Classification

| Source keys | Classification | Observed failure | Current policy |
| --- | --- | --- | --- |
| `doosan_bears_events`, `doosan_bears_ticket`, `lotte_giants_events`, `lotte_giants_fnb`, `lotte_giants_ticket`, `nc_dinos_events`, `nc_dinos_food_seat`, `nc_dinos_ticket` | Official-team operational sources | HTTP 403 for Doosan/NC; browser connection closed for Lotte | Keep visible as unresolved operational findings. Do not fabricate data or change registry success fields. |
| `namuwiki_kbo`, `gujangfood_com` | Third-party or low-reliability enrichment | HTTP 403 for NamuWiki; DNS name resolution failure for GuJangFood | Candidates for a non-blocking contract, but no blocking exemption is active until an explicit source contract change is reviewed. |

## Table Freshness Separation

The health check now reports required and optional empty-table counts separately.
The current production snapshot has zero required-table issues and five optional
table issues (`player_milestones`, `player_splits_stats`,
`player_draft_histories`, `stadium_transit_times`, and `stadium_congestion`).
These table findings are distinct from source transport failures.

## Recovery Criteria

Promote a source back to healthy only when a live fetch returns an accepted
status, a non-empty valid payload is parsed, and a raw snapshot is persisted.
Use the source-specific retry before changing registry metadata:

```bash
venv/bin/python -m src.cli.refresh_source_snapshots \
  --source-key <source_key> --max-hours 0 --json
```
