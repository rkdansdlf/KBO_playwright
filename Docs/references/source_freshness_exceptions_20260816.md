# Source Freshness Exceptions - 2026-08-16

## Verified Refresh

The read-only source refresh was run with Playwright fallback enabled. There are
40 active sources, 30 refreshed successfully, and 10 currently have no
successful fetch recorded. No failed source was marked successful and no
synthetic snapshot was written.

| Source keys | Observed failure | Policy |
| --- | --- | --- |
| `doosan_bears_events`, `doosan_bears_ticket`, `nc_dinos_events`, `nc_dinos_food_seat`, `nc_dinos_ticket`, `namuwiki_kbo` | HTTP 403 from HTTPX and Playwright | Keep visible as blocked; repair with an approved source-specific access path or manual operator review. |
| `lotte_giants_events`, `lotte_giants_fnb`, `lotte_giants_ticket` | Browser connection closed | Keep active; retry with bounded backoff and investigate endpoint/network changes. |
| `gujangfood_com` | DNS `ERR_NAME_NOT_RESOLVED` | Do not retry indefinitely; verify whether the domain is still authoritative before changing the source URL. |

## Gate Policy

- A failed fetch must not update `last_success_at`, `last_content_hash`, or raw
  snapshot success state.
- Official KBO and official-team sources remain active and visible until a
  replacement source is approved. Their failure is an operational warning,
  not permission to fabricate data.
- Low-reliability third-party sources such as `namuwiki_kbo` may be excluded
  from a blocking production freshness gate only through an explicit source
  contract change. They must remain visible in reports.
- `never_crawled` is a freshness finding, not proof that the source is empty.
- Empty optional tables (`player_milestones`, `futures_game_schedules`,
  `player_splits_stats`, `player_draft_histories`, and realtime stadium tables)
  must be classified separately from source transport failures.

## Next Repair Criteria

Promote a source back to healthy only when a live fetch returns an accepted
status, a non-empty valid payload is parsed, and a raw snapshot is persisted.
Use the source-specific CLI retry before changing registry metadata:

```bash
venv/bin/python -m src.cli.refresh_source_snapshots \
  --source-key <source_key> --max-hours 0 --json
```
