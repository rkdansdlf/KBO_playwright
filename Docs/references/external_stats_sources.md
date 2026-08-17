# External Season Statistics

The repository supports opt-in season-stat imports from the FanGraphs KBO data
endpoint and STATIZ season tables. These providers are supplemental sources;
official KBO season rows remain the primary aggregates.

## Commands

Register the source metadata first:

```bash
python3 -m src.cli.seed_data_sources
```

Parse FanGraphs without database writes:

```bash
python3 -m src.cli.crawl_external_stats \
  --season 2025 --provider fangraphs --dry-run
```

Persist provider rows, project linked metrics, and rebuild rankings using the
explicit provider:

```bash
python3 -m src.cli.crawl_external_stats \
  --season 2025 --provider fangraphs --save --project --rebuild-rankings
```

STATIZ collection requires an authorized session when the site presents its
login page. The adapter reports that response as an access failure and never
attempts a browser or anti-bot bypass. An authorized session cookie may be
provided through the `STATIZ_COOKIE` environment variable; never commit it or
place it in source snapshots.

## Storage

`external_season_stats` keeps one normalized provider row with:

- provider, stat type, season, league, and level
- provider player ID and source player/team labels
- season-aware canonical team code and conservative KBO player ID link
- provider metric JSON and parser version
- source URL, response hash, fetch time, and resolution status

Linked rows can be projected into the existing season aggregate as:

```text
player_season_batting.extra_stats.external_sources.fangraphs
player_season_pitching.extra_stats.external_sources.statiz
```

Projection does not overwrite official flat metrics. Ranking rebuilds only use
an external provider when `--external-provider` is explicitly supplied.

## Metric Notes

- Percent fields such as `BB%` and `K%` retain the displayed percentage value.
- Baseball innings such as `120.2` are normalized to `innings_outs=362` and
  `innings_pitched=120.666...`.
- FanGraphs bilingual names use the Korean display name for player matching.
- FanGraphs KBO team labels such as `Bears (KBO)` are mapped to season-aware
  KBO team codes.

## Compliance

The four external `DataSource` entries are seeded inactive pending terms,
access, and redistribution review. Raw HTML archival is disabled unless
`--capture-raw` is supplied. Do not expose or redistribute provider content
without reviewing the applicable terms and attribution requirements.
