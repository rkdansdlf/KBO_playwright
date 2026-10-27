# Historical Season Import (2001~2007)

How to bring an externally obtained box score set into the database, and why the
repository cannot obtain it by itself.

## 1. Why this is manual

The operational crawlers cannot fill 2001~2007. Measured on 2026-10-27:

| Source | State |
| --- | --- |
| KBO GameCenter | Has the data. `robots.txt` is `User-agent: * / Disallow: /` — every crawler is refused. |
| Naver record / schedule / relay | 404 / `games=0` / empty inning 1. Those seasons were never served. |
| 나무위키 team season docs | Season summaries only — no monthly sub-documents, no box scores. |
| 위키백과 | Standings and win-loss tables only (`build_season_anchors.py` level). |
| 스태티즈 | `Allow: /` and `Disallow: /` both present for `*`; RFC 9309 takes the more restrictive rule, and AI crawlers are named and refused. |
| OASIS (nl.go.kr web archive) | `oasis.nl.go.kr` is unreachable. |
| Wayback Machine | 48 game pages exist (`schedule/box/{game_id}.asp`), 2003: 3, 2004: 45. |
| 야구나라 | Game pages exist but the oldest is 2025-05-01. |
| LOPES `KBO_light.csv` | Not usable: `lopes.hufs.ac.kr` is unreachable and the GitHub repository covers **2008~2022** only. |
| Naver News Search API | `401` — the key in `.env` has no news-search scope. |
| 공공데이터포털 / 문화빅데이터 | No game-level box scores. |

So the data has to come from outside: the printed 『한국야구연감』 (library
reading room, OCR), a Sports2i licensed dump, or news box-score text (BigKinds).
Whichever route is used, the repository-side work is the same.

**Scope note.** 2,665 regular-season games (2001~2007) plus 47 exhibition games
(2010) and 4,598 games whose Naver relay was never served are missing. This
runbook covers the box scores; the relay-missing games stay missing.

## 2. What to deliver

Four CSV files, UTF-8, with a header row. Only `games` is required — supply the
others when the source has them.

### games.csv (required)

| Column | Notes |
| --- | --- |
| `game_id` | `YYYYMMDD` + away + home + DH. Must start with the season. |
| `game_date` | `YYYY-MM-DD`. |
| `away_team`, `home_team` | KBO team codes. |
| `away_score`, `home_score` | **Required.** The checks below need them. |
| `stadium` | Optional. |
| `game_status` | Defaults to `COMPLETED` on ingest. |

### innings.csv (optional, strongly recommended)

| Column | Notes |
| --- | --- |
| `game_id` | Must exist in games.csv. |
| `team_side` | `away` or `home`. |
| `inning` | Integer. |
| `runs` | Integer. |

### batting.csv / pitching.csv (optional)

`batting`: `game_id`, `team_side`, `player_id`, `player_name`, `team_code`,
`appearance_seq`, `pa`, `ab`, `hits`, `hr`, `rbi`, `bb`.

`pitching`: `game_id`, `team_side`, `player_id`, `player_name`, `team_code`,
`appearance_seq`, `so`.

Unrecognised columns are ignored by the converter and by the ingestor. Columns
the ingestor does not read *yet* (doubles, triples, stolen bases, earned runs)
should still be kept in the CSV: extend the ingestor when the source supplies
them rather than re-importing later.

## 3. Convert and validate

```bash
python3 -m scripts.maintenance.import_season_boxscores \
  --season 2005 \
  --games   data/incoming/2005_games.csv \
  --innings data/incoming/2005_innings.csv \
  --batting data/incoming/2005_batting.csv \
  --pitching data/incoming/2005_pitching.csv \
  --out-dir data/archives \
  --source-name kbo_yearbook_ocr \
  --source-url "국립중앙도서관 한국야구연감 2005" \
  --authorization-ref "reading-room 2026-11-03"
```

It writes `data/archives/kbo_2005.json` and `kbo_2005.manifest.json`. The digest
is computed from the file on disk, so the manifest always describes bytes that
exist.

### What the converter refuses

| Check | Why |
| --- | --- |
| inning total ≠ final score | This is what a mis-scanned table looks like, and a stored box score becomes the evidence for every later aggregate. Only games with innings in the file are checked. |
| `game_id` outside the target season | The ingestor writes by `game_id`; a wrong one edits another season. |
| same `game_id` twice | The second row would silently win. |
| missing final score | Without one the file cannot be checked. |
| inning/batting/pitching row whose game is absent from games.csv | The ingestor would insert an orphan. |
| `team_side` other than `away`/`home` | The column drives which side a row is filed under. |
| `authorization_ref` empty or a placeholder (`unknown`, `n/a`, …) | The manifest is a provenance record; "unknown" records nothing. |

A refusal prints the reason and exits 1. Nothing is written on refusal.

## 4. Load

```bash
python3 -m src.cli.collection.ingest_historical_archive \
  --file data/archives/kbo_2005.json \
  --manifest data/archives/kbo_2005.manifest.json \
  --season 2005 \
  --dry-run --json
```

Drop `--dry-run` once the summary looks right. The manifest is **required** for
every season ≥ 1982 (`_load_provenance` returns `None` without one, and the CLI
exits 2). The ingestor re-validates the digest, the season, the game ids, and
rejects `data_class == "synthetic_fixture"` or `verified is False`.

The provenance lands on each game's `game_metadata.source_payload.provenance`.

## 5. After loading

Inning and lineup chunks are generated by `--source inning_scores` and
`--source lineups`. Because the ingest writes real 2001~2007 rows, the next
index run picks them up incrementally — nothing already embedded changes.

Row counts to expect, from the dry-run summary: `games_ingested`,
`innings_records_ingested`, `game_batting_records_ingested`,
`game_pitching_records_ingested`.

## 6. What is not covered

* 시즌 누적 tables (`player_season_*`, `team_season_*`) are not produced here:
  a box score file does not state season totals, and deriving them from a
  partial set of games would publish a wrong total. Recalculate after the games
  exist (`recalc_player_stats`, `recalc_team_stats`).
* Play-by-play is not covered. Neither the yearbook nor Sports2i's box-score
  dump necessarily carries it; check what the source provides before assuming.
* 4,598 relay-missing games (2016~2021 mostly) are a separate gap — Naver never
  served those relays and no alternative source has been found for them.
