# RAG Identity Phase B — Re-key and Re-index Plan

Last updated: 2026-11-01

Phase B normalizes the RAG index identity so that a chunk's `source_row_id` is
the same key the generator produces today. Until it runs, the store holds two
conventions at once: the natural keys the generator writes, and the legacy
`id=N` keys an older pipeline wrote. That split is why four golden queries
cannot find answers that exist, and why a re-crawl can produce a second chunk
for a row that already has one.

**Numbers below were measured before a host-wide PostgreSQL outage on
2026-11-01 (see `KNOWN_LIMITATIONS.md`). Re-measure before applying.**

## 1. What decides the path

The generator builds each key through a function in
`src/services/rag_index_identity.py`. **The ten sources that have a `stable_*`
function are exactly the ten the re-key tool supports**, because the tool needs
to recompute the same key from source rows:

| `stable_*` function | Source |
| --- | --- |
| `stable_award_source_row_id` | `awards` |
| `stable_team_history_source_row_id` | `team_history` |
| `stable_milestone_source_row_id` | `milestone` |
| `stable_futures_source_row_id` | `futures_schedule` |
| `stable_splits_source_row_id` | `player_splits` |
| `stable_player_movement_source_row_id` | `player_movements` |
| `stable_pbp_source_row_id` | `game_play_by_play` |
| `stable_highlight_source_row_id` | `game_highlights` |
| `stable_team_event_source_row_id` | `team_events` |
| `stable_press_release_source_row_id` | `kbo_press_releases` |

`player_season_batting` and `player_season_pitching` build their keys **inline**
in `build_rag_index._iter_batting_chunks` / `_iter_pitching_chunks`, so they
have no function the census can call:

```python
f"{batting.player_id}_{batting.season}_{batting.team_code or 'NA'}_{batting.league or 'UNKNOWN'}"
```

That is the whole gap. Two functions plus two census record iterators close it.

## 2. Two populations, two different treatments

### A. Re-key (identity swap, embedding reusable) — BLOCKED, see §2-B

Measured: **8,274** rows with `index_status='ACTIVE'` and `source_row_id ~ '^id=[0-9]+$'`,
excluding `game_summary`.

| Source | Rows | Tool support | Re-keyable? |
| --- | --- | --- | --- |
| `player_movements` | 4,503 | present | **no — the id is not the source row's id** |
| `player_season_batting` | 2,572 | added | **no — same reason** |
| `awards` | 486 | present | **no — same reason** |
| `team_history` | 385 | present | **no — same reason** |
| `player_season_pitching` | 328 | added | **no — same reason** |

All 8,274 carry both `content_hash` and `embedding`; `indexed_at` is NULL for
every one of them (the pipeline that wrote them did not stamp it).

**Re-keying is an identity swap, not a re-embed.** The chunk text does not
change when `source_row_id` changes, so `content_hash` stays valid and the
existing vector stays correct, and `embedding_cache` (**311,113 rows /
1,321 MB** under `perplexity/pplx-embed-v1-4b`) would absorb a re-embed anyway.

### B. Why the `id=N` keys cannot be re-keyed (measured 2026-11-02)

**The `N` in `id=N` is not the source row's primary key.** Three sources were
checked against the live data, and in every one the ids name different rows:

| Chunk | Says | `source_table.id = N` says |
| --- | --- | --- |
| `player_movements:id=3901` | 구자욱(외야수) 2022-05-04 부상자 명단 | 권혁경(포수) 2022-07-21 (구자욱 is `id=3814`) |
| `awards:id=130` | 2021 Golden Glove 강민호 | 1987 골든글러브 포수 이만수 |
| `awards:id=131` | 2021 Golden Glove 강백호 | 1987 골든글러브 1루수 김성한 |
| `player_season_batting:id=8207` | 안권수 2021 두산 | **matches** — the exception, not the rule |

The census matched all of them anyway, because `_source_entries` looks a legacy
key up in a map keyed by the source's `id`. Numeric equality is not identity, so
`safe_source_matches: 9,811` were **false matches** and the 860
`safe_rekey_candidates` (awards 485, team_history 375) cannot be trusted either.

**Consequences**

* **Do not run `apply_rag_rekey` on this population.** A false match would move a
  chunk onto another row's natural key and, where that key already exists, merge
  two identities.
* **`TARGET_EXISTS_CONTENT_MISMATCH` (8,951) is not evidence of a duplicate.**
  It compares the chunk against whatever row the id happened to land on. The
  live counts are real; the pairing is not.
* The census improvement that surfaced this (recognising the `id=` prefix) is
  still correct — it is what made the false matches visible instead of silent.

**What would be needed**: a match on the chunk *content* rather than on a numeric
id. The legacy text carries the fields (date, player, team, section for
movements; year, award, player for awards), so the pairing is derivable — but it
is parsing, so it fails loudly on formats it does not recognise and needs its own
verification before any write. That is a piece of work, not a migration step.

**Meanwhile**: 8,274 legacy chunks stay as they are. They are retrievable, their
embeddings are valid, and their text is a summary the current generator cannot
reproduce — deleting them would lose the `[TL;DR]`/`[핵심 문장]`/`[상세]` form
outright. Leaving them costs duplicate hits for the same player-season; removing
them costs information. Neither is free, which is why the decision belongs after
the content-based pairing exists.

### B. Re-index and clean (`#part` split keys)

`REINDEX_REQUIRED` holds **5,589** rows:

| Source | Rows | Key form |
| --- | --- | --- |
| `player_season_batting` | 5,199 | `id=N#partM` |
| `player_season_pitching` | 365 | `id=N#partM` |
| `markdown_docs` | 24 | `/app/docs/kbo_knowledge/...` |
| `kbo_regulations` | 1 | `reg_dummy_001` |

**Nothing in this repository can produce those keys.** `#part` appears in no
`src/` file, and neither does any writer of `REINDEX_REQUIRED` — only the
constant and its consumers exist. They came from an older tool or manual SQL.

The current generator emits **one chunk per source row, unsplit**. So for the
5,564 `id=N#partM` rows the sequence is:

1. Re-index the source rows (the generator writes the natural key).
2. Verify the new chunk carries at least as much as the split it replaces —
   splitting happened because the text was long, so the concatenation must be
   compared, not assumed.
3. Delete the `#part` rows through the lifecycle (`DELETE_PENDING` → `DELETED`),
   never by dropping rows directly.

The 24 `markdown_docs` rows are a **duplicate, not a legacy key**: their key is
the container absolute path (`/app/docs/kbo_knowledge/2025_fa_market.md_0`) while
the same document already sits under the current relative key
(`kbo_knowledge/2025_fa_market.md_0`). They are cleanup. `reg_dummy_001` looks
like a test fixture and needs a decision rather than a migration.

## 3. Expected effect

Four golden queries miss answers that **exist** in the store, and all four are
waiting on the natural keys (measured 2026-11-01, before the outage):

| Query | Expected id | Where it lives now |
| --- | --- | --- |
| `prod_award_018` | `awards:2025_MVP_NONE_폰세` | `awards` chunk `id=N` |
| `prod_award_019` | `awards:2025_골든글러브_투수_폰세` | `awards` chunk `id=N` |
| `prod_history_023` | `team_history:2003_KIA` | `team_history` chunk `id=54` |
| `prod_history_024` | `team_history:2003_HU` | `team_history` chunk `id=253` |

`stable_award_source_row_id` produces `f"{year}_{award_type}_{category or 'NONE'}_{player_name}"`
and `stable_team_history_source_row_id` produces `f"{season}_{team_code}"` — the
golden labels **are** those outputs. The golden set was written against the
post-re-key state, which is why `--require-corpus` reports them as absent.

## 4. Procedure

```bash
# 1. Add the two identity functions (code), then extend the census:
#      src/services/rag_index_identity.py    stable_player_season_{batting,pitching}_source_row_id
#      src/services/rag_identity_census.py   _batting_records / _pitching_records
#      src/cli/rag/census_rag_identity.py    add both to --source choices

# 2. Census (read-only) and inspect the manifest before anything is applied.
./venv/bin/python -m src.cli.rag.census_rag_identity \
    --source awards --source team_history --source player_movements \
    --source player_season_batting --source player_season_pitching \
    --output /tmp/phase_b_manifest.json --fail-on-unsafe

# 3. Apply (dry-run first; the tool writes nothing without --apply).
./venv/bin/python -m src.cli.rag.apply_rag_rekey --manifest /tmp/phase_b_manifest.json
./venv/bin/python -m src.cli.rag.apply_rag_rekey --manifest /tmp/phase_b_manifest.json --apply

# 4. Re-measure. Recall must not fall; the four queries above must resolve.
./venv/bin/python -m src.cli.rag.evaluate_rag_retrieval \
    --dataset Docs/references/rag_golden_queries.json --variant bm25 --top-k 5 --json

# 5. Only the 5,589 split/duplicate rows remain, and they need re-indexing.
```

## 5. Guardrails carried from earlier re-keys

* **`--fail-on-unsafe` is not optional.** A collision (two rows whose source
  fields produce the same natural key) or a target conflict (a natural key that
  already exists) means the swap would merge two identities or create a
  duplicate. Both are reported, neither is auto-resolved.
* **A chunk whose natural key already exists is not a safe re-key.** It is
  already re-keyed; swapping it again duplicates the identity. The census
  exempts a numeric key only when it is *not* among the source's natural keys —
  that exemption is what keeps a completed re-key from being reported as
  `SAFE_REKEY` a second time.
* **Delete through the lifecycle.** `ACTIVE → DELETE_PENDING → DELETED`; a
  direct `ACTIVE → DELETED` is refused by `can_transition`, and a `PURGED` row
  cannot be restored.
* **Do not widen the deletion predicate.** The `embedding IS NULL` scan is the
  only thing separating "legacy key" from "still serving": `game_summary` alone
  holds 68,829 rows that are all legacy-keyed **and** embedded, and they are not
  in scope.

## 6. Preconditions

* **Korean Series ends 2026-11-10.** Re-indexing during the postseason competes
  with live collection for the same database and the same embedding budget.
* **The 2026 postseason gaps close first.** `20261031HTSS0` and the Korean
  Series have no data at all right now, blocked on sources
  (`KNOWN_LIMITATIONS.md`, 2026-11-01). Those are facts the index will need to
  carry once the sources answer.
* **Re-measure every count in this document.** They were taken before a
  host-wide outage; the store may have moved.

## 7. Not in scope

* `game_summary` (68,829 legacy-keyed, embedded, **not an indexed source**).
  It is referenced as the canonical example of a table that must not be swept up
  by a key-shape rule.
* `game_date` column backfill (4% populated: 15,262 of 377,381;
  `game_play_by_play` is 0 of 158,857). The expression index
  `idx_rag_chunks_meta_game_date` already makes the metadata path fast, so this
  is tidiness rather than a fix.
* Korean team names in chunk text (the `2026-07-17 HH 순위` vs "한화" gap). It
  needs a generator change **and** a re-embed, so it belongs to the same window
  but is a separate decision.
