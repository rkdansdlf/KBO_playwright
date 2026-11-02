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
id. **Measured 2026-11-02 — the pairing is derivable, and the parse is exact.**

The legacy text carries every field the source row keys on. Parsed over the whole
population, with zero writes:

| Source | Rows | Matched | What was needed |
| --- | --- | --- | --- |
| `player_movements` | 4,503 | **4,502 (100.0%)** | `(date, player, section)`, then the chunk's team name to split the 643 twins |
| `team_history` | 385 | **324 (84.2%)** | `(season, team_name)` — no name→code table needed, the source has the name |
| `awards` | 486 | needs 3 aliases | content says `Golden Glove`/`Rookie of the Year`/`MVP`; the column says `골든글러브`/`신인상`/`MVP` |

**Parse failures were 0 in every source.** The failures are all *matching*, not
parsing:

* **`player_movements`**: one chunk of 4,503 did not parse. 643 rows matched two
  source rows because a franchise appears under both its era code and its current
  one (`HT` 해태 and `KIA` 기아), and the chunk names the team
  (`팀: 해태 타이거즈`) — the team name resolves all 643, leaving **1**.
* **`team_history`**: 61 chunks name a short form (`삼미`) while the source column
  holds `삼미\n슈퍼스타즈`. Normalising the source name (split on whitespace, take
  the first token) should close most of the gap; this is unverified.
* **`awards`**: only three award names appear in the legacy text, while the table
  holds six kinds. The three missing ones (`수비상`, `올스타전MVP`,
  `한국시리즈MVP`) have no legacy chunks at all — that generator predates them.

**Still open**: `player_season_batting`/`pitching` (4,437 chunks). Their key ends
in `_{league}`, and the legacy text does not carry the league — the identifier is
recoverable (`[핵심 문장] 2021년 두산 베어스 안권수` gives year, team name, player
name) but a player-season can hold several leagues, so the row is *narrowed*
rather than *identified*. Treat these two as a separate question from the three
above, where the pairing is exact.

**The tool now exists** — `src/cli/rag/match_legacy_content.py` with its pure
matching in `src/services/rag_legacy_content_match.py`. It reproduces the counts
and adds the verdict each pairing produces (measured 2026-11-02, read-only):

| Source | Legacy | Matched | Verdict |
| --- | --- | --- | --- |
| `player_movements` | 4,503 | **4,503 (100%)** | **DUPLICATE 4,503** |
| `awards` | 486 | 410 | **REKEY 410** |
| `team_history` | 385 | 385 | REKEY 375 · DUPLICATE 10 |

`team_history` reached **385/385** once the source column's embedded newline
(`삼미\n슈퍼스타즈`) was normalised — the earlier 324 was a matching artifact, not
a data gap.

**What the verdicts mean**

* **`REKEY` 785** (awards 410 + team_history 375): the natural key is free, so
  the identity can move onto it. This is the population `apply_rag_rekey` may be
  pointed at, and it is under a tenth of what the census claimed.
* **`DUPLICATE` 4,513** (movements 4,503 + team_history 10): the natural key
  already exists, so the legacy chunk is a second copy of an indexed row.
  Re-keying would merge two identities and is refused. Whether to delete them is
  the `[TL;DR]` question in §2-A, unchanged.

Two matching gaps closed on 2026-11-02: one movement chunk was written without a
team field (the field is now optional, since the team only splits twins), and the
bulk of the awards misses were shortened foreign names (`로하스` for
`멜 로하스 주니어`), which a same-year, same-award substring fallback resolves
when exactly one row contains the name.

**Still unexamined — 76 awards.** These are not name shortenings:
`(2021, 골든글러브, 구자욱)` has the right year and award, and the source table has
no such winner. Either the source table is incomplete or those chunks came from a
different snapshot. This does not block the 785 re-keys (they matched exactly),
but it does mean **no count here should be described as covering the whole
awards population**.

### The manifest exists, and the applier accepts it (2026-11-02)

`apply_rag_rekey` was blocked on a manifest the census could not produce, because
its entries were false matches. `match_legacy_content --output` now writes one
from the content pairing, translating the verdicts into the applier's vocabulary
(`REKEY` → `SAFE_REKEY`, `DUPLICATE` → `TARGET_EXISTS_CONTENT_MISMATCH`).

The manifest structure lives in `src/services/rag_rekey_manifest.py`, shared with
the census. Two things were fixed on the way:

* `disposition_counts` is derived from `entries` rather than accepted as an
  argument — two sources for one number is how a manifest disagrees with itself.
* `git_commit_sha` was resolved through a hardcoded `/usr/bin/git`, which does not
  exist on the Windows host the scheduler runs on, so every manifest generated
  there carried `"unknown"` and the applier refused it. It now resolves `git` from
  `PATH`.

Measured, read-only: `match_legacy_content --source awards --output m.json` then
`apply_rag_rekey --manifest m.json` returned
**`status=SUCCESS_APPLIED mode=dry-run rekeyed=410`**. The applier validates the
body hash, the commit, and the entry count before touching anything, so a
manifest it accepts is one whose structure is sound — that is the precondition
`--apply` needs, and it is now met.

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
