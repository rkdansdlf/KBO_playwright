# RAG Identity Phase B — Re-key and Re-index Plan

Last updated: 2026-11-02 (awards 76 examined)

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
`src/services/rag_index_identity.py`. **Every source that has a `stable_*`
function is a source the re-key tool can help with**, because the tool needs
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
| `stable_player_season_batting_source_row_id` | `player_season_batting` |
| `stable_player_season_pitching_source_row_id` | `player_season_pitching` |

The last two rows were the gap until 2026-11-02: they built their keys **inline**
in `build_rag_index._iter_batting_chunks` / `_iter_pitching_chunks`, so the census
had no function to call and both sources were invisible to it:

```python
f"{batting.player_id}_{batting.season}_{batting.team_code or 'NA'}_{batting.league or 'UNKNOWN'}"
```

Both now route through the shared helpers, so the twelve functions and the
twelve census sources agree again. They were added for completeness rather than
for this phase: the content matcher pairs those two sources' legacy chunks
through their text (§2-B), not through these functions, and it is the matcher's
verdict that decides what happens to them.

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
| `awards` | 486 | **325** | **REKEY 325** (2026-11-03, franchise 대조 후) |
| `team_history` | 385 | 385 | REKEY 375 · DUPLICATE 10 |

`team_history` reached **385/385** once the source column's embedded newline
(`삼미\n슈퍼스타즈`) was normalised — the earlier 324 was a matching artifact, not
a data gap.

**What the verdicts mean**

* **`REKEY` 700** (awards 325 + team_history 375, both applied): the natural key is free, so
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

**Examined 2026-11-02 — the source table is the incomplete side.** The 76 are not
stale chunks naming dead players; they name winners the table does not hold:

| Years | `awards` rows / year | `외야수` rows | Chunk claims |
| --- | --- | --- | --- |
| 1982~2021 (40 years) | **8** | **1** | 3 outfielders |
| 2022~2025 (4 years) | 10 | 3 | 3 outfielders |

Every one of the 40 deficient years holds a single outfield row where the league
awards three, so roughly **80 winners are absent from the source** — and the
legacy chunks carry them. `(2021, 골든글러브, 구자욱)` is one of those two missing
outfielders, not an error in the chunk.

The natural key is still derivable for these, because the chunk holds all four
fields it composes from (`시즌`, `수상 종류`, `선수`, `포지션`) — but `포지션` is an
English code (`P`/`C`/`1B`/`OF`/`DH`) and `category` is Korean (`투수`/`포수`/`1루수`/`외야수`/`지명 타자`), so the translation is required first. Re-keying them before the
source table is completed would leave identities the generator cannot reproduce:
when those winners are eventually crawled, the row the generator writes must hash
to the same key, which it only does if both sides agree on the category.

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
**`status=SUCCESS_APPLIED mode=dry-run rekeyed=325`** (awards, after the franchise fix below).

> [!WARNING]
> **Safety Re-evaluation & Rekey Hold Notice (2026-11-03)**:
> Dry-run success verifies JSON schema structure and chunk presence, but does
> **not** constitute authorization for live writes (`--apply`).
>
> 1. **Content Proof v1 Required**: `apply_rag_rekey --apply` now strictly enforces
>    `match_schema_version: "content-proof-v1"`. Every `SAFE_REKEY` entry must
>    carry verified evidence: `matched_source_pk`, `proof.matched_fields`,
>    `proof.source_snapshot_hash`, and `proof.chunk_content_hash`. Legacy numeric
>    census manifests lacking proof are unconditionally rejected (exit 2).
> 2. **Fail-Closed Write Gates**: The applier verifies candidate count == 1,
>    verifies that target natural keys are not already occupied in `rag_chunks`,
>    and validates CAS chunk content hashes.
> 3. **Matcher Precision**: Movement, award, and team history matchers now reject
>    single-candidate mismatches (`FIELD_CONFLICT`) on team, position/category,
>    ranking, and city.
> 4. **Execution Status**: Live `--apply` remains **BLOCKED** until formal sample
>    audits (30+ boundary samples per source) and cross-store (sparse/vector)
>    consistency checks are completed. Do not execute unverified rekey runs.

**Meanwhile**: 8,274 legacy chunks stay as they are. They are retrievable, their
embeddings are valid, and their text is a summary the current generator cannot
reproduce — deleting them would lose the `[TL;DR]`/`[핵심 문장]`/`[상세]` form
outright. Leaving them costs duplicate hits for the same player-season; removing
them costs information. Neither is free, which is why the decision belongs after
the content-based pairing exists.

### C. Re-index and clean (`#part` split keys)

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

## 8. Pre-flight re-measurement and the full content comparison (2026-11-02)

The numbers the plan was built on were re-measured after the 2026-11-01 host-wide
PostgreSQL outage, and **all of them reproduce unchanged**: 8,274 ACTIVE legacy
`id=N` chunks (embedding and `content_hash` present, `indexed_at` NULL for every
one), 5,564 `id=N#partM` split rows, `REINDEX_REQUIRED` 5,589. The outage moved
nothing.

**The §2-B conclusion is confirmed and strengthened by a full comparison.** The
1,790 split groups (5,199 parts) carry a `[META]` JSON in their last part with the
real identity — `player_id`, `season`, `team` (Korean name mapped to the code) — so
the natural key each group *should* have is derivable from the chunk itself.
Results across all 1,790 groups:

| Outcome | Groups |
| --- | --- |
| Natural key exists (ACTIVE) | **1,371** |
| Natural key does not exist — the split rows are the only copy | **248** |
| `[META]` missing or unparseable | 171 |

**For every one of the 1,371 matched groups the natural-key chunk is shorter than
the split rows it would replace — 100% of them.** Worked example, 마해영 2002
Korean Series (6 games, 0.458):

* natural key `95549_2002_SS_KOREAN_SERIES`: **135 chars** — one summary line
* split rows `id=1096#part1+#part2`: **~190 chars** — `[TL;DR]` + `[핵심 문장]` +
  `[상세]` with ISO/BABIP/XR; `#part3` is a `[META]` JSON with `pa`, `wrc_plus`,
  `aliases` that the generator's `meta` does not carry

So plan step 2 ("verify the new chunk carries at least as much as the split it
replaces") **fails for 100% of the matched population**, and step 3 (delete the
`#part` rows) would destroy information in every one of them. The 248 unmatched
groups would be deleted outright. The 171 unparseable ones cannot even be judged.

**Phase B's cleanup stage is therefore cancelled, not deferred.** The direction
that loses nothing is the other way round: make the generator emit the rich form
(`[TL;DR]` / `[핵심 문장]` / `[상세]` / `[META]`) under the natural key, re-index,
and *then* the split rows are true duplicates whose deletion is safe. Until that
exists, both populations stay as they are: retrievable, embedded, duplicated for
the same player-season — which the search already tolerates.

(`[META]` parsing detail: the JSON is followed by a footer line
(`… / 기준일 2026-04-19`), so it must be read with `json.JSONDecoder().raw_decode`,
not `json.loads` — a plain `loads` rejects 100% of them as "Extra data".)

## 9. P1/P7 outcome — 700 chunks re-keyed, measured (2026-11-03)

**Applied**: 375 `team_history` + 325 `awards` = **700 identities moved**. Every
apply ran through the content-proof manifest; none rolled back.

**`awards` went 49 → 325 matched.** The 361 `field_conflict` rows were re-examined
and **289 were not conflicts at all** — the chunk writes an era code where the
table writes a Korean name, and a string comparison reads one club as two:

| chunk | table | same franchise |
| --- | --- | --- |
| `HT` | `KIA 타이거즈` | 5 |
| `WO` | `넥센` | 11 |
| `CB` | `청보` | 6 |
| `TP` | `태평양` | 6 |

`team_franchise_ids` resolves each label through `franchise_id_for_team_code`,
whose era table carries `NX`/`CB`/`TP` that `team_franchises` omits — so a resolver
reading only that table returns nothing for them and the pair reads as a conflict.
A **precomputed map does not work** either: the chunk's label is unknown until match
time, and a code the map lacks compares as "no team" (the first implementation lost
all 289 that way). The remaining 29 were `kt wiz`/`kt 위즈`, now an alias in
`TEAM_NAME_TO_CODE`. Position spellings also fold (`지명 타자`/`지명타자`, and
`좌익수`/`우익수`/`중견수` → `외야수`).

**36 conflicts survive, and they are real**: the chunk says `외야수` where the row
says `지명 타자`. Normalization cannot decide those — the source needs re-checking
(§2-C's 1982~2021 outfield gap is the same table).

**Golden set, before → after the 700:**

| | before | after |
| --- | --- | --- |
| Recall@5 | 0.8128 | **0.8795** |
| MRR | 0.7178 | **0.7844** |
| hit_rate | 0.8333 | **0.9000** |
| precision@5 | 0.1933 | 0.2067 |
| p95 | 1,071 ms | **405 ms** |
| references absent from the store | 5 | **1** |

The one remaining missing reference is `awards:2025_MVP_NONE_폰세`: **no legacy MVP
chunk exists for 2025** (the legacy generator wrote Golden Glove only that year), so
it can only be satisfied by running the awards generator — which is §2-C's
direction, the generator emitting the rich form.

**Legacy population after**: `game_summary` 68,827 (out of scope) ·
`player_movements` 4,503 (all DUPLICATE) · `player_season_batting` 2,572 ·
`awards` **112** (36 conflicts + 76 source gaps) · `player_season_pitching` 328 ·
`team_history` 10 (DUPLICATE).

**Still unapplied**: the 10 `team_history` DUPLICATE rows. `TARGET_EXISTS_CONTENT_MISMATCH`
means the natural key is already indexed, so re-keying would merge two identities;
whether to delete them is §2-C's rich-form question.

## 10. P4 outcome — the awards source was wrong, not the matcher (2026-11-04)

The 36 `field_conflict` rows §9 left behind were not a matcher problem. The
**crawler had been dropping cells**, and re-crawling fixed what matching could
not.

**The defect**: the 위키 골든글러브 table declares ``외야수`` and ``지명 타자``
with **`colspan=4` each** while its data rows carry **twelve cells**. The crawler
took the header's *cell* count (9) as the grid width and truncated there, so the
third outfielder and the DH were dropped and the second outfielder was labelled
``지명 타자`` (that header sat at index 8). Measured: 1982~2021 stored **8 rows a
year** with one outfielder; 2022~2025 stored 10 because 야구나라 filled them.
**About 80 rows across 40 seasons were missing.** The other three tables
(MVP 6/6, 신인상 6/6, 수비상 10/10) and the draft tables are unaffected — none of
them use `colspan`.

The fix expands a header label across the columns it spans, and that is **all** it
does. A data-width guard and header padding were written first and reverted —
mutations could not kill them, because `_render_table` already truncates and pads
every row to the grid width, and widening `headers` would have changed
`draft_wiki_crawler`'s pick-order arithmetic, which reads `len(headers)`.

**The second defect**: `_dedup` compared the category verbatim, so the two sources'
spellings let the same winner through twice. ``지명 타자`` (위키) vs ``지명타자``
(야구나라) — 4 rows, 2022~2025, each with a different `team_name` too, so
`uq_award_record` did not catch them. And 야구나라 carries no position for MVP and
신인상, so those rows entered as `category=None` and split from Wikipedia's —
5 rows (2022 이정후·정철원, 2023 문동주, 2024 김도영·김택연). Both are fixed at
dedup, folding **spaces only**: 수비상's 좌익수/중견수/우익수 stay distinct because
the source distinguishes them (the matcher folds them later because the legacy
chunks say only ``OF``; that is a comparison rule, not a storage rule).

**The cleanup**: re-crawl took `awards` 486 → 616. Then 52 rows the current crawler
would never emit were deleted — 40 mislabelled ``지명 타자`` and 12 second copies
from the `team_name` split (`KIA` vs `KIA 타이거즈`). **616 − 52 = 564, exactly the
crawl's own output.** The rows are in
`data/repair_backups/awards_cleanup_20261104.csv` and the deletion is recorded in
`correction_audit_trail` (`entity_type='award'`, `field_name='row_deleted'`).

**The result**: re-matching awards legacy (112 rows left after §9) gave **106
matched, `field_conflict` 0** — the 36 conflicts were collateral damage from the
crawler, all of them, and re-keying fixed them. 106 more identities moved.

**Six rows remain unmatched, and they are not a matcher problem either**: the
source has no such winner. The names disagree across eras and languages —
2014 `밴헤켄`/`헤켄` (the Dutch surname), 1998·1999·1994 `류지현`/`유지현` (the
spelling changed after 두음법칙 reform; the wiki table uses the period form), 2004
`이진영` (that year's outfielders are 브룸바·이병규·박한이), and 1982 신인상 is `-`
in the chunk with no row at all. **No name-guessing rule was added** — partial
matching would resurrect exactly the false confidence the numeric census had.
They keep their embeddings and stay searchable.

**Golden set: unchanged** (Recall@5 0.8795, MRR 0.7844, hit_rate 0.9000). This work
made the *source* accurate; it did not change the reference identities, so the
30-query set cannot see it. The one absent reference is still
`awards:2025_MVP_NONE_폰세` — no legacy MVP chunk exists for 2025, which only
running the generator can fix (§2-C).

**Known limit, recorded not fixed**: the two sources also spell player names
differently (``에릭 페디``/``페디``, ``코디 폰세``/``폰세``), so those two winners
still have two rows (2023 and 2025 MVP). Deciding "same person" from a partial
string is a guess, and the crawl stage is the wrong place to guess.

## 11. P2 outcome — the movements key was three keys at once (2026-11-05)

`player_movements.team_code` holds Korean names (`롯데`, `삼성`), era codes (`HT`,
`SK`) and canonical codes (`KIA`) -- and the natural key was built from that column.
So one movement was indexed under up to three spellings. Measured on the 6,401
natural-key chunks: **canonical 2,922 · Korean-name 2,724 · era-code 755**, and
**1,504 of them were the same movement stored twice**. Meanwhile 2,821 canonical
keys had no chunk at all.

`canonical_team_id` was already in the source (2,776 rows differ from `team_code`).
Four call sites now pass `canonical_team_id or team_code`: the builder, the rekey
matcher, the census, and the cleanup tool. They must compute the same bytes -- a
rekey swaps one identity for another, so a matcher that disagrees with the builder
produces keys the index does not contain.

**A bug found while reading the index**: `_movement_index` had been giving
`player_name` the column that holds the team code (a column added in the same
edit shifted the tuple). Name-based matching was quietly comparing against the
wrong field. One test in `tests/cli/test_match_legacy_content_movement.py` now
pins that column.

**What was deleted, and what had to be true first**: 7,227 rows -- 4,503 legacy
(`id=N`) and 2,724 Korean-name keys. The legacy half was verified row by row
(4,503/4,503): every one has a natural-key chunk with the same `(date, player,
section)`, every note it carries is in that counterpart, and the source's
`remarks` is at most 28 characters so the generator's `remarks[:150]` truncation
loses nothing. **The first pass read the legacy form as richer because it is
longer** -- it writes a header line and five labelled fields where the natural key
writes one line. Length is not information.

**Resumability, not atomicity, was the answer.** Two attempts to delete all 7,227
in one transaction died with `could not receive data from server: Operation timed
out` while the server sat idle with no locks: the connection was being dropped by
a Tailscale link that has been flapping all session (it also produced a
`password authentication failed` episode and two offline periods). The path now
commits every 200 rows, so a drop costs the batch in flight and the same command
resumes -- an id already off the surface is counted as `already_done`.

**A second silence**: the golden-set coverage check stayed **green** through the
delete while one of the deleted chunks was a golden-set answer. `_lookup_rows` did
not look at `index_status`, so `DELETED` counted as present. Recall fell 0.9462 →
0.9128 and nothing said why -- the same silence this check exists to break. It now
counts only `ACTIVE`, and the golden set's `2018-12-07_삼성_김동엽…` reference was
updated to its canonical `2018-12-07_SS_김동엽…` (the source holds **two rows** that
day -- `넥센`/`SK→넥센` and `삼성`/`넥센→삼성` -- so keeping both canonical keys is
correct; the fixture was pointing at the old name).

**Result**: absent references 0 · **Recall@5 0.9462 · MRR 0.8511 · hit_rate 0.9667 ·
p95 390ms** -- unchanged from before the cleanup, with 7,227 duplicate rows gone.
Audit: `damage 0`, `consistent true`, `deleted 9,604`.

**Still open**: one new movement (the crawler keeps adding) is unindexed until the
nightly run; `update_status`-style last-write inspection is not part of this work.
