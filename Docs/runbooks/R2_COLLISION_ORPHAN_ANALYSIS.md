# R2 Collision & Orphan Analysis Guide

## Overview
Before production apply, the following collision and orphan rows require manual review and classification.

---

## PBP Collisions (4 rows, 2 game IDs)

| Game ID | Content Hash | Chunk IDs | Recommended Action |
|---------|--------------|-----------|-------------------|
| `20240922WOSS0` | `88cf3acb7d31bbb744de97e5e8c3ee5eb2b9cdb0e71d27d6b24cbca17e8d0215` | 1680134, 1680198 | Keep first, tombstone second |
| `20250720WOSS0` | `d0aaba264e6fd5dd7331daf8496bd570f736255f7a1c6f5762fedc8d47823409` | 1812291, 1812324 | Keep first, tombstone second |

### Investigation Steps
```bash
# Fetch both entries for comparison
python -c "
from src.db.engine import get_db_session
from src.models.rag_chunk import RagChunk
from sqlalchemy import select

with get_db_session() as session:
    for cid in [1680134, 1680198, 1812291, 1812324]:
        chunk = session.get(RagChunk, cid)
        print(f'ID={cid} table={chunk.source_table} legacy={chunk.source_row_id} natural={chunk.source_row_id}')
        print(f'  content_hash={chunk.content_hash}')
        print(f'  content[:200]={chunk.content[:200]}')
        print()
"
```

### Decision Matrix
| Pattern | Action |
|---------|--------|
| Identical content, same source_row_index | Keep first, tombstone second |
| Different source_row_index, same game | Keep both with distinct natural keys |
| Content differs | Investigate upstream source |

---

## Content Mismatch Analysis (260 rows)

### Breakdown by Source
| Source | Mismatch Rows | Likely Cause |
|--------|---------------|--------------|
| game_play_by_play | ~197 | Template/whitespace changes, metadata reordering |
| game_highlights | ~63 | Description/template updates |

### Analysis Script
```python
# Cluster mismatches by diff pattern
from src.db.engine import get_db_session
from src.models.rag_chunk import RagChunk
from sqlalchemy import select
import difflib

with get_db_session() as session:
    mismatches = session.execute(
        select(RagChunk).where(
            RagChunk.disposition == "TARGET_EXISTS_CONTENT_MISMATCH"
        )
    ).scalars().all()

    for chunk in mismatches:
        legacy = session.execute(
            select(RagChunk).where(
                RagChunk.source_table == chunk.source_table,
                RagChunk.source_row_id == chunk.legacy_source_row_id
            )
        ).scalar_one()

        diff = list(difflib.unified_diff(
            legacy.content.splitlines(),
            chunk.content.splitlines(),
            lineterm=""
        ))
        if diff:
            pattern = classify_diff(diff)
            print(f"{chunk.source_table} {chunk.chunk_id}: {pattern}")
```

### Classification Patterns
| Pattern | Example | Action |
|---------|---------|--------|
| Whitespace only | `content  vs  content` | Safe rekey (content identical) |
| Template update | `"Player: A" → "선수: A"` | Rekey (semantic same) |
| Metadata reorder | `A, B, C → B, A, C` | Safe rekey |
| Actual content change | `"홈런" → "안타"` | Manual review |

---

## Orphan Analysis (9,514 rows)

### Breakdown by Source
| Source | Orphan Count | % of Source |
|--------|--------------|-------------|
| game_play_by_play | 9,085 | 7.5% |
| player_movements | 401 | 5.9% |
| game_highlights | 26 | 1.2% |
| awards | 2 | 0.4% |

### Orphan Classification Script
```python
from src.db.engine import get_db_session
from src.models.rag_chunk import RagChunk
from sqlalchemy import select, func

with get_db_session() as session:
    # Group orphans by season/game pattern
    orphans = session.execute(
        select(RagChunk).where(
            RagChunk.disposition == "ORPHAN_SOURCE_ROW"
        )
    ).scalars().all()

    # Group by season
    by_season = {}
    for o in orphans:
        year = o.season_year or "unknown"
        by_season.setdefault(year, 0)
        by_season[year] += 1

    for year, count in sorted(by_season.items()):
        print(f"Season {year}: {count} orphans")
```

### Orphan Classification Categories
| Category | Criteria | Action |
|----------|----------|--------|
| Source deleted | Source row no longer exists in Oracle | Tombstone (if historical) or Archive |
| Source ID changed | Legacy ID format changed | Map to new ID, rekey |
| Never crawled | Source never existed | Tombstone |
| Legacy only | Historical data without source | Keep as-is, mark `ARCHIVED` |
| Duplicate natural key | Same natural key, different legacy | Deduplicate |

---

## Resolution Workflow

### For Each Collision/Mismatch/Orphan
```bash
1. Fetch details
   python -m src.cli.rag.census_rag_identity --source game_play_by_play --season 2024 --json | jq '.entries[] | select(.disposition=="SOURCE_COLLISION")'

2. Classify
   # Use analysis scripts above

3. Document decision
   # Add to DECISION_LOG.md
   echo "2024-08-30: PBP collision 20240922WOSS0 - keep 1680134, tombstone 1680198 (identical content)" >> DECISION_LOG.md

4. Update manifest (if needed)
   # Modify entry disposition to TARGET_EXISTS_SAME_CONTENT or ORPHAN_SOURCE_ROW

5. Re-run apply
   python -m src.cli.rag.apply_rag_rekey --manifest r2_manifest.json --apply
```

---

## Decision Log Template

```markdown
# R2 Rekey Decision Log

## 2026-08-30
### PBP Collision: 20240922WOSS0
- Chunks: 1680134, 1680198
- Content hash: 88cf3acb7d31bbb744de97e5e8c3ee5eb2b9cdb0e71d27d6b24cbca17e8d0215
- Decision: Keep 1680134 (first), tombstone 1680198
- Reason: Identical content hash, same source_row_index
- Approved by: [name]

### PBP Collision: 20250720WOSS0
- Chunks: 1812291, 1812324
- Content hash: d0aaba264e6fd5dd7331daf8496bd570f736255f7a1c6f5762fedc8d47823409
- Decision: Keep 1812291 (first), tombstone 1812324
- Reason: Identical content hash, same source_row_index
- Approved by: [name]

## 2026-08-30
### Content Mismatch Pattern: Whitespace normalization
- Affected: ~180 game_play_by_play rows
- Pattern: Legacy has `content  ` vs natural has `content`
- Decision: Safe rekey (semantic identical)
- Approved by: [name]
```

---

## Post-Resolution Verification

```bash
# After resolving all decisions, re-run census
python -m src.cli.kbo rag census --dry-run --json --output r2_manifest_v2.json

# Verify unsafe count reduced
cat r2_manifest_v2.json | jq '.unsafe_entry_count'
# Should be 0 or minimal

# Re-run apply
RAG_INDEX_ALLOW_WRITE=1 RAG_INDEX_ALLOW_PRODUCTION_WRITE=1 \
python -m src.cli.rag.apply_rag_rekey --manifest r2_manifest_v2.json --apply --json
```
