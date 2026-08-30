# R2 Natural-Key Rekey: Oracle Staging Rehearsal & Production Apply Runbook

## Overview
This runbook documents the complete procedure for executing the R2 natural-key rekey on Oracle staging, validating results, and promoting to production.

## Prerequisites

### Environment Variables
```bash
# Oracle connection (staging)
export DATABASE_URL="oracle+oracledb://KBO_APP@<staging_tns>"
export TNS_ADMIN=/path/to/wallet
export RAG_INDEX_VERSION=rag-v2
export RAG_INDEX_ALLOW_WRITE=1
export RAG_INDEX_ALLOW_PRODUCTION_WRITE=1

# Census source (can be same as target for in-place rekey)
export RAG_SOURCE_DB_URL="${DATABASE_URL}"
export RAG_INDEX_DB_URL="${DATABASE_URL}"

# Output directory for artifacts
export R2_REKEY_OUTPUT_DIR="/data/r2_rekey"
```

### Required Access
- Oracle wallet for staging (`TNS_ADMIN`)
- `RAG_INDEX_ALLOW_WRITE=1` and `RAG_INDEX_ALLOW_PRODUCTION_WRITE=1`
- Maintenance lock acquisition capability
- Read/write access to `data/r2_rekey/` directory

---

## Phase 1: Census & Manifest Generation

### 1.1 Run Census on Staging
```bash
# Generate manifest with full header
python -m src.cli.kbo rag census \
  --dry-run \
  --json \
  --output /data/r2_rekey/r2_manifest_$(date +%Y%m%d_%H%M%S).json \
  --sample 50
```

### 1.2 Validate Manifest
```bash
# Check header fields
cat r2_manifest_*.json | jq '.manifest_header'

# Verify counts match expectations
cat r2_manifest_*.json | jq '.totals'
cat r2_manifest_*.json | jq '.sources[] | {source_table, legacy_numeric_rows, safe_rekey_candidates, orphan_rows, collision_rows}'
```

### 1.3 Expected Staging Census (Reference)
| Source Table | Legacy Rows | Safe Rekey | Target Exists Same | Orphan | Collision | Mismatch |
|--------------|-------------|------------|-------------------|--------|-----------|----------|
| awards | ~495 | ~492 | ~1 | ~2 | 0 | 0 |
| team_history | ~385 | ~375 | ~10 | 0 | 0 | 0 |
| player_movements | ~6,802 | ~5,867 | ~534 | ~401 | 0 | 0 |
| game_play_by_play | ~121,449 | ~111,536 | ~627 | ~9,085 | 4 | ~197 |
| game_highlights | ~2,120 | ~2,031 | ~63 | ~26 | 0 | 63 |
| **Total** | **~131,378** | **~120,428** | **~1,172** | **~9,514** | **4** | **~260** |

---

## Phase 2: Dry-Run Apply

### 2.1 Execute Dry-Run
```bash
RAG_INDEX_ALLOW_WRITE=1 RAG_INDEX_ALLOW_PRODUCTION_WRITE=1 \
python -m src.cli.rag.apply_rag_rekey \
  --manifest r2_manifest_YYYYMMDD_HHMMSS.json \
  --json
```

### 2.2 Verify Dry-Run Output
```json
{
  "dry_run": false,
  "rekeyed": 120428,
  "tombstoned": 1172,
  "skipped": 9778,
  "preimage_path": "data/r2_rekey/r2_rekey_YYYYMMDD_HHMMSS_preimage.jsonl",
  "rollback_path": "data/r2_rekey/r2_rekey_YYYYMMDD_HHMMSS_rollback.json"
}
```

### 2.3 Verify Artifacts
```bash
# Check preimage (should have entry_count lines)
wc -l data/r2_rekey/r2_rekey_YYYYMMDD_HHMMSS_preimage.jsonl

# Verify rollback manifest
cat data/r2_rekey/r2_rekey_YYYYMMDD_HHMMSS_rollback.json | jq '.entries | length'
cat data/r2_rekey/r2_rekey_YYYYMMDD_HHMMSS_rollback.json | jq '.manifest_header'
```

---

## Phase 3: Production Apply (Canary)

### 3.1 Apply First Canary Batch (futures_schedule)
```bash
# Filter manifest for single source
cat r2_manifest.json | jq '
  .entries |= map(select(.source_table == "futures_schedule"))
' > r2_manifest_futures.json

# Update header counts
cat r2_manifest_futures.json | jq '
  .totals.source_rows = (.entries | length) |
  .totals.legacy_numeric_rows = (.entries | map(select(.legacy_source_row_id | test("^\\d+$"))) | length) |
  .sources = [.sources[] | select(.source_table == "futures_schedule")]
' > r2_manifest_futures_fixed.json

# Apply
RAG_INDEX_ALLOW_WRITE=1 RAG_INDEX_ALLOW_PRODUCTION_WRITE=1 \
python -m src.cli.rag.apply_rag_rekey \
  --manifest r2_manifest_futures_fixed.json \
  --apply --json
```

### 3.2 Post-Canary Validation
```bash
# Verify no new legacy rows created
python -m src.cli.kbo rag census --source futures_schedule --dry-run --json

# Check audit
python -m src.cli.rag.audit_rag_index --require-postings --json
python -m src.cli.rag.audit_rag_tombstones --fail-on-unexplained --json

# Golden query regression
python -m src.cli.kbo rag evaluate --golden-path Docs/references/rag_golden_queries.json --json
```

### 3.3 Canary Sources Order
| Order | Source | Rows | Risk |
|-------|--------|------|------|
| 1 | futures_schedule | ~127 | Low (no legacy) |
| 2 | awards | ~495 | Low |
| 3 | team_history | ~385 | Low |
| 4 | player_movements | ~6,802 | Medium |
| 5 | game_highlights | ~2,120 | Medium |
| 6 | game_play_by_play | ~121,449 | High (collisions, orphans) |

---

## Phase 4: Full Production Apply

### 4.1 Execute Full Apply
```bash
RAG_INDEX_ALLOW_WRITE=1 RAG_INDEX_ALLOW_PRODUCTION_WRITE=1 \
python -m src.cli.rag.apply_rag_rekey \
  --manifest r2_manifest_YYYYMMDD_HHMMSS.json \
  --apply --json 2>&1 | tee apply_YYYYMMDD_HHMMSS.log
```

### 4.2 Post-Apply Validation Checklist
- [ ] `rekeyed + tombstoned + skipped == total_entries`
- [ ] `audit_rag_index --require-postings` → `consistent: true`
- [ ] `audit_rag_tombstones --fail-on-unexplained` → `consistent: true`
- [ ] Golden query regression: `recall@5 >= 0.85`, `MRR >= 0.70`
- [ ] No new legacy numeric rows in any source table
- [ ] Preimage file exists and has correct line count
- [ ] Rollback manifest created and valid

---

## Phase 5: Rollback Procedure (If Needed)

### 5.1 Execute Rollback
```bash
RAG_INDEX_ALLOW_WRITE=1 RAG_INDEX_ALLOW_PRODUCTION_WRITE=1 \
python -m src.cli.rag.apply_rag_rekey \
  --manifest data/r2_rekey/r2_rekey_YYYYMMDD_HHMMSS_rollback.json \
  --apply --json
```

### 5.2 Post-Rollback Validation
```bash
# Verify rollback completed
python -m src.cli.kbo rag census --dry-run --json | jq '.totals.legacy_numeric_rows'
# Should return to pre-apply counts

# Verify audit
python -m src.cli.rag.audit_rag_tombstones --fail-on-unexplained --json
```

---

## Collision Handling (PBP - 4 rows)

The PBP collision involves 2 game IDs with duplicate content:
- `20240922WOSS0_content_88cf3acb7d31bbb744de97e5e8c3ee5eb2b9cdb0e71d27d6b24cbca17e8d0215` → IDs 1680134, 1680198
- `20250720WOSS0_content_d0aaba264e6fd5dd7331daf8496bd570f736255f7a1c6f5762fedc8d47823409` → IDs 1812291, 1812324

**Action**: Manual review required before apply. Options:
1. Keep first, tombstone second
2. Merge into single entry
3. Investigate upstream source deduplication

---

## Monitoring & Alerting

### Key Metrics to Watch
| Metric | Threshold | Action |
|--------|-----------|--------|
| New legacy rows/hour | > 0 | Alert: ingestion regression |
| Orphan row increase | > 100/day | Investigate source |
| Collision increase | > 0 | Immediate investigation |
| Golden query recall@5 | < 0.85 | Rollback consideration |
| Golden query p95 latency | > 500ms | Performance investigation |

### Dashboards
- Census drift: `kbo rag census --dry-run --json` (scheduled daily)
- Index health: `audit_rag_index` + `audit_rag_tombstones` (scheduled daily)
- Retrieval quality: `rag evaluate` (scheduled weekly)

---

## Rollback Decision Matrix

| Condition | Action |
|-----------|--------|
| `audit_rag_index` finds `findings > 0` | Rollback |
| `audit_rag_tombstones` finds `unexplained > 0` | Rollback |
| Golden query `recall@5 < 0.85` | Rollback + investigate |
| `p95 latency > 2000ms` | Rollback + investigate |
| New legacy rows detected | Pause, investigate, then rollback if confirmed |

---

## Contacts & Escalation

| Role | Contact | Escalation |
|------|---------|------------|
| DBA (Oracle) | - | Schema locks, performance |
| Data Engineering | - | Ingestion pipeline |
| Platform | - | Lock issues, rollback execution |

---

## Appendix: Key Commands Reference

```bash
# Census with full output
python -m src.cli.kbo rag census --dry-run --json --output manifest.json --sample 100

# Apply dry-run
python -m src.cli.rag.apply_rag_rekey --manifest manifest.json --json

# Apply with canary source
python -m src.cli.rag.apply_rag_rekey --manifest manifest.json --apply --json

# Rollback
python -m src.cli.rag.apply_rag_rekey --manifest rollback.json --apply --json

# Post-apply audits
python -m src.cli.rag.audit_rag_index --require-postings --json
python -m src.cli.rag.audit_rag_tombstones --fail-on-unexplained --json

# Golden query evaluation
python -m src.cli.kbo rag evaluate --golden-path Docs/references/rag_golden_queries.json --json
```

---

## Revision History

| Date | Version | Author | Changes |
|------|---------|--------|---------|
| 2026-08-30 | 1.0 | - | Initial version |