# Brief Facts AI ETL Comprehensive Optimization Design

**Date:** 2026-04-24  
**Timeline:** 3-5 days  
**Approach:** Configuration-First + Code Changes with Rolling Deployment  
**Success Target:** 25-40% execution time improvement, zero data loss, full regression testing

---

## Executive Summary

The brief_facts_ai ETL processes ~350 crimes per run with current execution time of ~40+ hours. Root cause analysis identified:
- **Primary bottleneck (unavoidable):** LLM call latency (54-400s per crime) 
- **Secondary bottlenecks (optimizable):** Per-crime DB queries (5-10%), per-accused writes (10-15%), connection pool undersizing (5-10%), dedup candidate redundancy (5-10%)

This design implements a three-phase optimization plan that eliminates secondary bottlenecks through configuration changes, code refactoring, and comprehensive testing—without breaking changes or skipped records.

**Constraints:**
- Zero breaking changes
- Zero skipped extraction records
- Safe rollback at each phase
- Rolling deployment with metrics monitoring

---

## Architecture Overview

### Three-Phase Rollout

```
Phase 1 (Day 1): CONFIG ONLY
  └─ Connection pool sizing via .env
     Risk: None | Gain: 5-10% | Rollback: Immediate

Phase 2 (Days 2-3): CODE CHANGES  
  ├─ Remove duplicate accused writes
  ├─ Cache dedup candidates per crime
  └─ Optimize dedup scoring scope
     Risk: Low | Gain: 15-25% | Rollback: Git revert

Phase 3 (Days 4-5): SAFETY + TESTING
  ├─ Batch commit strategy hardening
  ├─ Data integrity monitoring
  └─ Comprehensive regression testing
     Risk: Very Low | Gain: 0% (safety only) | Rollback: Config revert
```

**Total Expected Improvement:** 25-40% execution time reduction

---

## Phase 1: Connection Pool Configuration (Day 1)

### Problem

Current connection pool is undersized for 3 concurrent workers:
```
Current configuration:
  DB_POOL_MIN_SIZE=2
  DB_POOL_MAX_SIZE=5
```

With 3 workers processing crimes simultaneously, each worker needs an active connection. Pool max of 5 leaves only 2 spares for retry logic and edge cases. Workers block waiting for available connections.

### Solution

Increase pool size to accommodate 3 workers + buffer:
```
New configuration:
  DB_POOL_MIN_SIZE=5
  DB_POOL_MAX_SIZE=10
```

**Why these numbers:**
- Min 5: Covers baseline 3 workers + 2 spares for overhead
- Max 10: Allows growth for concurrent database operations within worker threads

### Implementation

**File:** `.env`

**Change:**
```diff
- DB_POOL_MIN_SIZE=2
+ DB_POOL_MIN_SIZE=5
- DB_POOL_MAX_SIZE=5
+ DB_POOL_MAX_SIZE=10
```

**Restart required:** Yes (worker restart picks up new .env values)

### Validation

**Metrics to monitor (production):**
- `pg_stat_activity.count` - Should stay < 10 (not exceeding pool max)
- Worker thread blocking time - Should decrease
- Brief_facts_ai crime processing time baseline - Document for Phase 2 comparison

**Success criteria:**
- Pool utilization stabilizes at 5-7 connections (not maxing out at 5)
- No connection timeout errors in logs
- Worker processing latency unchanged or improved

### Rollback

Change `.env` back to original values and restart workers (5 minute downtime).

### Expected Gain

5-10% execution time reduction (eliminates connection pool contention)

---

## Phase 2: Code Changes (Days 2-3)

### Change 2.1: Remove Duplicate Accused Writes

#### Problem

Code writes accused records twice:
1. **Line 1138** in `main.py`: `insert_accused_facts(conn, row_data)` — per-accused INSERT
2. **Line 848** in `main.py`: `bulk_upsert_brief_facts_ai(conn, enriched_rows)` — bulk upsert of same data

Flow:
```python
# Lines 985-1139: Process each accused
for i, row in enumerate(valid_accused, start=1):
    row_data = {... all fields ...}
    branch_records.append(row_data)
    insert_accused_facts(conn, row_data)  # ← WRITE 1: PER-ACCUSED INSERT
    count += 1

# Lines 844-848: Later, bulk write same data
enriched_rows = write_drugs_by_accused_in_memory(branch_records, extractions)
bulk_upsert_brief_facts_ai(conn, enriched_rows)  # ← WRITE 2: SAME DATA AGAIN
```

This causes:
- 2× disk I/O per record
- 10-15% execution time overhead (round-trips + disk fsync)
- Potential race condition if one write fails but other succeeds

#### Solution

Remove line 1138 `insert_accused_facts()` call. The `bulk_upsert_brief_facts_ai()` at line 848 already writes all records atomically.

#### Implementation

**File:** `brief_facts_ai/main.py`

**Location:** Lines 1137-1139

**Current code:**
```python
branch_records.append(row_data)
insert_accused_facts(conn, row_data)  # ← DELETE THIS LINE
count += 1
```

**New code:**
```python
branch_records.append(row_data)
count += 1
```

**Changes:** 1 line deletion

### Change 2.2: Cache Dedup Candidates Per Crime

#### Problem

`_resolve_canonical_identity()` (line 1092, called per-accused) invokes `fetch_dedup_candidates()` for each accused with identical or very similar parameters:

```python
# Lines 985-1107: Loop over accused in crime
for i, row in enumerate(valid_accused, start=1):
    accused_id = row.get('accused_id')
    full_name = row.get('full_name')
    
    # ... processing ...
    
    # Line 1092: Called per-accused
    canonical_person_id, ..., = _resolve_canonical_identity(
        conn, crime_id,
        {'full_name': full_name, ...},
        ps_code,
    )

# Inside _resolve_canonical_identity, line 380:
candidates = fetch_dedup_candidates(conn, current_crime_id, full_name, ps_code)
```

If crime has 5 accused:
- fetch_dedup_candidates called 5× (or more, if names differ)
- Each call hits database (expensive)
- Candidates often overlap (same person in multiple dedup matches)

**Impact:** 5-10% execution overhead (redundant database queries)

#### Solution

Fetch all dedup candidates once per crime (before processing accused), cache them, reuse for all accused in that crime.

#### Implementation

**File:** `brief_facts_ai/main.py`

**Changes:**

1. **Before accused loop (after line 960), add cache initialization:**
```python
# ── Dedup candidate caching ──
_dedup_candidate_cache = {}  # key: (crime_id, full_name, ps_code) → value: candidates list
```

2. **In _resolve_canonical_identity function (around line 380), replace fetch call:**

**Current (line 380):**
```python
candidates = fetch_dedup_candidates(conn, current_crime_id, full_name, ps_code)
```

**New:**
```python
# Check cache first
cache_key = (current_crime_id, full_name, ps_code)
if cache_key not in _dedup_candidate_cache:
    _dedup_candidate_cache[cache_key] = fetch_dedup_candidates(
        conn, current_crime_id, full_name, ps_code
    )
candidates = _dedup_candidate_cache[cache_key]
```

**Changes:** 2 locations modified (cache init + cache lookup)

### Change 2.3: Optimize Dedup Scoring Scope

#### Problem

`fetch_dedup_candidates()` returns candidates based on phonetic matching (SOUNDEX) across entire system—potentially 100+ candidates per accused. Scoring all candidates is CPU-expensive (O(N²) token matching per candidate).

Current scope: All persons matching SOUNDEX across all PS codes and all time periods.

#### Solution

Narrow candidate scope to reduce scoring load while preserving accuracy:
- **Same PS code:** More likely to be same person (same jurisdiction)
- **Recent 6 months:** Crimes tend to cluster in time and location

Layer 0 (`accused_id` lookup, line 375) still catches cross-jurisdiction matches, so this optimization is safe for fuzzy-match fallback cases.

#### Implementation

**File:** `brief_facts_ai/db.py`

**Function:** `fetch_dedup_candidates(conn, crime_id, full_name, ps_code)`

**Current query (pseudo-code):**
```sql
SELECT * FROM persons 
WHERE soundex(full_name) = soundex(?)
ORDER BY last_updated DESC
```

**New query:**
```sql
SELECT * FROM persons 
WHERE soundex(full_name) = soundex(?)
  AND ps_code = ?                              -- Same jurisdiction
  AND crime_date >= NOW() - INTERVAL '6 months' -- Recent cases
ORDER BY last_updated DESC
```

**Changes:** 2 WHERE conditions added to narrow scope

**Trade-off analysis:**
- **Gain:** 20-30% fewer candidates to score per accused, 5-10% execution improvement
- **Risk:** Could miss genuine cross-jurisdiction matches
- **Mitigation:** Layer 0 `accused_id` lookup (line 375) catches person_id matches globally. This optimization only affects fuzzy-match fallback for NEW persons (not in system yet), where cross-jurisdiction is rare.

### Phase 2 Implementation Order

**Day 2:** 
1. Implement Change 2.1 (remove duplicate write) — lowest risk, immediate gain
2. Test on staging: 100 crimes, verify identical output

**Day 3:**
1. Implement Change 2.2 (dedup candidate caching) — medium risk, cache logic
2. Implement Change 2.3 (dedup scope optimization) — medium risk, query change
3. Test on staging: 100 crimes, verify dedup matches identical
4. Deploy to production with monitoring

### Validation

**Data integrity checks:**
- Brief_facts_ai row counts before/after (should be identical)
- No duplicate records created
- Dedup match distributions (should match Phase 1 baseline)

**Performance checks:**
- Execution time improvement 15-25%
- Crime processing latency per worker
- Database query count reduction

**Success criteria:**
- Zero duplicate records
- Dedup results identical to Phase 1 baseline
- Execution time improvement 15-25%
- All 350 crimes process without errors

---

## Phase 3: Safety & Testing (Days 4-5)

### Change 3.1: Batch Commit Strategy Hardening

#### Problem

Current batch commit strategy (lines 853-870):
```python
crime_count += 1
should_commit = (crime_count % batch_commit_size == 0)

if should_commit:
    conn.commit()
else:
    sp_name = f"sp_crime_{crime_id}"
    cur.execute(f"SAVEPOINT {sp_name}")
```

**Issues:**
1. `batch_commit_size` value unknown (need to verify in etl_config.py)
2. If too large (e.g., 500): Loss of ~500 crimes on failure (unrecoverable)
3. If too small (e.g., 5): Frequent fsync overhead (performance hit)
4. SAVEPOINT errors silently ignored (per-crime rollback may fail silently)

#### Solution

1. **Verify current batch_commit_size** in `brief_facts_ai/etl_config.py`
2. **Set optimal value: batch_commit_size = 50**
   - 50 crimes × ~120s = 6000s = 100 minutes between commits
   - Risk: Lose up to 50 crimes on failure (recoverable)
   - fsync overhead: ~50 per 10 hours (minimal, ~0.1% overhead)
3. **Add safety logging** to track commits

#### Implementation

**File 1:** `brief_facts_ai/etl_config.py`

**Verify and set:**
```python
batch_commit_size = 50  # Optimized: not too large, not too small
```

**File 2:** `brief_facts_ai/main.py`

**Location:** Lines 855-861

**Current:**
```python
if should_commit:
    conn.commit()
    commit_count += 1
    logging.debug(f"Batch commit #{commit_count} after {batch_commit_size} crimes")
```

**Enhanced:**
```python
if should_commit:
    # Calculate crime range for this batch
    batch_start = crime_count - batch_commit_size + 1
    batch_end = crime_count
    
    conn.commit()
    commit_count += 1
    
    logging.info(
        f"Batch commit #{commit_count}: crimes {batch_start}-{batch_end}, "
        f"total_rows_written={rows_written}"
    )
```

### Change 3.2: Data Integrity Monitoring

#### What to Monitor

**1. Dedup Match Distribution (per crime):**
```python
# After _process_branch_*() completes (around line 1140)
dedup_tiers = {}
for row in branch_records:
    tier = row.get('dedup_match_tier')
    dedup_tiers[tier] = dedup_tiers.get(tier, 0) + 1

logging.info(f"Crime {crime_id}: dedup distribution: {dedup_tiers}")
```

**Why:** Detects if optimization (Change 2.3) causes unexpected dedup behavior (e.g., more false positives). If tier distribution shifts significantly, indicates potential issue.

**2. Write Operation Validation (per crime):**
```python
# After bulk_upsert (around line 848)
written_count = len(enriched_rows)
logging.info(f"Crime {crime_id}: {written_count} rows written to brief_facts_ai")
```

**Why:** Detects silent failures (e.g., upsert silently drops rows). Should match expected count (num_accused + orphan rows).

**3. Cache Hit Rate (for dedup candidates):**
```python
# Track cache misses vs hits during dedup
cache_hits = sum(1 for key in _dedup_candidate_cache if key in used_keys)
cache_total = len(_dedup_candidate_cache)
logging.info(f"Dedup cache: {cache_hits}/{cache_total} hits")
```

**Why:** Validates that caching (Change 2.2) is working. High hit rate = fewer database queries.

#### Implementation

**File:** `brief_facts_ai/main.py`

**Locations:** After each major operation (lines 848, 1140, and within dedup loop)

**Changes:** Add 3 logging statements with structured data for dashboard ingestion

### Change 3.3: Comprehensive Regression Testing

#### Test Matrix

| Test | Scope | What to Verify | Pass Criteria |
|------|-------|----------------|---------------|
| **Baseline (Phase 1 only)** | 100 crimes | Execution time, dedup distribution, row counts | Document baseline metrics |
| **Phase 2 Code** | 100 crimes | Execution time, dedup results identical to baseline | 15-25% improvement, identical dedup matches |
| **Data Integrity** | 350 crimes | Brief_facts_ai row count, no duplicates | Rows match input, zero duplicates |
| **Dedup Accuracy** | 10 crimes (spot check) | Dedup match results vs. baseline | False positives/negatives = 0 |
| **Cross-PS Cases** | 5 crimes with cross-PS accused | Layer 0 (accused_id) still catches them | All cross-PS persons linked correctly |
| **Full Run** | 350 crimes | End-to-end execution, all phases enabled | < 28 hours, zero timeouts, zero skipped |
| **Failure Recovery** | Simulate kill at random crime | Commit safety, data loss on restart | Lost crimes < 50, recoverable |

#### Test Execution Schedule

**Day 4:**
1. Run Baseline test (Phase 1 metrics)
2. Deploy Phase 2 code
3. Run Phase 2 Code test (100 crimes)
4. Run Dedup Accuracy test (10 crimes spot check)

**Day 5:**
1. Deploy Phase 3 (monitoring + commit strategy)
2. Run Data Integrity test (350 crimes)
3. Run Cross-PS test (5 crimes)
4. Run Full Run test (350 crimes)
5. Run Failure Recovery simulation
6. Document results, greenlight for production rolling deployment

#### Success Criteria (All Tests Pass)

- Execution time: 25-40% improvement (Phase 1 baseline to Phase 2)
- Dedup accuracy: Identical results to baseline (zero false positives/negatives)
- Data integrity: Zero duplicate rows, all records present
- Safety: Batch commits every 100 minutes, < 50 crime loss on failure
- Monitoring: All metrics captured and dashboarded
- No regressions in downstream ETL (validate on next full run)

---

## Rollback Strategy

### Per-Phase Rollback

| Scenario | Rollback Action | Downtime | Data Risk |
|----------|-----------------|----------|-----------|
| **Phase 1 failure** | Revert .env pool size to original | 5 min (restart workers) | None (config only) |
| **Phase 2 failure** | `git revert` Phase 2 commits, redeploy | 10 min | Low (bulk_upsert still works) |
| **Phase 3 failure** | Revert logging + batch_commit_size | 5 min | Very low (safety code) |
| **Data corruption** | Restore brief_facts_ai from backup | 30 min | Low (backup available) |

### Rollback Triggers

- **Automatic:** Execution time increases > 50% vs baseline
- **Automatic:** Dedup false positives detected (tier distribution shift > 10%)
- **Automatic:** Row count mismatch (written_count != expected_count)
- **Manual:** Any unrecoverable database error in logs
- **Manual:** Brief_facts_ai data corruption detected
- **Manual:** Downstream ETL failures (IR, arrests, persons tables)

---

## Monitoring & Metrics Dashboard

### Metrics to Capture (Phase 1-3)

**Performance Metrics:**
- Total execution time per run
- Per-crime processing latency (average)
- Worker thread utilization
- Database query count
- Connection pool utilization

**Data Integrity Metrics:**
- Brief_facts_ai row count (should match input)
- Duplicate row count (should be 0)
- Dedup match distribution (by tier)
- Write operation row count (per crime)

**Safety Metrics:**
- Batch commit frequency
- Commit latency (disk fsync time)
- Savepoint success/failure count
- Cache hit rate (dedup candidates)

### Dashboard Template

```
BRIEF_FACTS_AI OPTIMIZATION DASHBOARD
=====================================

PHASE 1 BASELINE (CONFIG ONLY)
  Execution time: 42 hours
  Connection pool utilization: 4-5 (max 5)
  Dedup tier distribution: {0: 150, 1: 120, 2: 60, 3: 20}
  Row count: 350 × 5 accused = 1,750 rows

PHASE 2 + 1 (CODE CHANGES)
  Execution time: 32 hours (23% improvement ✓)
  Connection pool utilization: 5-7 (max 10)
  Dedup tier distribution: {0: 150, 1: 120, 2: 60, 3: 20} (identical ✓)
  Row count: 1,750 rows (identical ✓)
  Duplicate rows: 0 (✓)
  Cache hit rate: 75% (efficient ✓)

PHASE 3 (SAFETY + MONITORING)
  Batch commits: 7 (every ~100 minutes ✓)
  Commit latency: 200ms avg (acceptable ✓)
  Failures: 0 (✓)
  Recovery time: < 5 min if failure (✓)
```

---

## Risk Assessment & Mitigation

| Risk | Likelihood | Severity | Mitigation |
|------|------------|----------|-----------|
| Phase 1: Connection exhaustion | Low | High | Monitor pg_stat_activity, set max_overflow limit |
| Phase 2: Duplicate write removal breaks logic | Low | High | Verify bulk_upsert handles all cases before removing insert_accused_facts |
| Phase 2: Dedup scope too narrow (miss cross-PS) | Medium | Low | Layer 0 accused_id lookup still global; only affects fuzzy fallback |
| Phase 2: Cache memory growth | Low | Medium | Cache cleared per crime (not persistent); max size bounded by one crime's accused |
| Phase 3: Batch commit size too large | Low | Medium | Set batch_commit_size=50 empirically; monitor loss on failure |
| Phase 3: Monitoring overhead | Very Low | Low | Add logging conditionally (only at INFO level, not DEBUG) |
| Downstream impact | Very Low | Medium | Validate IR, arrests, persons ETL after Phase 2 deploys |

---

## Success Definition

### Must Have (Blocking for Production)
- ✓ Zero broken changes (all phases rollback-able)
- ✓ Zero skipped extraction records
- ✓ Execution time improvement ≥ 25% (Phase 1 → Phase 2)
- ✓ Dedup results identical to baseline (zero new false positives)
- ✓ Brief_facts_ai row count identical before/after
- ✓ All 350 crimes process without timeout
- ✓ Batch commits working, < 50 crime loss on failure

### Should Have (Nice to Have)
- ≥ 30% execution time improvement (stretch goal)
- Cache hit rate > 70% (validates Change 2.2)
- Monitoring dashboard fully instrumented
- Load test on 500+ crimes (stretch, if time)

### Out of Scope
- Refactor LLM parallelism (requires architectural change)
- Async pipeline (Phase 2 of prior PERFORMANCE_AUDIT_COMPLETE.md)
- Database query profiling (covered by PERFORMANCE_AUDIT_COMPLETE.md)

---

## Implementation Sequence

**Day 1 (Phase 1):**
- [ ] Verify current DB_POOL_* values in .env
- [ ] Update .env: DB_POOL_MIN_SIZE=5, DB_POOL_MAX_SIZE=10
- [ ] Restart workers, monitor pg_stat_activity
- [ ] Document baseline metrics

**Day 2 (Phase 2 Start):**
- [ ] Implement Change 2.1 (remove duplicate write)
- [ ] Test: 100 crimes, verify output identical
- [ ] Commit to feature branch

**Day 3 (Phase 2 Complete):**
- [ ] Implement Change 2.2 (dedup candidate caching)
- [ ] Implement Change 2.3 (dedup scope optimization)
- [ ] Test: 100 crimes, verify dedup results
- [ ] Test: Spot check 10 crimes for accuracy
- [ ] Merge to main, deploy to production with metrics

**Day 4 (Phase 3 Start):**
- [ ] Implement Change 3.1 (batch commit hardening)
- [ ] Verify batch_commit_size=50 in etl_config.py
- [ ] Implement Change 3.2 (data integrity monitoring)
- [ ] Run regression test matrix (partial)

**Day 5 (Phase 3 Complete):**
- [ ] Complete regression test matrix
- [ ] Run full 350-crime end-to-end test
- [ ] Simulate failure recovery
- [ ] Document all metrics
- [ ] Greenlight for production rolling deployment

---

## Files to Modify

| File | Changes | Risk | Review Priority |
|------|---------|------|-----------------|
| `.env` | DB_POOL_MIN_SIZE, DB_POOL_MAX_SIZE | None (config) | Low |
| `brief_facts_ai/main.py` | Remove line 1138, add caching, add logging | Low | High |
| `brief_facts_ai/db.py` | fetch_dedup_candidates WHERE scope | Medium | High |
| `brief_facts_ai/etl_config.py` | Verify batch_commit_size=50 | None (verify) | Low |

---

## Communication Plan

- **Day 1 end:** Slack notification of Phase 1 deployment, baseline metrics
- **Day 2 end:** Phase 2 code review request
- **Day 3 end:** Phase 2 production deployment with monitoring link
- **Day 5 end:** Phase 3 regression test results, greenlight decision
- **Post-deployment:** Weekly metric review for 2 weeks (catch regressions early)

---

## Appendix: Code Examples

### Example: Change 2.1 Removal (Line 1138)

```python
# BEFORE
for i, row in enumerate(valid_accused, start=1):
    # ... process row ...
    row_data = {...}
    branch_records.append(row_data)
    insert_accused_facts(conn, row_data)  # ← DELETE
    count += 1

# AFTER
for i, row in enumerate(valid_accused, start=1):
    # ... process row ...
    row_data = {...}
    branch_records.append(row_data)
    count += 1
```

### Example: Change 2.2 Caching

```python
# Before accused loop
_dedup_candidate_cache = {}

# Inside _resolve_canonical_identity (line 380)
cache_key = (current_crime_id, full_name, ps_code)
if cache_key not in _dedup_candidate_cache:
    _dedup_candidate_cache[cache_key] = fetch_dedup_candidates(
        conn, current_crime_id, full_name, ps_code
    )
candidates = _dedup_candidate_cache[cache_key]
```

### Example: Change 2.3 Query Scope

```sql
-- BEFORE
SELECT * FROM persons 
WHERE soundex(full_name) = soundex(?)
ORDER BY last_updated DESC

-- AFTER
SELECT * FROM persons 
WHERE soundex(full_name) = soundex(?)
  AND ps_code = ?
  AND crime_date >= NOW() - INTERVAL '6 months'
ORDER BY last_updated DESC
```

---

## References

- Bottleneck Analysis: `/home/eagle/.claude/projects/-data-drive-etl-process-dev/memory/brief_facts_ai_bottlenecks.md`
- Previous Performance Audit: `/data-drive/etl-process-dev/PERFORMANCE_AUDIT_COMPLETE.md`
- Brief Facts AI Requirements: `/data-drive/etl-process-dev/brief_facts_ai_requirements.md`
- Main implementation: `/data-drive/etl-process-dev/brief_facts_ai/main.py`
