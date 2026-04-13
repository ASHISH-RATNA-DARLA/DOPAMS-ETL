---
name: dopams-etl-triage
description: "Diagnose and fix DOPAMS ETL pipeline issues with a repeatable workflow. Use when ETL jobs fail, row counts mismatch, SQL refreshes break, triggers misbehave, or Python ETL modules regress."
argument-hint: "Describe the failing ETL module, symptoms, and recent changes"
---

# DOPAMS ETL Triage

Use this skill to run a structured, root-cause-first ETL debugging and remediation workflow in this repository.

## When to Use

- ETL script failures in module folders (for example IR, crimes, persons, disposal)
- Materialized view refresh issues or stale data in search/profile views
- Trigger or schema drift after migration changes
- Performance regressions in refresh/transform scripts
- Data quality mismatches between source and target counts

## Inputs to Collect First

- Failing module or script path
- Exact error text and where it appears (terminal/log/DB)
- Expected vs actual row counts or behavior
- Last known good commit or change window
- Environment details (database, schema, branch)

## Procedure

1. Scope the failure precisely.
   - Identify one failing unit first (single SQL file, Python ETL module, or refresh script).
   - Record reproducible steps and expected output.
2. Reproduce and capture evidence.
   - Run only the smallest command/query needed to reproduce.
   - Save exact error output, offending SQL statement, and sample IDs if available.
3. Establish a failure timeline.
   - Pinpoint when behavior diverged and what changed (DDL, ETL logic, source data shape, runtime environment).
   - Compare with the last known good state.
4. Trace dependencies.
   - Map upstream and downstream dependencies: source tables, transforms, views, triggers, and consumers.
   - Confirm whether breakage is logic, schema, data, or performance.
5. Form and rank hypotheses.
   - Prioritize high-probability causes first: column mismatch, join cardinality explosion, filter drift, trigger side effects, null handling, transaction boundaries, and stale materialized views.
6. Prove or reject hypotheses with targeted checks.
   - Use focused SQL probes and small-scope runs to isolate the first failing boundary.
   - Keep evidence for each rejected hypothesis to avoid circular debugging.
7. Apply minimal fix.
   - Change only the smallest surface required.
   - Preserve existing public contract (table/view shape and key semantics) unless explicitly requested.
8. Verify behavior.
   - Re-run reproduction steps.
   - Validate row counts, spot-check records, and ensure no new errors in edited files.
9. Assess blast radius.
   - Check related modules and dependent materialized views.
   - Confirm no regressions in nearby ETL workflows.
10. Document closure.
   - Summarize root cause, fix, evidence, residual risk, and follow-up actions.

## Decision Branches

- If failure is syntax/runtime immediate:
  - Fix parser/runtime blockers first, then resume logic validation.
- If failure is data mismatch only:
  - Focus on joins, filters, groupings, and deduplication behavior.
- If failure appears after migration/DDL:
  - Audit schema compatibility and trigger assumptions before changing transform logic.
- If performance regression dominates:
  - Profile the query path, inspect heavy joins/functions, and optimize before broad rewrites.

## Module-Specific Branches

- If module is IR or files ingestion:
   - Validate JSON extraction assumptions, mandatory field mapping, and null fallbacks.
- If module is persons/accused/profile:
   - Validate identity joins, dedup strategy, and normalization rules before aggregation changes.
- If module is disposal/case status:
   - Validate state transition logic and status precedence rules.
- If module is refresh views/search MVs:
   - Validate refresh order, dependency chain, and index/selectivity assumptions.

## Completion Criteria

- Reproducible issue is no longer reproducible.
- Primary acceptance checks pass (counts, sample correctness, no new lint/syntax problems in touched files).
- No detected regressions in direct dependents.
- Final note includes root cause and rollback-safe context.

## Output Format

Provide a concise report with:

1. Symptom summary
2. Root cause
3. Exact change made
4. Validation evidence
5. Remaining risks

## References

- [verification checklist](./references/verification-checklist.md)
- [project README](../../../README.md)
- [ETL diagnostics](../../../ETL_DIAGNOSTICS.md)
- [thread safety checklist](../../../THREAD_SAFETY_CHECKLIST.md)
