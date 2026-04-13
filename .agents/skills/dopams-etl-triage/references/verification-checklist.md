# Verification Checklist

Use this checklist after applying an ETL fix.

## Functional

- Reproduction command/query now succeeds
- Expected row counts match for target entity
- Sample records are semantically correct

## Stability

- No new syntax or import errors in touched files
- Dependent scripts or views still execute
- Trigger side effects are unchanged unless intentionally modified

## Data Safety

- No accidental widening/narrowing of result set
- Null handling and defaults remain intentional
- Key uniqueness/dedup assumptions still hold

## Operational

- Steps to rerun and validate are documented
- Known limitations or risks are listed
- Rollback approach is clear if applicable
