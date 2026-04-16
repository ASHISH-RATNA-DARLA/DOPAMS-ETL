# ETL Backfill Execution Guide

## Pre-Backfill Checklist

✅ Database is truncated (except Knowledge Base)
✅ etl_persons.py isoformat bug is fixed
✅ etl-crimes/config.py updated (start: June 2022, end: Apr 16, 2026)
✅ crimes table has PRIMARY KEY on crime_id (added for ON CONFLICT upsert)
⚠️ Need to: Clear etl_run_state checkpoints on remote server
⚠️ Need to: Confirm etl-mongo-to-postgresql is not running

## Step 0: Fix PRIMARY KEY constraints on UPSERT tables

Multiple tables use ON CONFLICT clauses that require PRIMARY KEY or UNIQUE constraints. Apply ALL 10 constraint fixes (6 primary tables + 4 IR subtables + 1 special case):

```bash
psql -h 192.168.103.106 -U dev_dopamas -d dev-3 << 'EOF'
-- ========== PRIMARY TABLES (6) ==========
-- Add PRIMARY KEY constraints for single-column upserts
ALTER TABLE public.crimes ADD CONSTRAINT pk_crimes_id PRIMARY KEY (crime_id);
ALTER TABLE public.accused ADD CONSTRAINT pk_accused_id PRIMARY KEY (accused_id);
ALTER TABLE public.persons ADD CONSTRAINT pk_persons_id PRIMARY KEY (person_id);
ALTER TABLE public.properties ADD CONSTRAINT pk_properties_id PRIMARY KEY (property_id);
ALTER TABLE public.interrogation_reports ADD CONSTRAINT pk_ir_id PRIMARY KEY (interrogation_report_id);

-- Add composite UNIQUE constraint for disposal table's ON CONFLICT
ALTER TABLE public.disposal ADD CONSTRAINT uk_disposal_composite UNIQUE (crime_id, disposal_type, disposed_at);

-- ========== INTERROGATION REPORT SUBTABLES (4) ==========
-- Add PRIMARY KEY constraints for IR subtables using ON CONFLICT DO NOTHING
ALTER TABLE public.ir_regular_habits ADD CONSTRAINT pk_ir_regular_habits_id PRIMARY KEY (id);
ALTER TABLE public.ir_media ADD CONSTRAINT pk_ir_media_id PRIMARY KEY (id);
ALTER TABLE public.ir_interrogation_report_refs ADD CONSTRAINT pk_ir_interrogation_report_refs_id PRIMARY KEY (id);
ALTER TABLE public.ir_indulgance_before_offence ADD CONSTRAINT pk_ir_indulgance_before_offence_id PRIMARY KEY (id);

-- ========== PENDING FK TABLE (1 SPECIAL CASE) ==========
-- Add PRIMARY KEY and partial unique index for ir_pending_fk
ALTER TABLE public.ir_pending_fk ADD CONSTRAINT pk_ir_pending_fk_id PRIMARY KEY (id);
CREATE UNIQUE INDEX idx_ir_pending_fk_ir_id_unresolved 
  ON public.ir_pending_fk (ir_id) 
  WHERE NOT resolved;

-- ========== VERIFICATION ==========
-- Verify all 10 constraints were added
SELECT table_name, constraint_name, constraint_type 
FROM information_schema.table_constraints 
WHERE constraint_type IN ('PRIMARY KEY', 'UNIQUE') 
  AND table_name IN ('crimes', 'accused', 'persons', 'properties', 'interrogation_reports', 'disposal',
                      'ir_regular_habits', 'ir_media', 'ir_interrogation_report_refs', 
                      'ir_indulgance_before_offence', 'ir_pending_fk')
ORDER BY table_name;

-- Verify partial unique index exists
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'ir_pending_fk' AND indexname = 'idx_ir_pending_fk_ir_id_unresolved';
EOF
```

Expected output shows PRIMARY KEY and UNIQUE constraints on 11 tables:
```
 table_name                        | constraint_name                   | constraint_type
-----------------------------------+-----------------------------------+-----------------
 accused                           | pk_accused_id                     | PRIMARY KEY
 crimes                            | pk_crimes_id                      | PRIMARY KEY
 disposal                          | pk_disposal_pkey                  | PRIMARY KEY
 disposal                          | uk_disposal_composite             | UNIQUE
 interrogation_reports             | pk_ir_id                          | PRIMARY KEY
 ir_indulgance_before_offence      | pk_ir_indulgance_before_offence_id| PRIMARY KEY
 ir_interrogation_report_refs      | pk_ir_interrogation_report_refs_id| PRIMARY KEY
 ir_media                          | pk_ir_media_id                    | PRIMARY KEY
 ir_pending_fk                      | pk_ir_pending_fk_id               | PRIMARY KEY
 ir_regular_habits                 | pk_ir_regular_habits_id           | PRIMARY KEY
 persons                           | pk_persons_id                     | PRIMARY KEY
 properties                        | pk_properties_id                  | PRIMARY KEY
```

**Why these fixes are needed:**

**Primary Tables (6):**
- `crimes`: ON CONFLICT (crime_id) at etl_crimes.py:673-696
- `accused`: ON CONFLICT (accused_id) at etl_accused.py:1448-1471  
- `persons`: PRIMARY KEY needed as target for accused.person_id foreign key
- `properties`: ON CONFLICT (property_id) at etl_properties.py
- `interrogation_reports`: ON CONFLICT (ir_id) at etl_ir.py
- `disposal`: ON CONFLICT (crime_id, disposal_type, disposed_at) at etl_disposal.py

**IR Subtables (4):**
- `ir_regular_habits`: ON CONFLICT DO NOTHING at ir_etl_enhanced.py:650
- `ir_media`: ON CONFLICT DO NOTHING at ir_etl_enhanced.py:878
- `ir_interrogation_report_refs`: ON CONFLICT DO NOTHING at ir_etl_enhanced.py:890
- `ir_indulgance_before_offence`: ON CONFLICT DO NOTHING at ir_etl_enhanced.py:919

**Pending FK Table (1):**
- `ir_pending_fk`: ON CONFLICT (ir_id) WHERE NOT resolved at ir_etl_enhanced.py:277

## Step 1: Verify etl-mongo-to-postgresql is NOT Running

On remote server:
```bash
ps aux | grep -E "etl-mongo|mongo" | grep -v grep
```

Expected: No output (process not running)

If running, stop it:
```bash
pkill -f etl-mongo-to-postgresql
```

## Step 2: Pull Latest Changes on Remote Server

```bash
cd /data-drive/etl-process-dev
git pull
```

## Step 3: Clear ETL Checkpoints

This allows backfill to start from June 2022:

```bash
chmod +x /home/ashish-ratna/DOPAMS-ETL/PREPARE_BACKFILL.sh
/home/ashish-ratna/DOPAMS-ETL/PREPARE_BACKFILL.sh
```

Or manually via psql:
```bash
psql -h 192.168.103.106 -U dev_dopamas -d dev-3 -c "DELETE FROM etl_run_state;"
```

## Step 4: Start Backfill

```bash
cd /data-drive/etl-process-dev
nohup python3 etl_master/master_etl.py > etl_master/master_run.out 2>&1 &
```

Or run in foreground for monitoring:
```bash
python3 etl_master/master_etl.py
```

## Step 5: Monitor Backfill Progress

Real-time monitoring:
```bash
tail -f /data-drive/etl-process-dev/etl_master/master_run.out
```

Or check specific step logs:
```bash
# Latest log directory
LATEST_LOG=$(find /data-drive/etl-process-dev/etl_master/logs -type d -name "*" | sort -r | head -1)
tail -f "$LATEST_LOG/master.log"

# Specific step (e.g., crimes)
tail -f "$LATEST_LOG/crimes/execution.log"
```

## Step 6: Verify Data Population

Once backfill completes:

```bash
psql -h 192.168.103.106 -U dev_dopamas -d dev-3 <<EOF
-- Check data in main tables
SELECT 'Crimes' as table_name, COUNT(*) as record_count FROM crimes
UNION ALL
SELECT 'Accused', COUNT(*) FROM accused
UNION ALL
SELECT 'Persons', COUNT(*) FROM persons
UNION ALL
SELECT 'Brief Facts AI', COUNT(*) FROM brief_facts_ai
UNION ALL
SELECT 'Brief Facts Drug', COUNT(*) FROM brief_facts_drug
UNION ALL
SELECT 'Disposal', COUNT(*) FROM disposal
UNION ALL
SELECT 'Arrests', COUNT(*) FROM arrests
UNION ALL
SELECT 'MO Seizures', COUNT(*) FROM mo_seizures
ORDER BY record_count DESC;

-- Check checkpoint state after backfill
SELECT module_name, last_successful_end FROM etl_run_state ORDER BY last_successful_end DESC;
EOF
```

## Step 7: Troubleshooting

### If backfill fails at a specific step:

1. Check the step's execution log:
   ```bash
   cat "$LATEST_LOG/{step_name}/execution.log"
   ```

2. Common issues:
   - **API timeout**: Increase `API_TIMEOUT` in .env (default: 180s)
   - **Database connection**: Verify POSTGRES_* environment variables
   - **Memory**: Monitor system resources during large backfill
   - **Disk space**: Ensure `/data-drive/` has sufficient space for logs

3. Resume from last successful step:
   ```bash
   # Check last successful order
   tail -100 "$LATEST_LOG/master.log" | grep "SUCCESS"
   
   # Resume from order N (e.g., order 5):
   python3 etl_master/master_etl.py --start-order 5
   ```

## Step 8: Post-Backfill

Once backfill is complete:

1. Schedule daily ETL runs:
   ```bash
   # Add to crontab for daily execution
   # Example: Run at 2 AM IST every day
   0 20 * * * cd /data-drive/etl-process-dev && python3 etl_master/master_etl.py
   ```

2. Monitor first daily run to verify incremental mode works

3. Check for any unhandled API updates (tables with modified dates that need UPDATE logic)

## Estimated Timeline

- **Backfill duration**: 4-24 hours (depends on data volume and API responsiveness)
- **Data volume**: June 2022 to Apr 2026 = ~4 years of data
- **Expected data size**: Potentially 1-10 million records across all tables

## Safety Notes

- Backfill is **read-only** (API calls only, no data deletion)
- Can be safely re-run if interrupted
- Checkpoints prevent duplicate inserts if re-run
- All logs preserved for audit trail
