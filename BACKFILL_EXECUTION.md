# ETL Backfill Execution Guide

## Pre-Backfill Checklist

✅ Database is truncated (except Knowledge Base)
✅ etl_persons.py isoformat bug is fixed
✅ etl-crimes/config.py updated (start: June 2022, end: Apr 16, 2026)
⚠️ Need to: Clear etl_run_state checkpoints on remote server
⚠️ Need to: Confirm etl-mongo-to-postgresql is not running

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
