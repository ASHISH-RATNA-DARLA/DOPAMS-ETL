# ETL Test Scripts

Two test scripts are provided to run each ETL in master order with comprehensive logging.

## Scripts Overview

### 1. `test_etl_from_config.sh` (RECOMMENDED)
**Most flexible - reads from your input.txt configuration**

- Parses `input.txt` to determine ETL steps in order
- Handles `[Order X]`, `Name:`, and `Cd:` directives
- Automatically activates virtual environments
- Saves logs in both:
  - Each ETL's root directory: `etl_*/etl_test_YYYYMMDD_HHMMSS.log`
  - Master directory: `/data-drive/etl-process-dev/test_logs_YYYYMMDD_HHMMSS/`
- Better error handling and color-coded output

### 2. `test_each_etl.sh` (ALTERNATIVE)
**Hardcoded step list - faster to run**

- Defines ETL steps directly (crimes, accused, persons, etc.)
- Useful if input.txt changes frequently
- Same logging structure as config version

## Installation

Copy scripts to remote server:

```bash
scp test_etl_from_config.sh eagle@192.168.103.182:/data-drive/etl-process-dev/
scp test_each_etl.sh eagle@192.168.103.182:/data-drive/etl-process-dev/

# SSH to server
ssh eagle@192.168.103.182

# Make scripts executable
cd /data-drive/etl-process-dev
chmod +x test_etl_from_config.sh test_each_etl.sh
```

## Usage

### Quick Start (from /data-drive/etl-process-dev)

```bash
# Using config file (recommended)
./test_etl_from_config.sh

# Using hardcoded steps
./test_each_etl.sh
```

### With Custom Paths

```bash
# Test specific directory
./test_etl_from_config.sh /custom/path /custom/path/input.txt

# The script will use defaults if paths are omitted
```

### Full Example

```bash
ssh eagle@192.168.103.182

cd /data-drive/etl-process-dev

# First, apply the database constraint fixes
psql -h 192.168.103.106 -U dev_dopamas -d dev-3 << 'EOF'
ALTER TABLE public.crimes ADD CONSTRAINT pk_crimes_id PRIMARY KEY (crime_id);
ALTER TABLE public.accused ADD CONSTRAINT pk_accused_id PRIMARY KEY (accused_id);
ALTER TABLE public.persons ADD CONSTRAINT pk_persons_id PRIMARY KEY (person_id);
ALTER TABLE public.properties ADD CONSTRAINT pk_properties_id PRIMARY KEY (property_id);
ALTER TABLE public.interrogation_reports ADD CONSTRAINT pk_ir_id PRIMARY KEY (interrogation_report_id);
ALTER TABLE public.disposal ADD CONSTRAINT uk_disposal_composite UNIQUE (crime_id, disposal_type, disposed_at);
EOF

# Clear checkpoints
psql -h 192.168.103.106 -U dev_dopamas -d dev-3 -c "DELETE FROM etl_run_state;"

# Run the test suite
./test_etl_from_config.sh

# Monitor logs in real-time (in another terminal)
tail -f test_logs_*/crimes_test_*.log
```

## Output Structure

### Master Log Directory
```
test_logs_20260416_142530/
├── 01_crimes_test_20260416_142530.log
├── 02_accused_test_20260416_142530.log
├── 03_persons_test_20260416_142530.log
├── 04_disposal_test_20260416_142530.log
├── 05_arrests_test_20260416_142530.log
├── 06_mo_seizures_test_20260416_142530.log
├── 07_chargesheet_test_20260416_142530.log
├── 08_interrogation_reports_test_20260416_142530.log
├── 09_brief_facts_ai_test_20260416_142530.log
└── 10_properties_test_20260416_142530.log
```

### Individual ETL Logs
```
etl-crimes/
├── etl_test_20260416_142530.log  (from test script)
└── other logs...

etl-accused/
├── etl_test_20260416_142530.log  (from test script)
└── other logs...
```

## Log Format

Each log file contains:
```
========================================================================
ETL Step: Crimes
Order: 1
Working Directory: ./etl-crimes
Start Time: 2026-04-16 14:25:30
Commands: 1
========================================================================

[INFO] Activating virtual environment...
[INFO] Python: /data-drive/etl-process-dev/etl-crimes/venv/bin/python3
[INFO] Python Version: Python 3.11.x

───────────────────────────────────────────────────────────────────────
Command 1: python3 etl_crimes.py
───────────────────────────────────────────────────────────────────────

... ETL output ...

[OK] Command completed successfully

========================================================================
Step Status: SUCCESS
End Time: 2026-04-16 14:26:45
========================================================================
```

## Viewing Logs

### Real-time monitoring (during test run)
```bash
# Watch main test output
tail -f test_logs_*/master.log

# Watch specific ETL
tail -f test_logs_*/01_crimes_test_*.log

# Watch step-specific logs
tail -f etl-crimes/etl_test_*.log
```

### After test completion
```bash
# View summary
./test_etl_from_config.sh  # Shows summary with file paths

# View specific ETL logs
cat test_logs_*/02_accused_test_*.log

# Check for errors
grep -r "ERROR\|FAILED\|✗" test_logs_*/

# Count records inserted
grep "record_count\|inserted\|updated" test_logs_*/*.log
```

## Environment Variables

The scripts automatically:
- Activate virtual environments (`venv/bin/activate`)
- Set timezone to Asia/Kolkata (inherited from system)
- Preserve environment variables from parent shell
- Load `.env` files if present

### Manually set environment
```bash
# Before running script
export DOPAMS_ENV=dev
export API_TIMEOUT=300

./test_etl_from_config.sh
```

## Troubleshooting

### Script fails with "No such file or directory"
```bash
# Make sure you're in the right directory
pwd  # Should be /data-drive/etl-process-dev

# Check config file exists
ls -l input.txt

# Verify script is executable
chmod +x test_etl_from_config.sh
```

### Virtual environment not activated
```bash
# Check venv structure
ls etl-crimes/venv/bin/activate
ls etl-accused/venv/bin/activate

# Manually activate and test
source etl-crimes/venv/bin/activate
python3 --version
deactivate
```

### ETL fails silently
```bash
# Check the actual log file (not just console output)
cat test_logs_*/01_crimes_test_*.log | grep -A 10 "ERROR"

# Check database constraints were applied
psql -h 192.168.103.106 -U dev_dopamas -d dev-3 << 'EOF'
SELECT table_name, constraint_name FROM information_schema.table_constraints 
WHERE constraint_type IN ('PRIMARY KEY', 'UNIQUE') 
  AND table_name IN ('crimes', 'accused', 'persons', 'properties', 'interrogation_reports', 'disposal');
EOF
```

## Running Individual ETLs

If you want to run just one ETL:

```bash
cd /data-drive/etl-process-dev/etl-crimes
source venv/bin/activate
python3 etl_crimes.py

# View the log
cat etl_test_*.log
```

## Integration with CI/CD

For automated testing:

```bash
#!/bin/bash
# run_etl_tests.sh (for cron or CI)

cd /data-drive/etl-process-dev

# Run test
./test_etl_from_config.sh > test_run.txt 2>&1
EXIT_CODE=$?

# Send notification
if [ $EXIT_CODE -ne 0 ]; then
    echo "ETL tests failed - check logs"
    # Send email, Slack, etc.
    exit 1
fi

echo "All ETL tests passed"
exit 0
```

## Script Features

Both scripts include:
- ✅ Automatic virtual environment detection and activation
- ✅ Color-coded output (success/error/info/warning)
- ✅ Per-step timing measurements
- ✅ Dual logging (both local and master directory)
- ✅ Comprehensive error handling
- ✅ Step-by-step execution with summary
- ✅ Exit codes for CI/CD integration
- ✅ Timestamp-based log organization
- ✅ Configuration parsing (for config version)

## Performance Notes

- First run may be slower (venv initialization, cache build)
- Subsequent runs are faster (cached dependencies)
- Database constraint checks add minimal overhead
- API calls are the bottleneck (depends on API responsiveness)

Typical execution times:
- crimes: 2-5 minutes
- accused: 2-5 minutes
- persons: 5-15 minutes (depends on API response time)
- Others: 1-10 minutes each
- **Total: 20-60 minutes for full backfill**

## Next Steps After Testing

1. Review logs in `test_logs_YYYYMMDD_HHMMSS/`
2. Check database record counts
3. If all steps pass:
   - Clear checkpoints again (optional)
   - Run full backfill: `python3 etl_master/master_etl.py`
   - Monitor production mode execution
4. If any step fails:
   - Check log file for error details
   - Fix the issue (check CONSTRAINT_FIXES.md)
   - Re-run just that step, or the whole test

## References

- CONSTRAINT_FIXES.md - Root cause analysis of silent failures
- BACKFILL_EXECUTION.md - Full backfill execution guide
- BACKFILL_PLAN.md - Overall strategy and checkpoint system
