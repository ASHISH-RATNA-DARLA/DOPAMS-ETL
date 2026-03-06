#!/bin/bash

source /data-drive/etl-process-dev/venv/bin/activate
cd /data-drive/etl-process-dev/etl_master

# Run master ETL in background using nohup.
# Full orchestrator output goes to master_etl_full.log.
nohup python3 /data-drive/etl-process-dev/etl_master/master_etl.py \
	> /data-drive/etl-process-dev/etl_master/master_etl_full.log 2>&1 &

echo "Master ETL started in background (PID: $!)"

