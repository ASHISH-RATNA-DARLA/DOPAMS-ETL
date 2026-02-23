#!/bin/bash
cd /data-drive/etl-process-dev/etl_refresh_views
source /data-drive/etl-process-dev/venv/bin/activate
python3 /data-drive/etl-process-dev/etl_refresh_views/views_refresh_sql.py
