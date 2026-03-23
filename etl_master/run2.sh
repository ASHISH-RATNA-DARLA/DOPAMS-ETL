#!/bin/bash

# Activate the virtual environment
source /data-drive/etl-process-dev/venv/bin/activate

# Set log file path
LOGFILE="/data-drive/etl-process-dev/etl_master/order_27_30_combined.log"

# Order 27: update_file_id
cd /data-drive/etl-process-dev/etl-files/etl_pipeline_files
python3 main_standalone.py >> "$LOGFILE" 2>&1

# Order 28: files_download_media_server
cd /data-drive/etl-process-dev/etl-files/etl_files_media_server
python3 -m etl_files_media_server.main >> "$LOGFILE" 2>&1

# Order 29: update_file_extentions
cd /data-drive/etl-process-dev/etl-files/update_file_urls_with_extensions
python3 update_file_urls_with_extensions.py >> "$LOGFILE" 2>&1

# Order 30: refresh_views
cd /data-drive/etl-process-dev/etl_refresh_views
python3 views_refresh_sql.py >> "$LOGFILE" 2>&1

echo "Orders 27-30 completed. Logs in $LOGFILE"