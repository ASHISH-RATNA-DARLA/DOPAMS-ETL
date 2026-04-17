#!/bin/bash
# Start Files Media Server Downloader with tomcat group

# Change to the script directory
cd "$(dirname "$0")"

# Activate virtual environment if it exists
if [ -f "../../venv/bin/activate" ]; then
    source ../../venv/bin/activate
elif [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
fi

# Check if user is in tomcat group
if ! groups | grep -q tomcat; then
    echo "ERROR: User is not in tomcat group. Please log out and log back in after running: sudo usermod -a -G tomcat $(whoami)"
    exit 1
fi

# Start the downloader with tomcat as primary group
# Using 'newgrp tomcat' to switch to tomcat group, then run the command
# Note: newgrp starts a new shell, so we need to run everything in one command
newgrp tomcat <<EOFNEWGRP
cd "$(pwd)"
nohup python3 -m etl_files_media_server.main > etl_files_media_server.log 2>&1 &
echo "Downloader started with PID: \$!"
echo "Check logs with: tail -f etl_files_media_server.log"
EOFNEWGRP

