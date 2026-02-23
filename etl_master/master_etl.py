import os
import sys
import subprocess
import logging
import argparse
import re
from datetime import datetime
import time

# Configure Logging
log_file = 'master_etl.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def parse_input_file(file_path):
    """
    Parses the input configuration file to extract ordered process blocks.
    
    Args:
        file_path (str): Path to the configuration file.
        
    Returns:
        list of dict: A list of process blocks, e.g., [{'order': '1', 'commands': [...]}]
    """
    if not os.path.exists(file_path):
        logger.error(f"Configuration file not found: {file_path}")
        sys.exit(1)

    with open(file_path, 'r') as f:
        lines = f.readlines()

    processes = []
    current_block = None
    
    # Regex to identify order headers like [Order 1], [Order 2]
    header_pattern = re.compile(r'^\[Order\s+(\d+)\]', re.IGNORECASE)

    for line in lines:
        line = line.strip()
        
        # Skip empty lines
        if not line:
            continue
            
        # Check for headers
        match = header_pattern.match(line)
        if match:
            # Save previous block if it exists
            if current_block:
                processes.append(current_block)
            
            # Start new block
            current_block = {
                'order': match.group(1),
                'name': None,
                'commands': []
            }
            continue

        # If we are inside a block, add commands
        if current_block:
            # Skip comments
            if line.startswith('#'):
                continue
            
            # Check if this is the first line in the block (potentially a name)
            # Heuristic: If it's a single word and not a command start
            if not current_block['commands'] and not current_block['name']:
                # If line is single word and doesn't look like a path/command
                # OR if the user format is consistently Name then Command
                # We'll allow spaces in names too, so we check for command indicators
                is_command_like = (
                    line.startswith('/') or 
                    line.startswith('./') or 
                    line.startswith('cd ') or 
                    line.startswith('source ') or 
                    line.startswith('python') or 
                    '=' in line
                )
                
                if not is_command_like:
                    current_block['name'] = line
                    continue

            current_block['commands'].append(line)
            
    # Append the last block
    if current_block:
        processes.append(current_block)

    # Validate commands in all processes
    for process in processes:
        for command in process['commands']:
            # Heuristic check for missing 'cd'
            # If command looks like an absolute path and doesn't start with typical command indicators
            if command.startswith('/') and ' ' not in command:
                logger.warning(
                    f"Potential Issue in [Order {process.get('order')}]: "
                    f"Command '{command}' looks like a path but lacks 'cd'. "
                    f"Verify if this should be 'cd {command}'."
                )
        
    return processes

def execute_process(process):
    """
    Executes a single process block.
    
    Args:
        process (dict): Process info containing 'order', 'name', and 'commands'.
    """
    order = process['order']
    name = process.get('name', 'Unnamed Process')
    commands = process['commands']
    
    logger.info(f"--- Starting Process [Order {order}: {name}] ---")
    
    if not commands:
        logger.warning(f"Process [Order {order}: {name}] has no commands. Skipping.")
        return True

    # Combine commands into a single shell command string.
    # We join with ' && ' so that if one step fails, the whole block fails immediately.
    full_command = " && ".join(commands)
    
    logger.info(f"Executing: {full_command}")
    
    try:
        # executable='/bin/bash' is crucial for 'source' to work
        subprocess.run(
            full_command, 
            shell=True, 
            executable='/bin/bash', 
            check=True,
            text=True
        )
        logger.info(f"--- Completed Process [Order {order}: {name}] Successfully ---\n")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Process [Order {order}: {name}] FAILED with exit code {e.returncode}")
        logger.error("Stopping Master ETL execution.")
        return False

def main():
    parser = argparse.ArgumentParser(description="Master ETL Orchestrator")
    parser.add_argument('--config', default='input.txt', help='Path to process configuration file')
    args = parser.parse_args()

    logger.info("Starting Master ETL Orchestrator")
    logger.info(f"Reading configuration from: {args.config}")

    processes = parse_input_file(args.config)
    
    if not processes:
        logger.warning("No process blocks found in configuration file.")
        logger.warning("Ensure blocks start with [Order X]")
        return

    logger.info(f"Found {len(processes)} processes to execute.")

    for process in processes:
        success = execute_process(process)
        if not success:
            sys.exit(1)
        
        # Add a delay between processes to avoid rate limiting/connection issues
        logger.info("Waiting 5 seconds before next process...")
        time.sleep(5)

    logger.info("All ETL processes finished successfully.")

if __name__ == "__main__":
    main()

