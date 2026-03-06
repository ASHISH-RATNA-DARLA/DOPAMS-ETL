#!/usr/bin/env python3
"""
ETL Validation Test Suite (smoke mode)
Runs each ETL subprocess in a minimal, fast validation mode.
"""
import subprocess
import logging
import sys
import os
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Test record IDs from Keep-1-record.sql
TEST_CRIME_ID = '62aa9b9ea2d2490c539be447'
TEST_PERSON_ID = '62ab45de447aa0823c735af1'
TEST_PS_CODE = '2022057'
TEST_CHARGESHEET_ID = '0294b57b-adf2-4d2a-9aa0-2808f88452fe'

# Define validation steps in current order
VALIDATION_STEPS = [
    {
        'name': 'Hierarchy',
        'command': 'cd etl-hierarchy && python3 etl_hierarchy.py',
        'checks': {'hierarchy': 1},
        'timeout': 180
    },
    {
        'name': 'Crimes',
        'command': 'cd etl-crimes && python3 etl_crimes.py',
        'checks': {'crimes': 1},
        'timeout': 180
    },
    {
        'name': 'Class Classification',
        'command': 'cd section-wise-case-clarification && python3 process_sections.py',
        'checks': {},  # May not have own table
        'timeout': 180
    },
    {
        'name': 'Case Status',
        'command': 'cd etl_case_status && python3 update_crimes.py',
        'checks': {},
        'timeout': 180
    },
    {
        'name': 'Accused',
        'command': 'cd etl-accused && python3 etl_accused.py',
        'checks': {'accused': 0},  # May be 0 with filtered data
        'timeout': 180
    },
    {
        'name': 'Persons',
        'command': 'cd etl-persons && python3 etl_persons.py',
        'checks': {'persons': 0},  # May be 0 with filtered data
        'timeout': 180
    },
    {
        'name': 'State/Country Update',
        'command': 'cd update-state-country && python3 update-state-country.py',
        'checks': {},
        'timeout': 180
    },
    {
        'name': 'Domicile Classification',
        'command': 'cd domicile_classification && python3 domicile_classifier.py',
        'checks': {},
        'timeout': 180
    },
    {
        'name': 'Properties',
        'command': 'cd etl-properties && python3 etl_properties.py',
        'checks': {'properties': 0},
        'timeout': 180
    },
    {
        'name': 'Interrogation Reports',
        'command': 'cd etl-ir && python3 ir_etl.py',
        'checks': {'interrogation_reports': 0},
        'timeout': 180
    },
    {
        'name': 'Disposal',
        'command': 'cd etl-disposal && python3 etl_disposal.py',
        'checks': {'disposal': 0},
        'timeout': 180
    },
    {
        'name': 'Arrests',
        'command': 'cd etl_arrests && python3 etl_arrests.py',
        'checks': {'arrests': 0},
        'timeout': 180
    },
    {
        'name': 'MO Seizures',
        'command': 'cd etl_mo_seizures && python3 etl_mo_seizure.py',
        'checks': {'mo_seizures': 0},
        'timeout': 180
    },
    {
        'name': 'Chargesheets',
        'command': 'cd etl_chargesheets && python3 etl_chargesheets.py',
        'checks': {'chargesheets': 0},
        'timeout': 180
    },
    {
        'name': 'Updated Chargesheet',
        'command': 'cd etl_updated_chargesheet && python3 etl_update_chargesheet.py',
        'checks': {},
        'timeout': 180
    },
    {
        'name': 'FSL Case Property',
        'command': 'cd etl_fsl_case_property && python3 etl_fsl_case_property.py',
        'checks': {'fsl_case_property': 0},
        'timeout': 180
    },
    {
        'name': 'Brief Facts - Accused (single API smoke)',
        'command': "cd brief_facts_accused && python3 -c \"from extractor import extract_accused_names_pass1; print(extract_accused_names_pass1('A1 Rahul sold ganja'))\"",
        'checks': {'brief_facts_accused': 0},
        'timeout': 180
    },
    {
        'name': 'Brief Facts - Drugs (single API smoke)',
        'command': 'cd brief_facts_drugs && python3 extractor.py',
        'checks': {'brief_facts_drug': 0},
        'timeout': 180
    },
]

def run_etl_step(step_name, command, timeout_seconds=180):
    """Run single ETL step and return success status"""
    logger.info(f"\n▶ Running: {step_name}")
    logger.info(f"  Command: {command}")
    
    try:
        step_env = os.environ.copy()
        # Best-effort smoke controls for ETL scripts that support env-based limits.
        step_env.update({
            'SMOKE_TEST': '1',
            'SMOKE_TEST_LIMIT': '1',
            'MAX_RECORDS': '1',
            'BATCH_SIZE': '1',
            'PARALLEL_LLM_WORKERS': '3',
            'VALIDATION_CRIME_ID': TEST_CRIME_ID,
        })

        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=step_env,
        )
        
        if result.returncode == 0:
            logger.info(f"✓ {step_name} completed successfully")
            if result.stdout:
                logger.debug(f"  Output: {result.stdout[:200]}")
            return True
        else:
            logger.error(f"✗ {step_name} FAILED with exit code {result.returncode}")
            if result.stderr:
                logger.error(f"  Error: {result.stderr[:500]}")
            if result.stdout:
                logger.error(f"  Output: {result.stdout[:500]}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"✗ {step_name} timed out (>{timeout_seconds}s)")
        return False
    except Exception as e:
        logger.error(f"✗ {step_name} failed with exception: {e}")
        return False

def main():
    logger.info("=" * 70)
    logger.info("ETL VALIDATION TEST SUITE")
    logger.info(f"Test Database: Single Crime (ID: {TEST_CRIME_ID})")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("=" * 70)
    
    results = []
    
    for i, step in enumerate(VALIDATION_STEPS, 1):
        success = run_etl_step(step['name'], step['command'], step.get('timeout', 180))
        results.append({
            'order': i,
            'name': step['name'],
            'success': success
        })
        
        if not success:
            logger.warning(f"  ⚠ Continuing with next step despite failure...")
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 70)
    
    passed = sum(1 for r in results if r['success'])
    failed = sum(1 for r in results if not r['success'])
    
    for result in results:
        status = "✓ PASS" if result['success'] else "✗ FAIL"
        logger.info(f"[{result['order']:2d}] {status:8} - {result['name']}")
    
    logger.info("=" * 70)
    logger.info(f"Results: {passed} passed, {failed} failed out of {len(results)} steps")
    logger.info("=" * 70)
    
    return 0 if failed == 0 else 1

if __name__ == '__main__':
    sys.exit(main())
