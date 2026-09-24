import re

with open('etl_chargesheets/etl_chargesheets.py', 'r') as f:
    content = f.read()

# Update module docstring
content = re.sub(
    r"Handles: chargesheets, chargesheet_acts, chargesheet_acts_sections, chargesheet_accused,",
    "Handles: chargesheets,",
    content
)

# Update transform_chargesheet to include acts and accused
content = re.sub(
    r"(\s+)'etl_run_id': etl_run_id\n(\s+)}(\n)",
    r"\1'etl_run_id': etl_run_id,\n\1'acts_and_sections': json.dumps(record.get('actsAndSections', [])),\n\1'accused_particulars': json.dumps(record.get('accusedParticulars', []))\n\2}\3",
    content
)

# Add acts_and_sections, accused_particulars to main insert
content = re.sub(
    r"'fetched_at', 'etl_run_id'",
    r"'fetched_at', 'etl_run_id', 'acts_and_sections', 'accused_particulars'",
    content
)

# Remove the calls to insert acts and accused
content = re.sub(
    r"\s+# Insert child records.*?insert_chargesheet_accused\(conn, record, row_id\)",
    "",
    content,
    flags=re.DOTALL
)

# Remove the child insert functions entirely
content = re.sub(
    r"def insert_chargesheet_acts\(conn, record, chargesheet_uuid\):.*?def insert_chargesheet_file",
    "def insert_chargesheet_file",
    content,
    flags=re.DOTALL
)

content = re.sub(
    r"def insert_chargesheet_accused\(conn, record, chargesheet_uuid\):.*?def get_existing_charge_sheet_ids",
    "def get_existing_charge_sheet_ids",
    content,
    flags=re.DOTALL
)

with open('etl_chargesheets/etl_chargesheets.py', 'w') as f:
    f.write(content)
print("Updated etl_chargesheets.py")
