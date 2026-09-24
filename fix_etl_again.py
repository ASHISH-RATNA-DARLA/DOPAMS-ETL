import re

# Fix etl_chargesheets.py
with open('etl_chargesheets/etl_chargesheets.py', 'r') as f:
    content = f.read()

content = re.sub(r"CHARGESHEET_ACTS_TABLE = .*?\n", "", content)
content = re.sub(r"CHARGESHEET_ACCUSED_TABLE = .*?\n", "", content)
content = re.sub(r"CHARGESHEET_ACTS_SECTIONS_TABLE = .*?\n", "", content)
content = re.sub(r"chargesheets/chargesheet_acts/.*?chargesheet_accused are written", "chargesheets are written", content, flags=re.DOTALL)
content = re.sub(r"\s+if not self\.table_exists\('chargesheet_acts'\):.*?continue", "", content, flags=re.DOTALL)
content = re.sub(r"\s+\{\s+'name': 'chargesheet_acts'.*?\},", "", content, flags=re.DOTALL)
content = re.sub(r"\s+\{\s+'name': 'chargesheet_accused'.*?\}", "", content, flags=re.DOTALL)
content = re.sub(r"Also handles related tables: chargesheet_acts, chargesheet_accused, and the", "Also handles related tables:", content)
content = re.sub(r"\s+self\.insert_chargesheet_accused.*?accused_data\)", "", content)
content = re.sub(r"def insert_chargesheet_accused\(.*?\):.*?def get_existing_charge_sheet_ids", "def get_existing_charge_sheet_ids", content, flags=re.DOTALL)

with open('etl_chargesheets/etl_chargesheets.py', 'w') as f:
    f.write(content)

# Fix etl_properties.py
with open('etl-properties/etl_properties.py', 'r') as f:
    content = f.read()

content = re.sub(r"PROPERTY_ADDITIONAL_DETAILS_TABLE = .*?\n", "", content)
content = re.sub(r"\s+self\.has_property_additional_details_table = False\n", "\n", content)
content = re.sub(r"\s+self\.upsert_property_additional_details\([^)]+\)\n", "\n", content)
content = re.sub(r"\s+self\.has_property_additional_details_table = self\.table_exists\(PROPERTY_ADDITIONAL_DETAILS_TABLE\)\n", "\n", content)
content = re.sub(r"\s+if self\.has_property_additional_details_table:\n\s+return True\n", "\n", content)

with open('etl-properties/etl_properties.py', 'w') as f:
    f.write(content)
print("done")
