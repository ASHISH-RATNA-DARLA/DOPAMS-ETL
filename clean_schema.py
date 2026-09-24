import re

with open('cctns-v2_schema.sql', 'r') as f:
    sql = f.read()

# Drop tables
tables_to_remove = [
    'property_additional_details',
    'chargesheet_acts',
    'chargesheet_acts_sections',
    'chargesheet_accused'
]

for table in tables_to_remove:
    # Remove CREATE TABLE
    sql = re.sub(rf"CREATE TABLE public\.{table} \([^;]+;\n", "", sql, flags=re.MULTILINE)
    # Remove indexes, FKs, triggers, comments related to this table
    sql = re.sub(rf"CREATE[^;]+ON public\.{table}[^;]+;\n", "", sql, flags=re.MULTILINE|re.IGNORECASE)
    sql = re.sub(rf"ALTER TABLE (ONLY )?public\.{table}[^;]+;\n", "", sql, flags=re.MULTILINE)
    sql = re.sub(rf"COMMENT ON TABLE public\.{table}[^;]+;\n", "", sql, flags=re.MULTILINE)

# Remove media column from properties and mo_seizures
sql = re.sub(r"    media jsonb DEFAULT '\[\]'::jsonb,\n", "", sql)

# Add JSONB columns to chargesheets
chargesheets_table_regex = re.compile(r"(CREATE TABLE public\.chargesheets \((?:.*?\n)+?)(    CONSTRAINT chargesheets_pkey PRIMARY KEY \(chargesheet_id\)\n\);)", flags=re.MULTILINE)
new_columns = "    acts_and_sections jsonb,\n    accused_particulars jsonb,\n"
sql = chargesheets_table_regex.sub(r"\1" + new_columns + r"\2", sql)

with open('cctns-v2_schema.sql', 'w') as f:
    f.write(sql)
print("Schema cleaned")
