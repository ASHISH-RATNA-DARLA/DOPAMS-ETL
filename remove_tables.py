import re

with open('cctns-v2_schema.sql', 'r') as f:
    sql = f.read()

tables_to_remove = [
    'properties_pending_fk', 'ir_pending_fk',
    'ir_associate_details', 'ir_consumer_details', 'ir_conviction_acquittal',
    'ir_defence_counsel', 'ir_dopams_links', 'ir_execution_of_nbw',
    'ir_family_history', 'ir_financial_history', 'ir_indulgance_before_offence',
    'ir_interrogation_report_refs', 'ir_jail_sentence', 'ir_local_contacts',
    'ir_modus_operandi', 'ir_new_gang_formation', 'ir_pending_nbw',
    'ir_previous_offences_confessed', 'ir_property_disposal', 'ir_regular_habits',
    'ir_regularization_transit_warrants', 'ir_shelter', 'ir_sim_details',
    'ir_sureties', 'ir_types_of_drugs'
]

for table in tables_to_remove:
    # Remove CREATE TABLE
    sql = re.sub(rf"CREATE TABLE public\.{table} \([^;]+;\n", "", sql, flags=re.MULTILINE)
    # Remove indexes, FKs, triggers, comments related to this table
    sql = re.sub(rf"CREATE[^;]+ON public\.{table}[^;]+;\n", "", sql, flags=re.MULTILINE|re.IGNORECASE)
    sql = re.sub(rf"ALTER TABLE (ONLY )?public\.{table}[^;]+;\n", "", sql, flags=re.MULTILINE)
    sql = re.sub(rf"COMMENT ON TABLE public\.{table}[^;]+;\n", "", sql, flags=re.MULTILINE)

# Also add the new JSONB columns to interrogation_reports
ir_table_regex = re.compile(r"(CREATE TABLE public\.interrogation_reports \((?:.*?\n)+?)(    CONSTRAINT interrogation_reports_pkey PRIMARY KEY \(interrogation_report_id\)\n\);)", flags=re.MULTILINE)
new_columns = ""
for table in tables_to_remove[2:]: # skip the two pending_fks
    col_name = table.replace('ir_', '')
    new_columns += f"    {col_name} jsonb,\n"

sql = ir_table_regex.sub(r"\1" + new_columns + r"\2", sql)

with open('cctns-v2_schema_new.sql', 'w') as f:
    f.write(sql)
print("Done")
