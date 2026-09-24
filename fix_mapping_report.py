import re

with open('cctns-v2_schema_mapping_report.md', 'r') as f:
    content = f.read()

# Update revision note
new_note = """**Revision 3 (this version):** Consolidated `properties_pending_fk` and `ir_pending_fk` into `etl_bookkeeping` (kind='fk_retry'). Furthermore, all 23 interrogation report child tables (e.g. `ir_associate_details`, `ir_consumer_details`, etc.) were removed, and their CCTNS JSON response mappings were merged directly into `interrogation_reports` as JSONB columns (e.g. `associate_details jsonb`). ETL code was updated to match, maintaining full historical coverage with no runtime references to the removed tables."""
content = re.sub(r"\*\*Revision 2 \(this version\).*?affected by the consolidation\.", new_note, content, flags=re.DOTALL)

# Remove mention of properties_pending_fk and ir_pending_fk from text
content = re.sub(r" \(\+ properties_pending_fk, ir_pending_fk\)", "", content)
content = re.sub(r", `properties_pending_fk`", "", content)
content = re.sub(r"; rows whose `crime_id` isn't yet in `crimes` are queued in `properties_pending_fk`", "; rows whose `crime_id` isn't yet in `crimes` are queued in `etl_bookkeeping`", content)

# Update row 16 (Interrogation Reports)
new_ir_row = "| 16 | `GET /interrogation-reports` (bulk) | `16_interrogation_reports_endpoint_to_get_interrogation_details_between_dates.json` | `interrogation_reports`, `file_media_bookkeeping` | `INTERROGATION_REPORT_ID, CRIME_ID, PERSON_ID`, plus nested objects: `PHYSICAL_FEATURES`, `SOCIO_ECONOMIC_PROFILE`, `COMMISSION_OF_OFFENCE`, `SHARE_OF_AMOUNT_SPENT`, `PRESENT_WHEREABOUTS` flattened into columns. Additionally, 23 child arrays (`FAMILY_HISTORY`, `LOCAL_CONTACTS`, `REGULAR_HABITS`, `TYPES_OF_DRUGS`, etc.) are mapped into dedicated `jsonb` columns directly on `interrogation_reports` (consolidated from 23 former child tables). `MEDIA` array is parked in `file_media_bookkeeping`. — `etl-ir/ir_etl.py:insert_main_record()`; FK validation uses `etl_fk_retry_queue` / `etl_bookkeeping`."
content = re.sub(r"\| 16 \| `GET /interrogation-reports`.*?\|", new_ir_row + "\n|", content, count=1, flags=re.DOTALL)

# Remove IR child tables + pending_fks from the Classification table ONLY
ir_child_tables = [
    'ir_associate_details', 'ir_consumer_details', 'ir_conviction_acquittal',
    'ir_defence_counsel', 'ir_dopams_links', 'ir_execution_of_nbw',
    'ir_family_history', 'ir_financial_history', 'ir_indulgance_before_offence',
    'ir_interrogation_report_refs', 'ir_jail_sentence', 'ir_local_contacts',
    'ir_modus_operandi', 'ir_new_gang_formation', 'ir_pending_nbw',
    'ir_previous_offences_confessed', 'ir_property_disposal', 'ir_regular_habits',
    'ir_regularization_transit_warrants', 'ir_shelter', 'ir_sim_details',
    'ir_sureties', 'ir_types_of_drugs', 'properties_pending_fk', 'ir_pending_fk'
]

lines = content.split('\n')
new_lines = []
in_classification = False
for line in lines:
    if "## 3. Current Table Classification (Updated)" in line:
        in_classification = True
    
    skip = False
    if in_classification and "|" in line:
        for t in ir_child_tables:
            if f"`{t}`" in line:
                skip = True
                break
    
    if not skip:
        new_lines.append(line)
content = '\n'.join(new_lines)

# Remove the specific standalone pending_fk row in classification
content = re.sub(r"\| `properties_pending_fk`, `ir_pending_fk`.*?\|\n", "", content)
content = re.sub(r"distinct mechanism\): `properties_pending_fk`, `ir_pending_fk` \(per-entity FK retry, ", "distinct mechanism): ", content)

# Update before/after counts
content = re.sub(r"The original pipeline relied on \*\*53\*\* tables\..*?Now down to \*\*45\*\*\.", "The original pipeline relied on **53** tables. Revision 2 brought it to 45. Revision 3 brings it down to **20**.", content, flags=re.DOTALL)

# Add a Section 5 for the new consolidation
section_5 = """
### 5. Final Consolidation (Revision 3)

The remaining ETL bookkeeping tables (`properties_pending_fk`, `ir_pending_fk`) were completely removed and folded into `etl_bookkeeping` (`kind = 'fk_retry'`), facilitated by standardizing their retry logic onto the `etl_fk_retry_queue` module.

The 23 IR child tables which existed solely to normalize nested array objects from the CCTNS response were eliminated. The data is now retained structurally as PostgreSQL `jsonb` array columns directly on the `interrogation_reports` parent table.

The repository was scrubbed of runtime references, and ETL tests validate insertion syntax. 
"""

content += section_5

with open('cctns-v2_schema_mapping_report.md', 'w') as f:
    f.write(content)
