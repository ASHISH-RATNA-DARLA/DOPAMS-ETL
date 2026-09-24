import re

with open('cctns-v2_schema_mapping_report.md', 'r') as f:
    content = f.read()

# Update final table counts
content = re.sub(r"Revision 3 brings it down to \*\*20\*\*\.", "Revision 3 brings it down to **20**. Revision 4 (final audit) brings it to **16**.", content)

# Update row 14
prop_row = "| 14 | `GET /property-details` (bulk) | `14_property_details_endpoint_to_get_property_details_created.json` | `properties`, `file_media_bookkeeping` | `PROPERTY_ID, CRIME_ID, CASE_PROPERTY_ID, PROPERTY_STATUS, RECOVERED_FROM, PLACE_OF_RECOVERY, DATE_OF_SEIZURE, NATURE, BELONGS, ESTIMATE_VALUE, RECOVERED_VALUE, PARTICULAR_OF_PROPERTY, CATEGORY, ADDITIONAL_DETAILS, MEDIA, DATE_CREATED, DATE_MODIFIED` — `etl-properties/etl_properties.py:transform_property()`; `ADDITIONAL_DETAILS` (which varies heavily in schema by CATEGORY) is stored directly as a `jsonb` column on `properties`; `media` is sent to `file_media_bookkeeping`."
content = re.sub(r"\| 14 \| `GET /property-details` \(bulk\).*?\|", prop_row + "\n|", content, count=1, flags=re.DOTALL)

# Update row 18
charge_row = "| 18 | `GET /chargesheets` (bulk) | `18_chargesheets_endpoint_to_get_chargesheets_updated_bet.json` | `chargesheets`, `file_media_bookkeeping` | `chargeSheetId, crimeId, chargeSheetNo, chargeSheetDate, chargeSheetType, courtName, isCcl, isEsigned, chargeSheetNoForIcjs, dateCreated, dateModified` → `chargesheets`; `uploadChargeSheet.fileId` → one `file_media_bookkeeping` row; `actsAndSections[]` → `acts_and_sections jsonb`; `accusedParticulars[]` → `accused_particulars jsonb`. Both nested arrays are stored directly in `chargesheets` to avoid redundant 1:N mapping tables."
content = re.sub(r"\| 18 \| `GET /chargesheets` \(bulk\).*?\|", charge_row + "\n|", content, count=1, flags=re.DOTALL)

# Delete redundant rows from Classification table
content = re.sub(r"\| `property_additional_details`.*?\|\n", "", content)
content = re.sub(r"\| `chargesheet_acts`.*?\|\n", "", content)
content = re.sub(r"\| `chargesheet_acts_sections`.*?\|\n", "", content)
content = re.sub(r"\| `chargesheet_accused`.*?\|\n", "", content)

# Add Section 6
section_6 = """
### 6. Final Audit and Consolidation (Revision 4)

In the final deep audit guided by the actual CCTNS JSON response directory (`14_property_details_*.json`, `18_chargesheets_*.json`, `16_interrogation_reports_*.json`):
* **IR child objects**: Confirmed via `jq` that the 23 `INTERROGATION_REPORTS` objects (e.g. `FAMILY_HISTORY`, `ASSOCIATE_DETAILS`) are strictly `1:N` arrays (`[ ... ]`). The 1:1 scalar objects (like `PHYSICAL_FEATURES`) were already fully flattened into proper PostgreSQL columns (e.g. `physical_beard`). Because the array objects are `1:N`, they must remain `jsonb` to comply with the architectural mandate of exactly ONE `interrogation_reports` business table without child tables.
* **Property Details**: `property_additional_details` was dropped entirely. The `ADDITIONAL_DETAILS` response field varies wildly depending on `CATEGORY` (e.g. Vehicles vs Drugs). This data is now stored directly in the `additional_details jsonb` column on the parent `properties` table.
* **Chargesheets**: `chargesheet_acts`, `chargesheet_acts_sections`, and `chargesheet_accused` were dropped entirely. The `actsAndSections[]` and `accusedParticulars[]` response fields are strictly `1:N` nested arrays, which are now correctly mapped directly into `acts_and_sections jsonb` and `accused_particulars jsonb` on the parent `chargesheets` table.
* **Media**: The redundant `media jsonb DEFAULT '[]'` column was purged from `properties` and `mo_seizures`, cementing `file_media_bookkeeping` as the sole authority for media links.
"""

content += section_6

with open('cctns-v2_schema_mapping_report.md', 'w') as f:
    f.write(content)
