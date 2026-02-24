                                            Master_ETL_Process 
:::::::MASTER-ETL:::::::
server : 192.168.103.106
path : /data-drive/etl-process-dev/etl_master
--> The Master ETL process is a centrally orchestrated daily data pipeline designed to ensure zero data loss and support incremental data updates.
--> Each time the ETL runs, it defines the data extraction window based on dates and timestamps.
--> The end date/time is always considered as:
Yesterday at 23:59:59
(i.e., the pipeline processes data only up to the end of the previous day).
--> During execution, the ETL checks the existing database for each API-specific target table.
--> For every table, it fetches the most recent values of:
created_date
updated_date
--> The latest available timestamp from the database is treated as the start date/time for the next extraction cycle.
--> Using this start and end range, the ETL pulls only the new or updated records from the source APIs.
-->This approach ensures:
Incremental loading
Continuous synchronization between source APIs and the database


The process runs in the following order of priority:
1.Hierarchy
2.Crimes
3.Case Classification Process
4.Case Status Update Process
5.Accused
6.Persons
7.Native State Update Process
8.Domicile Classification Process
9.Person Names Fix Process
10.Properties
11.Interrogation Reports (IR)
12.Disposal
13.Arrests
14.MO & Seizures
15.Chargesheets
16.Updated Chargesheets
17.FSL Case Property
18.Brief Facts (Accused)
19.Brief Facts (Drugs)
20.Drug Standardization
21.File ID Synchronization
22.Media Server Download
23.File Extension Correction
24.Refresh_views

25.De-duplicaiton process [ Currently stoped , process needs more cpu , memory configurations.]



1.Hierarchy: [ server : 192.168.103.106 ]
path : /data-drive/etl-process-dev/etl-hierarchy
process:
Start → Connect DB → Calculate Date Range → Split into Chunks
  ↓
For each chunk:
  → Fetch from API → Transform Data → Check if exists in DB
  → Insert (new) or Update (existing) → Log operations
  ↓
All chunks done → Generate Statistics → Close connections → End
---

2.Crimes: [ server : 192.168.103.106 ]
path : /data-drive/etl-process-dev/etl-crimes
process:
Start → Connect DB → Calculate Date Range → Split into Chunks
  ↓
For each chunk:
  → Fetch from Crimes API → Transform Data → Validate PS_CODE in Hierarchy
  → Check if crime_id exists → Insert (new) or Update (existing)
  → Log operations (including duplicates & PS_CODE failures)
  ↓
All chunks done → Generate Statistics → Close connections → End
---

3.Case Classification Process: [ server : 192.168.103.106 ]
path : /data-drive/etl-process-dev/section-wise-case-clarification
process:
START
  ↓
Check if column exists → Create if missing
  ↓
Fetch ALL records: SELECT crime_id, acts_sections, class_classification
  ↓
  ├─→ LOGIC 1: clean_text()
  │   └─→ Remove: "r/w", "NDPSA", "NDPSAA", "NDPSR"
  │   └─→ Normalize spaces
  │   └─→ Example: "8c NDPSA, r/w 20(b)(ii)(A)" → "8c , 20(b)(ii)(A)"
  │
  ├─→ LOGIC 2: extract_sections()
  │   └─→ Split by comma
  │   └─→ Filter out non-NDPS acts (DCA, CrPC, etc.)
  │   └─→ Example: ["8c", "20(b)(ii)(A)"]
  │
  ├─→ LOGIC 3: classify_row()
  │   │
  │   ├─→ normalize_item()
  │   │   └─→ Lowercase + remove non-alphanumeric
  │   │   └─→ Example: "20(b)(ii)(C)" → "20biic"
  │   │
  │   ├─→ classify_item() - RULES:
  │   │   ├─→ Rule 1: "20a" → cultivation
  │   │   ├─→ Rule 2: "27*" → small
  │   │   ├─→ Rule 3: "8c" → small
  │   │   ├─→ Rule 4: No letters → small
  │   │   └─→ Rule 5: Check a/b/c → a=small, b=intermediate, c=commercial
  │   │
  │   └─→ Pick highest priority:
  │       └─→ Cultivation(3) > Commercial(2) > Intermediate(1) > Small(0)
  │
  ├─→ LOGIC 4: Update Database
  │   ├─→ Compare existing vs new classification
  │   ├─→ If different → UPDATE crimes SET class_classification = ?
  │   └─→ Track stats (updated, no_change)
  │
  └─→ Commit every 100 records
  ↓
Close connection
  ↓
END
---

4.Case Status Update Process : [ server : 192.168.103.106 ]
path: /data-drive/etl-process-dev/etl_case_status
process:
START
  ↓
For each UPDATE statement in sequence:
  ├─→ Find rows WHERE case_status = 'old_value'
  └─→ SET case_status = 'new_value'
  ↓
MAPPING LOGIC (8 transformations):
  ├─→ 'PT Cases' → 'PT'
  ├─→ 'UI Cases' → 'UI'
  ├─→ 'New' → 'UI'
  ├─→ 'Chargesheet Created' → 'Chargesheeted'
  ├─→ 'compounded' → 'Compounded' (capitalize)
  ├─→ 'Pending Trial' → 'PT'
  ├─→ 'Under Investigation' → 'UI'
  └─→ 'Under Trial' → 'UI'
  ↓
END
---

5.Accused: [ server : 192.168.103.106 ]
path : /data-drive/etl-process-dev/etl-accused
process:
Start → Connect DB → Calculate Date Range → Split into Chunks
  ↓
For each chunk:
  → Fetch from Accused API → Transform Data
  → Validate CRIME_ID (with fallback to crime API if missing)
  → Create stub PERSON if needed
  → Check if accused_id exists → Insert (new) or Update (existing)
  → Log operations
  ↓
All chunks done → Generate Statistics → Close connections → End
---

6.Persons: [ server : 192.168.103.106 ]
path : /data-drive/etl-process-dev/etl-persons
process:
Start → Connect DB → Get person_ids from accused table
  ↓
For each person_id:
  → Fetch from Person Details API → Transform nested data to flat structure
  → Check if person_id exists → Insert (new) or Update (existing/stub)
  → Log operations
  ↓
All person_ids done → Generate Statistics → Close connections → End
---

7.Native State Update Process : [ server : 192.168.103.106 ]
path: /data-drive/etl-process-dev/update-state-country
process:
START
  ↓
Fetch Records from Database
  ├─→ Query: SELECT * FROM persons
  ├─→ WHERE: (permanent_state_ut IS NULL OR '') OR (permanent_country IS NULL OR '')
  └─→ AND NOT: (both state AND country already set) [SKIP these]
  ↓
For each RECORD in sequence:
  ├─→ Check: Both state AND country exist?
  │   └─→ YES → SKIP record
  │
  ├─→ Check: Any address info in permanent_* fields?
  │   └─→ NO → Set state=NULL, country=NULL → UPDATE → NEXT
  │
  ├─→ Check: State exists but country missing?
  │   ├─→ YES → Determine COUNTRY only
  │   └─→ NO → Determine BOTH state and country
  │
  └─→ LOCATION DETERMINATION:
      ├─→ STEP 1: Reference Data Lookup (fast)
      │   ├─→ Match state name → ref_data[country]["states"]
      │   ├─→ Match city → ref_data[country]["cities"]
      │   └─→ Match state/city in address components
      │   └─→ FOUND? → Return (state, country) → SKIP LLM
      │
      └─→ STEP 2: LLM Fallback (if not in ref_data)
          ├─→ Build prompt (scenario-based)
          ├─→ Call LLM API → Parse JSON response
          └─→ Return LocationResult {state, country, confidence, reasoning}
  ↓
UPDATE Database (unless --dry-run)
  ├─→ UPDATE persons
  ├─→ SET permanent_state_ut = {value} [if needed]
  ├─→ SET permanent_country = {value} [if needed]
  └─→ WHERE {id_column} = {record_id}
  ↓
END
---

8.Domicile Classification Process: [ server : 192.168.103.106 ]
path: /data-drive/etl-process-dev/domicile_classification
process:
START
  ↓
STEP 1: Check/Add Column
  ├─→ Query: Check if 'domicile_classification' column exists
  │   └─→ SELECT FROM information_schema.columns
  │
  ├─→ IF column NOT exists:
  │   ├─→ ALTER TABLE persons ADD COLUMN domicile_classification VARCHAR(50)
  │   └─→ COMMIT transaction
  │
  └─→ IF column exists:
      └─→ Use existing column (skip creation)
  ↓
STEP 2: Fetch All Records
  ├─→ Query: SELECT person_id, permanent_state_ut, permanent_country 
  │   FROM persons
  │   ORDER BY person_id
  └─→ Fetch all rows into memory
  ↓
For each RECORD in sequence:
  ├─→ Extract: person_id, permanent_state_ut, permanent_country
  │
  ├─→ NORMALIZE inputs:
  │   ├─→ Convert to lowercase
  │   ├─→ Strip whitespace
  │   └─→ Treat NULL, empty string, or 'default' as None
  │
  └─→ CLASSIFICATION LOGIC (classify_domicile function):
      ├─→ Check: Both state AND country are None?
      │   └─→ YES → Return NULL classification
      │
      ├─→ Check: Country exists and != "india"?
      │   └─→ YES → Return "international"
      │
      ├─→ Check: State is None?
      │   └─→ YES → Return NULL classification
      │
      ├─→ Check: State == "telangana"?
      │   └─→ YES → Return "native state"
      │
      ├─→ Check: State in INDIAN_STATES set?
      │   └─→ YES → Return "inter state"
      │
      └─→ Otherwise:
          └─→ Return "international"
  ↓
UPDATE Database
  ├─→ UPDATE persons
  ├─→ SET domicile_classification = {classification_value}
  └─→ WHERE person_id = {person_id}
  ↓
Close Database Connection
  ↓
END
---

9.Person Names Fix Process : [ server : 192.168.103.106 ]
path: /data-drive/etl-process-dev/fix_fullname
process:
START
  ↓
Check/Create raw_full_name Column
  ├─→ Query: Check if column exists
  ├─→ EXISTS? → Skip creation
  └─→ NOT EXISTS? → Will create later
  ↓
Fetch ALL Records from Database
  ├─→ Query: SELECT person_id, name, surname, full_name, alias, 
  │          relative_name, relation_type FROM persons
  └─→ Result: All persons in database
  ↓
For each RECORD in sequence:
  ├─→ Get original_name = full_name OR name OR ""
  ├─→ Check: Is original_name empty?
  │   └─→ YES → SKIP record
  │
  ├─→ Initialize: updated_name = original_name, changes = {}
  │
  ├─→ STEP 1: Extract Alias (if @ present and alias empty)
  │   ├─→ Check: "@" in updated_name AND alias is empty?
  │   ├─→ YES → Extract alias using extract_alias_from_name()
  │   │   ├─→ Split by "@"
  │   │   ├─→ parts[0] = primary_name
  │   │   ├─→ parts[1] = alias (cleaned)
  │   │   └─→ Add to changes["alias"]
  │   └─→ Update: updated_name = name_without_alias
  │
  ├─→ STEP 2: Extract Relationship Info (if fields empty)
  │   ├─→ Check: relative_name OR relation_type missing?
  │   ├─→ YES → Extract using extract_relationship_info()
  │   │   ├─→ Search for "s/o" → relation_type="Father"
  │   │   ├─→ Search for "d/o" → relation_type="Father"
  │   │   ├─→ Search for "w/o" → relation_type="Husband"
  │   │   └─→ Extract relative_name, remove from name
  │   └─→ Add to changes["relation_type"], changes["relative_name"]
  │
  └─→ STEP 3: Clean Name
      ├─→ Call clean_name(updated_name)
      ├─→ Remove: absconding, r/o, N/o, age, caste, phone, Aadhaar
      ├─→ Remove: case markers (A-123), "and others", parentheses content
      ├─→ Remove: vehicle/prisoner/CRPF info
      ├─→ Normalize: multiple spaces → single space
      └─→ Result: cleaned_name
  ↓
Collect Updates
  ├─→ Check: cleaned_name != original_name OR changes exist?
  ├─→ YES → Add to updates list
  │   ├─→ changes["raw_full_name"] = original_name
  │   ├─→ changes["full_name"] = cleaned_name
  │   └─→ Store person_id, original_name, changes
  └─→ NO → SKIP
  ↓
Create raw_full_name Column (if needed)
  ├─→ ALTER TABLE persons ADD COLUMN raw_full_name TEXT
  └─→ Commit
  ↓
Apply Updates to Database
  ├─→ For each update in updates:
  │   ├─→ Build dynamic UPDATE query
  │   ├─→ SET clauses from changes dict
  │   ├─→ WHERE person_id = {id}
  │   ├─→ Execute UPDATE
  │   └─→ Commit every 50 records
  └─→ Final commit
  ↓
END
```

---

## 2. fix_all_fullnames.py

```
START
  ↓
Fetch ALL Records from Database
  ├─→ Query: SELECT person_id, full_name, raw_full_name, 
  │          name, surname, alias FROM persons
  └─→ Result: All persons in database
  ↓
Categorize Records
  ├─→ records_with_at: full_name contains '@'
  ├─→ records_empty: full_name is NULL or empty
  └─→ records_needing_cleanup: full_name exists but cleaning produces different result
  ↓
Process Records with '@' in full_name
  ├─→ For each record:
  │   ├─→ Call clean_full_name(full_name)
  │   │   ├─→ Remove @ and everything after
  │   │   ├─→ Remove absconding, relationships (s/o, d/o, w/o)
  │   │   ├─→ Remove r/o, N/o, age, caste, phone, Aadhaar
  │   │   ├─→ Remove house numbers, case markers, "and others"
  │   │   ├─→ Remove vehicle/prisoner/CRPF info
  │   │   └─→ Normalize spacing
  │   ├─→ Check: raw_full_name exists?
  │   │   ├─→ NO → UPDATE full_name AND raw_full_name
  │   │   └─→ YES → UPDATE full_name only
  │   └─→ Commit
  ↓
Process Records Needing Cleanup
  ├─→ For each record:
  │   ├─→ Call clean_full_name(full_name)
  │   ├─→ Check: raw_full_name exists?
  │   │   ├─→ NO → UPDATE full_name AND raw_full_name
  │   │   └─→ YES → UPDATE full_name only
  │   └─→ Commit
  ↓
Process Records with Empty full_name
  ├─→ For each record:
  │   ├─→ Call construct_full_name(name, surname, alias)
  │   │   ├─→ Combine: name + surname + "@alias"
  │   │   └─→ Return joined string
  │   ├─→ Call clean_full_name(constructed)
  │   ├─→ Check: raw_full_name exists?
  │   │   ├─→ NO → UPDATE full_name AND raw_full_name (with constructed)
  │   │   └─→ YES → UPDATE full_name only
  │   └─→ Commit
  ↓
Final Commit
  └─→ Commit all remaining changes
  ↓
END
```

---

## 3. fix_name_field.py

```
START
  ↓
Fetch Records with '@' in name Field
  ├─→ Query: SELECT person_id, name, full_name, raw_full_name
  │          FROM persons
  │          WHERE name LIKE '%@%'
  │          ORDER BY person_id
  └─→ Result: All persons where name contains '@'
  ↓
Check: Any records found?
  ├─→ NO → Print "No records to update!" → END
  └─→ YES → Continue
  ↓
Show Sample (First 20)
  ├─→ For each record:
  │   ├─→ Extract clean_name = extract_clean_name(name)
  │   │   └─→ Split by "@", take parts[0].strip()
  │   ├─→ Display: person_id, current name, will become, full_name
  │   └─→ Show proposed change
  └─→ If more than 20, show "... and X more records"
  ↓
Apply Updates to Database
  ├─→ For each record:
  │   ├─→ Extract clean_name = extract_clean_name(name)
  │   ├─→ UPDATE persons SET name = %s WHERE person_id = %s
  │   ├─→ Execute with (clean_name, person_id)
  │   └─→ Commit every 50 records
  └─→ Final commit
  ↓
END
```

---

## 4. fix_surname_field.py

```
START
  ↓
Fetch Records with '@' in surname Field
  ├─→ Query: SELECT person_id, name, surname, full_name
  │          FROM persons
  │          WHERE surname LIKE '%@%'
  │          ORDER BY person_id
  └─→ Result: All persons where surname contains '@'
  ↓
Check: Any records found?
  ├─→ NO → Print "No records to update!" → END
  └─→ YES → Continue
  ↓
Show Sample (First 20)
  ├─→ For each record:
  │   ├─→ Clean surname = clean_surname(surname)
  │   │   ├─→ Check: Starts with "@"?
  │   │   │   └─→ YES → Return "" (empty)
  │   │   ├─→ Check: Contains "@"?
  │   │   │   └─→ YES → Split by "@", take parts[0].strip()
  │   │   └─→ NO → Return surname as-is
  │   ├─→ Display: person_id, name, current surname, will become
  │   └─→ Show proposed change
  └─→ If more than 20, show "... and X more records"
  ↓
Apply Updates to Database
  ├─→ For each record:
  │   ├─→ Clean surname = clean_surname(surname)
  │   ├─→ UPDATE persons SET surname = %s WHERE person_id = %s
  │   ├─→ Execute with (clean_surname, person_id)
  │   │   └─→ Note: surname can be empty string ""
  │   └─→ Commit every 50 records
  └─→ Final commit
  ↓
END
---

10.Properties : [ server : 192.168.103.106 ]
path: /data-drive/etl-process-dev/etl-properties
process:
Start → Connect DB → Calculate Date Range → Split into Chunks
  ↓
For each chunk:
  → Fetch from Properties API → Transform Data (including JSONB media)
  → Check if property_id exists → Insert (new) or Update (existing)
  → Log operations
  ↓
All chunks done → Generate Statistics → Close connections → End
---

11.Interrogation Reports (IR) : [ server : 192.168.103.106 ]
path: /data-drive/etl-process-dev/etl-ir
process:
Start → Connect DB → Calculate Date Range → Split into Chunks
  ↓
For each chunk:
  → Fetch from IR API → Transform & Normalize nested data
  → Check if ir_id exists → Insert/Update main record
  → Insert/Update related records (family_history, contacts, etc.)
  → Log operations
  ↓
All chunks done → Generate Statistics → Close connections → End
---
12.Disposal : [ server : 192.168.103.106 ]
path: /data-drive/etl-process-dev/etl-disposal
process:
Start → Connect DB → Calculate Date Range → Split into Chunks
  ↓
For each chunk:
  → Fetch from Disposal API → Transform Data → Validate CRIME_ID in crimes table
  → Check if disposal exists → Insert (new) or Update (existing)
  → Log operations
  ↓
All chunks done → Generate Statistics → Close connections → End
---

13.Arrests : [ server : 192.168.103.106 ]
path: /data-drive/etl-process-dev/etl_arrests
process:
Start → Connect DB → Calculate Date Range → Split into Chunks
  ↓
For each chunk:
  → Fetch from Arrests API → Transform Data
  → Validate CRIME_ID (required) → Validate PERSON_ID (optional, can be NULL)
  → Check if arrest exists → Insert (new) or Update (existing)
  → Log operations
  ↓
All chunks done → Generate Statistics → Close connections → End
---

14.MO & Seizures: [ server : 192.168.103.106 ]
path: /data-drive/etl-process-dev/etl_mo_seizures
process:
Start → Connect DB → Calculate Date Range → Split into Chunks
  ↓
For each chunk:
  → Fetch from MO Seizures API → Transform Data → Validate CRIME_ID in crimes table
  → Check if seizure exists → Insert (new) or Update (existing)
  → Log operations
  ↓
All chunks done → Generate Statistics → Close connections → End
---

15.Chargesheets : [ server : 192.168.103.106 ]
path: /data-drive/etl-process-dev/etl_mo_seizures
process:
Start → Connect DB → Calculate Date Range → Split into Chunks
  ↓
For each chunk:
  → Fetch from Chargesheets API → Transform & Normalize nested data
  → Validate CRIME_ID in crimes table
  → Check if chargesheet exists → Insert/Update main record
  → Insert/Update related records (files, acts, accused)
  → Log operations
  ↓
All chunks done → Generate Statistics → Close connections → End
---

16.Updated Chargesheets : [ server : 192.168.103.106 ]
path: /data-drive/etl-process-dev/etl_updated_chargesheet
process:
Start → Connect DB → Calculate Date Range → Split into Chunks
  ↓
For each chunk:
  → Fetch from Updated Chargesheets API → Transform Data → Validate CRIME_ID in crimes table
  → Check if update record exists → Insert (new) or Update (existing)
  → Log operations
  ↓
All chunks done → Generate Statistics → Close connections → End
---

17.FSL Case Property : [ server : 192.168.103.106 ]
path: /data-drive/etl-process-dev/etl_fsl_case_property
process:
Start → Connect DB → Calculate Date Range → Split into Chunks
  ↓
For each chunk:
  → Fetch from FSL Case Property API → Transform & Normalize nested data
  → Validate CRIME_ID in crimes table
  → Check if property exists → Insert/Update main record
  → Insert/Update media records
  → Log operations
  ↓
All chunks done → Generate Statistics → Close connections → End
---

18.Brief Facts (Accused) : [ server : 192.168.103.106 ]
path: /data-drive/etl-process-dev/brief_facts_accused
process:
1. START
   ↓
2. Fetch Crime: CR-2024-001
   ↓
3. PASS 1 (LLM):
   Input: brief_facts text
   Output: ["Rahul @ Rocky", "Suresh Kumar"]
   ↓
4. PASS 2 (LLM):
   Input: text + names
   Output: [
     {name: "Rahul @ Rocky", age: 25, gender: "Male", 
      role: "Caught selling 5kg Ganja"},
     {name: "Suresh Kumar", role: "Supplier, absconding"}
   ]
   ↓
5. Rule Classification:
   - Rahul: role="selling" → accused_type="peddler", status="arrested"
   - Suresh: role="Supplier" → accused_type="supplier", status="absconding"
   ↓
6. Database Matching:
   - Check if names exist in persons table
   - Match found? → Link person_id, enrich missing fields
   ↓
7. INSERT to Database:
   - Record 1: Rahul @ Rocky (peddler, arrested)
   - Record 2: Suresh Kumar (supplier, absconding)
   ↓
8. END
---

19.Brief Facts (Drugs) : [ server : 192.168.103.106 ]
path: /data-drive/etl-process-dev/brief_facts_drugs
process:
START
  ↓
Initialize Logging & Configuration
  ├─→ Setup logging format
  └─→ Load config from .env (DB, LLM settings)
  ↓
Connect to Database
  ├─→ Read DB credentials from config
  ├─→ Connect to PostgreSQL
  └─→ ERROR? → Exit with error message
  ↓
Determine Processing Mode
  ↓
MODE SELECTION:
  └─→ MODE 1: Dynamic Batch Mode
      ├─→ Query: SELECT c.crime_id, c.brief_facts
      ├─→ FROM crimes c
      ├─→ LEFT JOIN drug_table d ON c.crime_id = d.crime_id
      ├─→ WHERE d.crime_id IS NULL (unprocessed crimes)
      └─→ LIMIT 100 (batch size)
      └─→ Loop until no more unprocessed crimes
  ↓
For each CRIME RECORD in sequence:
  ├─→ Extract: crime_id, brief_facts (text)
  │
  └─→ DRUG EXTRACTION PROCESS:
      ├─→ STEP 1: LLM Extraction
      │   ├─→ Build prompt with extraction rules:
      │   │   ├─→ Container vs Content logic
      │   │   ├─→ Prioritize TOTAL quantities
      │   │   ├─→ Extract distinct seizures separately
      │   │   ├─→ Ignore samples, extract original totals
      │   │   └─→ Extract seizure worth (monetary value)
      │   │
      │   ├─→ Call LLM API (Ollama)
      │   │   ├─→ Model: qwen2.5-coder (from config)
      │   │   ├─→ Endpoint: http://localhost:11434/api
      │   │   └─→ Parse JSON response
      │   │
      │   └─→ Extract: List of DrugExtraction objects
      │       ├─→ drug_name
      │       ├─→ quantity_numeric, quantity_unit
      │       ├─→ drug_form, packaging_details
      │       ├─→ confidence_score
      │       └─→ seizure_worth (in rupees)
      │
      ├─→ STEP 2: Unit Standardization
      │   ├─→ For each drug extraction:
      │   │   ├─→ Truncate strings (prevent DB errors)
      │   │   │
      │   │   ├─→ WEIGHT Conversion:
      │   │   │   ├─→ kg → standardized_weight_kg (as-is)
      │   │   │   ├─→ grams → standardized_weight_kg (÷ 1000)
      │   │   │   └─→ mg → standardized_weight_kg (÷ 1,000,000)
      │   │   │
      │   │   ├─→ VOLUME Conversion:
      │   │   │   ├─→ liters → standardized_volume_ml (as-is, stored in liters)
      │   │   │   └─→ ml → standardized_volume_ml (÷ 1000, convert to liters)
      │   │   │
      │   │   ├─→ COUNT Units:
      │   │   │   ├─→ pieces, tablets, packets, etc.
      │   │   │   └─→ standardized_count (as-is)
      │   │   │
      │   │   ├─→ Name Standardization:
      │   │   │   └─→ cannabis/ganja/marijuana → "Ganja"
      │   │   │
      │   │   └─→ Seizure Worth Conversion:
      │   │       └─→ Rupees → Crores (÷ 10,000,000)
      │   │
      │   └─→ Set primary_unit_type: 'weight', 'volume', or 'count'
      │
      ├─→ STEP 3: Confidence Filtering
      │   ├─→ For each drug extraction:
      │   │   ├─→ Check: confidence_score >= 90?
      │   │   │   ├─→ YES → Proceed to insert
      │   │   │   └─→ NO → Skip (log message)
      │   │   │
      │   │   └─→ Count valid extractions
      │
      └─→ STEP 4: Database Insertion
          ├─→ If valid drugs found (count > 0):
          │   ├─→ For each drug (confidence >= 90):
          │   │   ├─→ INSERT INTO drug_table
          │   │   ├─→ Fields: crime_id, drug_name, quantity_numeric,
          │   │   │         quantity_unit, drug_form, packaging_details,
          │   │   │         confidence_score, standardized_weight_kg,
          │   │   │         standardized_volume_ml, standardized_count,
          │   │   │         primary_unit_type, is_commercial,
          │   │   │         seizure_worth, extraction_metadata (JSON)
          │   │   └─→ COMMIT transaction
          │   │
          └─→ If NO valid drugs found (count == 0):
              ├─→ Insert PLACEHOLDER record:
              │   ├─→ drug_name = "NO_DRUGS_DETECTED"
              │   ├─→ quantity_numeric = 0
              │   ├─→ confidence_score = 100
              │   └─→ (This marks crime as "processed" to prevent infinite loop)
              └─→ COMMIT transaction
  ↓
Batch Processing (Dynamic Mode only)
  ├─→ After processing batch of 100:
  │   ├─→ Log: "Batch complete. Total processed: X"
  │   └─→ Fetch next batch of 100 unprocessed crimes
  │
  └─→ Continue until: No more unprocessed crimes found
  ↓
Cleanup & Exit
  ├─→ Close database connection
  └─→ END
---

20.Drug Standardization : [ server : 192.168.103.106 ]
path: /data-drive/etl-process-dev/drug_standardization
process:
START
  ↓
Initialize Script (main())
  ├─→ Check: .env file exists?
  │   └─→ NO → Print error → EXIT
  │
  ├─→ Check: drug_mappings.json exists?
  │   └─→ NO → Print error → EXIT
  │
  └─→ Create DrugStandardizer instance
      ├─→ Load TABLE_NAME from .env
      ├─→ Load drug_mappings.json → Store in self.drug_mappings
      └─→ Initialize stats counters (total_processed, updated, skipped, errors, unmatched)
  ↓
Run Standardization (run())
  ├─→ Connect to Database (connect_to_database())
  │   ├─→ Read DB credentials from .env (HOST, PORT, NAME, USER, PASSWORD)
  │   ├─→ Establish PostgreSQL connection
  │   └─→ Create RealDictCursor for row access
  │   └─→ FAILED? → Log error → EXIT
  │
  ├─→ Fetch Records to Update (fetch_records_to_update())
  │   ├─→ Query: SELECT id, drug_name, primary_drug_name
  │   ├─→ FROM: {TABLE_NAME}
  │   ├─→ WHERE: primary_drug_name IS NULL
  │   │        OR primary_drug_name = ''
  │   │        OR primary_drug_name = 'Unknown'
  │   │        OR LOWER(TRIM(primary_drug_name)) = 'null'
  │   └─→ Return list of records
  │   └─→ NO RECORDS? → Log message → SKIP processing
  │
  └─→ Process Records (process_records())
      ↓
      For each RECORD in sequence:
        ├─→ Increment total_processed counter
        │
        ├─→ Check: drug_name exists and not empty?
        │   └─→ NO → Log warning → Increment skipped → NEXT record
        │
        ├─→ Normalize Drug Name (_normalize_drug_name())
        │   ├─→ Convert to lowercase
        │   └─→ Remove all spaces
        │   └─→ Example: "Aspirin 100mg" → "aspirin100mg"
        │
        ├─→ Lookup in Mappings
        │   ├─→ Check: normalized_name in self.drug_mappings?
        │   │
        │   ├─→ YES → Found mapping
        │   │   ├─→ Get primary_drug_name from mappings
        │   │   ├─→ Update record (update_record())
        │   │   │   ├─→ UPDATE {TABLE_NAME}
        │   │   │   ├─→ SET primary_drug_name = {mapped_value}
        │   │   │   └─→ WHERE id = {record_id}
        │   │   ├─→ SUCCESS? → Increment updated → Log success
        │   │   └─→ FAILED? → Increment errors
        │   │
        │   └─→ NO → No mapping found
        │       ├─→ Set primary_drug_name = original drug_name (fallback)
        │       ├─→ Update record with original value
        │       ├─→ Increment updated
        │       ├─→ Add to unmatched list (for reporting)
        │       └─→ Log: "No mapping found, set to original value"
        │
        └─→ NEXT record
      ↓
      Commit Transaction
        ├─→ self.connection.commit()
        ├─→ SUCCESS? → Log "All changes committed"
        └─→ FAILED? → Rollback → Log error
↓
Close Connection (close_connection())
  └─→ Close database connection
↓
END
---

21.File ID Synchronization : [ server : 192.168.103.106 ]
path: /data-drive/etl-process-dev/etl-files/etl_pipeline_files
process:
START
  ↓
Initialize (DB, API config, IdempotencyChecker, FilesLoader)
  ↓
Resume: get last processed date from files table (MAX(created_at) per source_type)
  ├─ Has last date? → Chunks from (last − 1 day) to today [or backwards: today → start_date, skip chunks after last]
  └─ No last date? → Chunks from 2022-01-01 to today [or backwards from today to 2022-01-01]
  ↓
For each DATE CHUNK (from_date, to_date):
  ├─ Build API URL (fromDate, toDate)
  ├─ Fetch: GET url → JSON
  ├─ Extract: parse JSON → list of file records (source_type, source_field, parent_id, file_id, api_date, …)
  └─ Append to total_files
  ↓
Load total_files:
  ├─ Dedupe in-memory
  ├─ (Optional) idempotency check per record
  ├─ INSERT ... ON CONFLICT DO UPDATE SET created_at = COALESCE(files.created_at, EXCLUDED.created_at) WHERE files.created_at IS NULL
  └─ Commit
  ↓
Repeat for next API (same flow, different extractor and source_type)
  ↓
Log final stats, close connection → END


22.Media Server Download : [ server : 192.168.103.106 ]
path: /data-drive/etl-process-dev/etl-files/etl_files_media_server
process:
START
  ↓
Entry: main() → FilesMediaServerETL().run()
  ↓
Setup & connect
  ├─→ Load config from config.py (DB_CONFIG, API_CONFIG, LOG_CONFIG)
  ├─→ Setup logger (console + file in logs/files_media_server_etl_*.log)
  ├─→ connect_db() → PostgreSQL using DB_CONFIG
  └─→ ensure_download_tracking_columns()
      ├─→ Add if missing: downloaded_at, is_downloaded, download_error, download_attempts, created_at
      └─→ Create indexes: idx_files_is_downloaded, idx_files_downloaded_at, idx_files_created_at, idx_files_source_type_created
  ↓
Fetch records from database
  ├─→ Query: SELECT source_type, source_field, file_id FROM {FILES_TABLE}
  ├─→ WHERE: file_id IS NOT NULL
  │          AND has_field IS TRUE
  │          AND is_empty IS FALSE
  │          AND (is_downloaded IS FALSE OR downloaded_at IS NULL)   [only undownloaded or failed]
  └─→ ORDER BY: created_at DESC NULLS LAST, source_type, file_id   [newest first]
  ↓
If no rows → log "No rows to process", close DB → END (success)
  ↓
For each RECORD (source_type, source_field, file_id) in sequence:
  ├─→ Check: file_id is NULL?
  │   └─→ YES → SKIP (skipped_null_file_id) → NEXT
  │
  ├─→ download_single_file(file_id, source_type, source_field):
  │   │
  │   ├─→ STEP 1: Check file exists (HEAD request to API)
  │   │   ├─→ URL: {FILES_BASE_URL}/{file_id}  (x-api-key header)
  │   │   ├─→ 404 / 400 → Mark as permanently failed in DB → FAIL → NEXT
  │   │   └─→ 200 (or 429/inconclusive) → proceed to download
  │   │
  │   ├─→ Increment download_attempts in DB for this file_id
  │   │
  │   ├─→ STEP 2: GET file (stream=True), with retries (max_retries, default 3)
  │   │   ├─→ 200 OK:
  │   │   │   ├─→ DESTINATION: map_destination_subdir(source_type, source_field)
  │   │   │   │   ├─→ crime + FIR_COPY → crimes/
  │   │   │   │   ├─→ person + IDENTITY_DETAILS → person/identitydetails/
  │   │   │   │   ├─→ person + MEDIA → person/media/
  │   │   │   │   ├─→ property + MEDIA → property/
  │   │   │   │   ├─→ interrogation + MEDIA → interrogations/media/
  │   │   │   │   ├─→ interrogation + INTERROGATION_REPORT → interrogations/interrogationreport/
  │   │   │   │   ├─→ interrogation + DOPAMS_DATA → interrogations/dopamsdata/
  │   │   │   │   ├─→ mo_seizures + MO_MEDIA → mo_seizures/
  │   │   │   │   ├─→ chargesheets + UPLOADCHARGESHEET → chargesheets/
  │   │   │   │   ├─→ case_property + MEDIA → fsl_case_property/
  │   │   │   │   └─→ No mapping? → SKIP (skipped_no_mapping) → NEXT
  │   │   │   │
  │   │   │   ├─→ dest_path = BASE_MEDIA_PATH / subdir / {file_id}{ext}
  │   │   │   │   (ext from Content-Type or Content-Disposition, fallback .pdf)
  │   │   │   ├─→ ensure_directory(dest_dir)  [create 775 if needed]
  │   │   │   │
  │   │   │   ├─→ Check: file already exists on disk at dest_path?
  │   │   │   │   └─→ YES → Mark is_downloaded=TRUE in DB → SKIP (skipped_exists_on_disk) → NEXT
  │   │   │   │
  │   │   │   ├─→ Stream response to file → os.chmod(dest_path, 0o644)
  │   │   │   ├─→ _mark_as_downloaded(file_id, success=True)
  │   │   │   └─→ stats["downloaded"] += 1 → SUCCESS
  │   │   │
  │   │   ├─→ 429 or 5xx → backoff (Retry-After or SECONDS_PER_REQUEST * attempt) → retry
  │   │   └─→ 400/401/403/404 (non-retriable) → _mark_as_downloaded(success=False, error_msg=…) → FAIL → NEXT
  │   │
  │   └─→ Timeout/other exception → backoff → retry up to max_retries; then mark failed → NEXT
  │
  └─→ After each request: _respect_rate_limit()  [10 RPM → ~6 s between requests]
  ↓
UPDATE database (per file, inside download_single_file)
  ├─→ On SUCCESS: UPDATE {FILES_TABLE} SET is_downloaded = TRUE, downloaded_at = CURRENT_TIMESTAMP, download_error = NULL WHERE file_id = %s
  ├─→ On FAIL:     UPDATE {FILES_TABLE} SET is_downloaded = FALSE, download_error = %s WHERE file_id = %s
  └─→ download_attempts incremented in both cases (once at start of attempt, once in _mark_as_downloaded)
  ↓
Final summary (logs)
  ├─→ Total rows fetched, total processed, downloaded, failed, skipped_*
  └─→ close_db()
  ↓
END  


23.File Extension Correction : [ server : 192.168.103.106 ]
path: /data-drive/etl-process-dev/etl-files/update_file_urls_with_extensions
process:
START
↓
Setup
├─ Load env (.env) → DB_CONFIG, BASE_MEDIA_PATH, BASE_FILE_URL
├─ Setup logger (console + logs/update_file_urls.log)
└─ Check: BASE_MEDIA_PATH exists? → NO → exit(1)
↓
Connect to database
├─ psycopg2.connect(**DB_CONFIG)
└─ Fail → exit(1)
↓
Disable trigger
├─ ALTER TABLE files DISABLE TRIGGER trigger_auto_generate_file_paths
└─ So the trigger does not overwrite file_url while the script runs
↓
For each SOURCE_TYPE in order:
crime → person → property → interrogation → mo_seizures → chargesheets → case_property
↓
Fetch records for this source_type
├─ Query: SELECT id, file_id, source_field, file_path, file_url FROM files
├─ WHERE: source_type = %s AND file_id IS NOT NULL AND file_url IS NOT NULL
└─ ORDER BY id
↓
For each RECORD in sequence:
├─ Map to subdirectory
│ ├─ map_destination_subdir(source_type, source_field)
│ │ (e.g. person + IDENTITY_DETAILS → person/identitydetails)
│ └─ No mapping? → SKIP record [unmappable source_field]
│
├─ Find file on disk
│ ├─ find_file_with_extension(file_id, subdir)
│ ├─ Look under: BASE_MEDIA_PATH / subdir / {file_id}.* (glob)
│ ├─ Optional: case-insensitive match if no match
│ └─ Not found? → SKIP record [file not on disk]
│
├─ Check URL
│ ├─ Compare file_url (without query string) with discovered extension
│ └─ URL already ends with that extension? → SKIP record
│
└─ Update DB
├─ Build new URL: file_url + extension (or insert extension before ? if URL has query params)
├─ UPDATE files SET file_url = %s WHERE id = %s
└─ Count as updated
↓
After all records for this source_type
└─ connection.commit()
↓
Next source_type (repeat until all 7 are done)
↓
Re-enable trigger
├─ ALTER TABLE files ENABLE TRIGGER trigger_auto_generate_file_paths
└─ Log warning: run migrate_trigger_preserve_extensions.sql so future updates keep extensions
↓
END


24.Refresh_views : [ server : 192.168.103.106 ]
path : /data-drive/etl-process-dev/etl_refresh_views
process:
Views give the application a simple, stable “table” (e.g. firs, accuseds, persons_view) that already has all the joins, formatting, and business rules done in the database, so the app can just query and use the data.


25.De-duplicaiton process : [ server : 192.168.103.114 ]
path: /data-drive/deduplication-agent-dopams
process:
START
↓
1. Entry point (CLI or API)
├─ CLI: python cli.py all or python cli.py run-deduplication
├─ API: GET /agent/init or GET /agent/deduplicate?min_confidence=...
└─ Optional: init-db → load-cache → then run deduplication
↓
2. Load persons (if using full pipeline)
├─ load-cache: load_persons_to_cache()
├─ Query: SELECT * FROM persons ORDER BY date_created DESC
├─ Optionally check agent_deduplication_tracker for already-processed IDs (for incremental runs)
└─ Store full list in Redis as persons:all (JSON)
↓
3. Run deduplication: get persons from cache
├─ Read persons:all from Redis
└─ If empty → error “No persons found in cache. Please run 'load-cache' first.”
↓
4. Build comparison matrix — build_comparison_matrix(persons, min_confidence, db)
├─ Get or create run: DedupRunMetadata (run_id, status=running, last_processed_index)
├─ Load in-memory state from DB: DedupCache from DedupClusterState, DedupComparisonProgress (resume from last_processed_index)
├─ Pre-fetch all crime/accused IDs for persons → fill DedupCache.crime_accused_cache
└─ Create blocks: create_district_blocks(persons) → map district → list of person indices (only same-district pairs will be compared)
↓
5. For each person index i (from last_processed_index to N)
├─ Skip? If person i is in a cluster but not the representative → skip (already represented by another person).
├─ Get candidates: get_candidates_for_person(i, blocks, persons) → only indices j in the same district block with j > i.
└─ Initialize comparison_map[person_id_i] = [].
↓
6. For each candidate j (for current person i)
├─ Skip if j <= i.
├─ Skip if i and j already in the same cluster (no need to compare again).
├─ Already compared? Check cache then DB for (i, j).
│ └─ If yes → use stored match_score_numeric, is_match, matching_method → go to “Update clusters / comparison map” below.
└─ Else → perform comparison:
↓
7. Comparison: Stage 1 — Tier hash matching
├─ For tiers 1..5 in order: compute generate_tier_hash(person_i, tier) and generate_tier_hash(person_j, tier).
├─ If same hash for a tier → tier match with that tier’s confidence (e.g. Tier 1=95%, 2=90%, 3=85%, 4=75%, 5=65%).
└─ If tier match:
│ ├─ Confidence > 65% → treat as match (no LLM), record tier description and method (e.g. tier3_hash).
│ ├─ Confidence 40–65% → call LLM (compare_person_pair(person_i, person_j)), use LLM score; ≥70% → match.
│ └─ < 40% → no match, record tier only.
└─ If no tier match → match_score_numeric=0, matching_method="no_match".
↓
8. Persist comparison (only if useful)
├─ Save to cache (in-memory) for this run.
├─ If match or LLM was used → add to batch; when batch size reached → bulk_insert_comparisons(db, batch) into DedupComparisonProgress.
└─ (Cheap “no match” results are not all written to DB to avoid table growth.)
↓
9. Update clusters (if this pair is a match)
├─ If both i and j in no cluster → create new cluster with next_cluster_id, add both; increment next_cluster_id.
├─ If only one in a cluster → add the other to that cluster.
└─ If in two different clusters → merge clusters (all members of one cluster move to the other).
↓
10. Fill result map
└─ If match_score_numeric >= min_confidence → append to comparison_map[person_id_i] (checked_person_id, score, fields_used, reasoning, matching_method).
↓
11. After each person i
├─ Flush any remaining comparison batch to DB, then db.commit().
├─ Periodically (e.g. every 50 persons): cleanup old entries from in-memory comparison cache to limit memory.
└─ Every 10 persons: update_run_progress(db, run_id, i+1), commit, log progress.
↓
12. After all persons processed
├─ Any remaining comparison batch → bulk insert, commit.
├─ Sync tracker: sync_dedup_tracker_from_cache(db, cache, persons)
│ ├─ Delete existing AgentDeduplicationTracker rows.
│ ├─ For each cluster: one row with canonical_person_id (first member), all_person_ids, all_crime_ids, all_accused_ids, matching method/score.
│ └─ For each person not in any cluster: one row (single-person “cluster”).
├─ Mark run completed: DedupRunMetadata.status = "completed", set completed_at.
└─ Return comparison_map.
↓
13. Write output (CLI)
├─ Optionally filter to “best match only” per person if include_all_matches=false.
├─ Write deduplication_results.json: status, min_confidence, statistics (total_persons, total_comparisons, persons_with_matches, etc.), comparison_matrix = comparison_map.
└─ Log “Results saved to deduplication_results.json”.
↓
END






