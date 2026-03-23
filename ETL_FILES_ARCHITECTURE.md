# ETL Files - Complete Architecture Overview

## 📋 Purpose
The **ETL Files module** is responsible for:
1. Extracting file references (IDs) from various APIs
2. Downloading the actual files from the API server
3. Storing file metadata and references in the database

---

## 🔄 Data Flow

```
API Response (Crimes/Persons/Property/Interrogation) 
    ↓
Extract File References/IDs from API Response
    ↓
Store File Metadata in "files" Table (Database)
    ↓
Download Actual Files using File IDs
    ↓
Save PDFs to Local Storage Directory
```

---

## 📥 INPUT SOURCES

### 1. **Crimes API**
- **Endpoint**: `{API_CONFIG['base_url']}/crimes`
- **File Field Extracted**: `FIR_COPY` (file ID)
- **Parent Record**: Crime records (CRIME_ID)
- **Source Type in DB**: `crime`

### 2. **Persons API**
- **Endpoint**: `{API_CONFIG['base_url']}/person-details`
- **File Field Extracted**: Multiple file fields from person records
- **Parent Record**: Person records
- **Source Type in DB**: `person`

### 3. **Property API**
- **Endpoint**: `{API_CONFIG['base_url']}/master-data/hierarchy`
- **File Fields Extracted**: Property-related file references
- **Parent Record**: Property records
- **Source Type in DB**: `property`

### 4. **Interrogation Reports (IR) API**
- **Endpoint**: `{API_CONFIG['base_url']}/interrogation-reports/v1/`
- **File Fields Extracted**: IR document file references
- **Parent Record**: Interrogation report records
- **Source Type in DB**: `interrogation`

### 5. **Other APIs**
- **MO Seizures API**: File references from seizure records
- **Chargesheet API**: File references from chargesheet records
- **FSL Case Property API**: File references from case property records

---

## 🔗 APIs USED

### Primary APIs (with configured endpoints)
```python
{
    'crimes_url': '{BASE_URL}/crimes',
    'accused_url': '{BASE_URL}/accused',
    'persons_url': '{BASE_URL}/person-details',
    'hierarchy_url': '{BASE_URL}/master-data/hierarchy',
    'ir_url': '{BASE_URL}/interrogation-reports/v1/',
    'files_url': '{BASE_URL}/files'  # Main file download endpoint
}
```

### File Download API
- **Endpoint**: `{API_CONFIG['base_url']}/files/{file_id}`
- **Method**: GET
- **Headers**: `x-api-key` (from API_CONFIG)
- **Response**: PDF binary content
- **Retry Logic**: 
  - Max retries: 3 (configurable via `API_MAX_RETRIES`)
  - Handles HTTP 429 (Rate Limiting) with exponential backoff
  - Handles 5xx errors with retry backoff
  - Uses `Retry-After` header if provided by API

---

## 💾 DATABASE TABLES WRITTEN

### Primary Table: `files`
**Purpose**: Store metadata about extracted file references

**Schema** (from files_loader.py):
```
files (
    id                      [Primary Key]
    source_type             [VARCHAR] - Type of source (crime, person, property, interrogation, etc.)
    source_field            [VARCHAR] - Field name from source (FIR_COPY, etc.)
    parent_id               [VARCHAR] - ID of parent record
    file_id                 [VARCHAR] - Unique file identifier from API
    file_index              [INTEGER] - Index for multiple files from same parent
    identity_type           [VARCHAR] - Optional identity type
    identity_number         [VARCHAR] - Optional identity number
    created_at              [TIMESTAMP] - When metadata was extracted (nullable, auto-filled)
    processed               [BOOLEAN] - Whether file has been downloaded/processed
)
```

**Idempotency Behavior**:
- Uses composite key: `(source_type, source_field, parent_id, file_id, file_index)`
- Prevents duplicate file reference extraction
- Updates `created_at` if NULL for existing records
- Respects `skip_existing` flag during loads

---

## 📂 FILE DOWNLOAD LOCATIONS

### Output Directory Configuration
```python
FILES_OUTPUT_DIR = os.getenv("FILES_MEDIA_BASE_PATH")
if not FILES_OUTPUT_DIR:
    logger.error("❌ FILES_MEDIA_BASE_PATH environment variable is not set. Please set it before running.")
    sys.exit(1)
```

**⚠️ REQUIRED**: The `FILES_MEDIA_BASE_PATH` environment variable **MUST** be set before running. The script will exit with an error if not provided.

**Example** (from your .env):
```
FILES_MEDIA_BASE_PATH=/mnt/shared-etl-files
```

### File Naming Convention
```
{file_id}.pdf

Example:
- FIR_COPY=ABC123 → ABC123.pdf
- Stored at: /data-drive/etl-process-dev/etl-files/tomcat/webapps/files/pdfs/ABC123.pdf
```

### File Handling Logic
```
IF file already exists:
    → Log existing file size
    → Delete old file
    → Download latest version (REPLACE mode)
    
ELSE (new file):
    → Download file directly (NEW mode)
    
Track statistics:
    - downloaded_new: New files downloaded
    - downloaded_replaced: Files that were updated
    - failed: Failed downloads
```

---

## 🏗️ ARCHITECTURE & COMPONENTS

### 1. **Two Main Implementations**

#### A. Simple ETL (`etl-files.py`)
- Single standalone script
- Reads FIR_COPY values from PostgreSQL `crimes` table
- Downloads PDFs directly using `/files/{fir_copy}` API
- Simple sequential processing
- Best for: Initial setup, direct crime file downloads

**Key Methods**:
```python
- connect_db()              # Database connection
- get_distinct_fir_copy_values()  # Fetch FIR_COPY IDs from crimes table
- download_file(file_id)   # Download single file
- run()                     # Main execution loop
```

#### B. Full Pipeline (`etl_pipeline_files/`)
- Modular architecture with Extract-Transform-Load (ETL) pattern
- Processes multiple APIs simultaneously
- Extracts file references from all APIs
- Uses idempotency checker for deduplication
- Supports resume from last processed date

**Components**:
```
etl_pipeline_files/
├── extract/
│   ├── base_extractor.py           [Base class for all extractors]
│   ├── crimes_extractor.py         [Extract FIR_COPY from crimes]
│   ├── persons_extractor.py        [Extract files from persons]
│   ├── property_extractor.py       [Extract files from properties]
│   ├── interrogation_extractor.py  [Extract files from IR]
│   ├── mo_seizures_extractor.py    [Extract files from seizures]
│   ├── chargesheets_extractor.py   [Extract files from chargesheets]
│   └── fsl_case_property_extractor.py
├── load/
│   ├── files_loader.py             [Load file records into DB]
│   └── __init__.py
├── config/
│   ├── database.py      [DB connection config]
│   └── api_config.py    [API endpoint config]
├── utils/
│   ├── logger.py        [Logging setup]
│   ├── date_utils.py    [Date range chunking]
│   └── idempotency.py   [Duplicate prevention]
├── main.py              [Full pipeline execution]
└── main_standalone.py   [Standalone alternative]
```

---

## 📊 Processing Statistics

Both implementations track:
```python
stats = {
    'total_fir_copy_values': 0,    # Total distinct file IDs found
    'total_processed': 0,           # Total attempts
    'skipped_null_or_empty': 0,    # Null/empty file IDs
    'downloaded_new': 0,            # New files downloaded
    'downloaded_replaced': 0,       # Updated files
    'failed': 0,                    # Download failures
    
    # Per-API stats (full pipeline):
    'crimes': {'processed': 0, 'inserted': 0, 'skipped': 0, 'errors': 0},
    'persons': {'processed': 0, 'inserted': 0, 'skipped': 0, 'errors': 0},
    'property': {'processed': 0, 'inserted': 0, 'skipped': 0, 'errors': 0},
    'interrogation': {'processed': 0, 'inserted': 0, 'skipped': 0, 'errors': 0},
    'mo_seizures': {'processed': 0, 'inserted': 0, 'skipped': 0, 'errors': 0},
    'chargesheets': {'processed': 0, 'inserted': 0, 'skipped': 0, 'errors': 0},
    'fsl_case_property': {'processed': 0, 'inserted': 0, 'skipped': 0, 'errors': 0}
}
```

---

## 🔐 Configuration

### Environment Variables Required (from config.py)

**Database**:
```
POSTGRES_HOST
POSTGRES_DB
POSTGRES_USER
POSTGRES_PASSWORD
POSTGRES_PORT
```

**API**:
```
DOPAMAS_API_URL        # Base URL (e.g., http://api.dopamas.com)
DOPAMAS_API_KEY        # API key for authentication
API_TIMEOUT            # Request timeout in seconds
API_MAX_RETRIES        # Max retry attempts (default: 3)
```

**Files / Media Storage** (REQUIRED):
```
FILES_MEDIA_BASE_PATH  # ⚠️ REQUIRED - Where to save PDFs (must be set, no default)
FILES_BASE_URL         # Base URL for accessing files
```

**ETL** (Optional):
```
LOG_LEVEL              # DEBUG, INFO, WARNING, ERROR
FILES_RETRY_DELAY_SECONDS  # Backoff delay for retries
CHUNK_OVERLAP_DAYS     # For date-based chunking
ENABLE_EMBEDDINGS      # true/false
```

**Table Overrides**:
```
CRIMES_TABLE           # Override target table name
```

---

## 📋 Logging

### Log File Location
```
logs/files_etl_YYYYMMDD_HHMMSS.log
```

### Log Format
```
[TIMESTAMP] - [LEVEL] - [MESSAGE]

Colors in console:
- DEBUG   → Cyan
- INFO    → Green
- WARNING → Yellow
- ERROR   → Red
- CRITICAL → Red on white
```

### Example Log Entries
```
2026-03-23 10:30:45 - INFO - ✅ Connected to database: dopams_db
2026-03-23 10:30:46 - INFO - 📥 Fetching distinct FIR_COPY values from table: crimes
2026-03-23 10:30:47 - INFO - Found 1000 distinct non-null FIR_COPY values
2026-03-23 10:30:48 - INFO - 📄 No existing file for FIR_COPY=FIR123, will download new file
2026-03-23 10:30:49 - INFO - ⬇️  Downloading FIR_COPY=FIR123 from http://api/files/FIR123 (attempt 1/3)
2026-03-23 10:30:50 - INFO - ✅ Downloaded FIR_COPY=FIR123 to /data-drive/.../FIR123.pdf, size=45678 bytes
```

---

## 🚀 Execution Modes

### Mode 1: Simple ETL (Direct Crime Files)
```bash
python etl-files.py
```
**Flow**:
1. Connect to PostgreSQL
2. Fetch all distinct FIR_COPY values from `crimes` table
3. For each FIR_COPY, download from `/files/{fir_copy}` API
4. Save to PDFs directory
5. Log results

### Mode 2: Full Pipeline (All APIs)
```bash
python etl_pipeline_files/main_standalone.py
```
**Flow**:
1. Initialize database and API connections
2. Get last processed date per API (for resume)
3. Process each API sequentially:
   - Crimes → Extract FIR_COPY → Load files metadata
   - Persons → Extract file fields → Load files metadata
   - Property → Extract file fields → Load files metadata
   - Interrogation → Extract file fields → Load files metadata
   - MO Seizures, Chargesheet, FSL Case Property → Similar process
4. After loading all file metadata, download files using file IDs
5. Report statistics

---

## ⚠️ Error Handling

### Retry Strategy for Downloads
```python
For each file_id:
    For attempt in range(1, max_retries + 1):
        Try:
            Call API
        Catch Timeout:
            Wait (base_delay * attempt) seconds → Retry
        Catch HTTP 429 (Rate Limited):
            Wait (base_delay * attempt * 2) seconds → Retry
            (or use Retry-After header if provided)
        Catch HTTP 5xx:
            Wait (base_delay * attempt) seconds → Retry
        Catch Non-retriable errors:
            Break (log error, mark as failed)
```

---

## 📝 Summary Table

| Aspect | Details |
|--------|---------|
| **Input Source** | Multiple APIs (Crimes, Persons, Property, IR, etc.) |
| **File References From** | API response fields like FIR_COPY, document URLs, etc. |
| **APIs Used** | 6+ DOPAMS APIs (configured in config.py) |
| **Database Table Written** | `files` table (metadata) |
| **Files Downloaded To** | `FILES_MEDIA_BASE_PATH` environment variable (e.g., `/mnt/shared-etl-files`) |
| **File Format** | PDF (named as `{file_id}.pdf`) |
| **Retry Logic** | Exponential backoff, max 3 attempts, handles rate limiting |
| **Idempotency** | Prevents duplicate extractions per composite key |
| **Logging** | Timestamped log files with statistics |

