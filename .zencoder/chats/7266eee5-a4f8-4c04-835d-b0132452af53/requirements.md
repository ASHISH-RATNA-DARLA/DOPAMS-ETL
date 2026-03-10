# Product Requirements Document (PRD) - Brief Facts Accused ETL Improvements

## 1. Overview
The Brief Facts (Accused) ETL process extracts accused details from FIR brief facts using LLMs and rules. Currently, it suffers from high NULL rates in `person_id` (database matching failures), missing demographic fields (LLM extraction issues), and poor classification (keyword matching limitations).

## 2. Goals
- **Improve Identity Matching**: Reduce `person_id` NULL rate by scoping the search to the specific crime.
- **Enhance Extraction Reliability**: Implement retries and correction prompts for LLM outputs.
- **Better Classification**: Expand the keyword-based rules for `accused_type` to cover all 8 schema-allowed categories.

## 3. Requirements

### 3.1 Scoped Identity Matching
- **Current Issue**: Global search in `persons` table leads to mismatches or no matches (100% NULL rate reported).
- **Requirement**: Replace global search with a scoped join: `crimes` → `accused` → `persons` filtered by `crime_id`.
- **Logic**: Fetch people already linked to the `crime_id` in the `accused` table, then perform fuzzy matching on names/aliases within this small set.
- **Threshold**: Use a lower fuzzy matching threshold (e.g., 75-80) since the search space is limited to the current case.

### 3.2 Robust LLM Extraction
- **Current Issue**: Silent drops on malformed or truncated JSON.
- **Requirement**: Implement a retry loop (1 retry).
- **Correction**: If the first attempt fails to parse, send a second prompt: "Fix this JSON to match schema" along with the error and the raw output.
- **Guard**: Ensure `max_tokens` is sufficient for the expected output.

### 3.3 Expanded Rule-Based Classification
- **Current Issue**: Strict keyword matching misses many cases (60% NULL rate).
- **Requirement**: Expand the keyword dictionary for `accused_type`.
- **Allowed Categories**:
    - `peddler`
    - `consumer`
    - `supplier`
    - `harbourer`
    - `organizer_kingpin`
    - `processor`
    - `financier`
    - `manufacturer`
- **Mapping**: Best judgment for investigative terms (e.g., "distributor" -> "supplier", "mastermind" -> "organizer_kingpin").

### 3.4 Schema Compliance
- Ensure all writes match the `brief_facts_accused` table schema.
- Map `unknown` status/type to `NULL` to satisfy DB constraints if necessary.

## 4. Non-Requirements
- No CSV export to local downloads folder is required for the production pipeline.
- No changes to the core `crimes` or `persons` table schemas.

