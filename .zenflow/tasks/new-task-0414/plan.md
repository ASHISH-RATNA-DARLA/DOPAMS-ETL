# Auto

## Configuration
- **Artifacts Path**: {@artifacts_path} → `.zenflow/tasks/{task_id}`

---

## Agent Instructions

Ask the user questions when anything is unclear or needs their input. This includes:
- Ambiguous or incomplete requirements
- Technical decisions that affect architecture or user experience
- Trade-offs that require business context

Do not make assumptions on important decisions — get clarification first.

---

## Workflow Steps

### [x] Step: Investigation & Root Cause Analysis
- Analyzed `.env.server` for configuration.
- Cross-checked `DB-schema.sql` for table structure and constraints.
- Analyzed `etl_files_media_server/main.py` for downloader logic.
- Identified potential cause for missing files: configuration changes and logic flaws.

### [x] Step: Implementation of Recommendations
- Create `etl-files/sync_files_state.py` to synchronize filesystem and database.
- Patch `etl_files_media_server/main.py` for improved error handling and existence validation.
- Provide final audit report.

