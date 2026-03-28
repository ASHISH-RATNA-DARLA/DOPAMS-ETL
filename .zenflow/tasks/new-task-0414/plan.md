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

### [x] Step: Prioritization of Downloads
- Updated `main.py` SQL query to prioritize `crime`, `chargesheets`, and `interrogation` source types.

