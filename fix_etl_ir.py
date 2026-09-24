import re

with open('etl-ir/ir_etl.py', 'r') as f:
    content = f.read()

# Add imports for etl_fk_retry_queue
imports = """
try:
    from etl_fk_retry_queue import push_fk_failure, drain_fk_queue as _drain_fk_queue
except ImportError:
    push_fk_failure = None
    _drain_fk_queue = None
"""
content = re.sub(r"from env_utils import get_etl_run_id", "from env_utils import get_etl_run_id\n" + imports, content)

# Remove all IR_*_TABLE definitions (except IR_TABLE and CRIMES_TABLE)
content = re.sub(r"IR_ASSOCIATE_DETAILS_TABLE.*?PENDING_FK_TABLE = 'ir_pending_fk'\n", "PENDING_FK_TABLE = 'ir_pending_fk'\n", content, flags=re.DOTALL)
content = re.sub(r"PENDING_FK_TABLE = 'ir_pending_fk'\n", "", content)

# Remove ensure_pending_table and ensure_schema_exists entirely
content = re.sub(r"    def ensure_pending_table\(self\).*?conn\.commit\(\)\n", "", content, flags=re.DOTALL)
content = re.sub(r"    def ensure_schema_exists\(self\).*?logger\.info\(\"✅ Ensured.*?\n", "", content, flags=re.DOTALL)
content = re.sub(r"            self\.ensure_pending_table\(\)\n", "", content)
content = re.sub(r"            self\.ensure_schema_exists\(\)\n", "", content)

# Replace queue_pending_fk and retry_pending_fk
queue_replacement = """    def queue_pending_fk(self, ir_raw: Dict, crime_id: str, conn, cursor):
        ir_id = ir_raw.get('INTERROGATION_REPORT_ID', 'unknown')
        if push_fk_failure:
            push_fk_failure(conn, source_table='interrogation_reports', record_id=ir_id,
                            record_json=json.dumps(ir_raw, default=str),
                            missing_fk_column='crime_id', missing_fk_value=crime_id)
            with self.stats_lock:
                self.stats['total_pending_fk'] += 1

    def _retry_ir_record(self, conn, record_json_str: str) -> bool:
        try:
            raw_data = json.loads(record_json_str)
            crime_id = raw_data.get('CRIME_ID')
            if crime_id not in self.crime_ids:
                return False
            
            with conn.cursor() as cur:
                success = self.insert_main_record(raw_data, cur)
            return success
        except Exception as e:
            logger.error(f"Error retrying IR record: {e}")
            return False

    def retry_pending_fk(self):
        if _drain_fk_queue:
            with self.db_pool.get_connection_context() as conn:
                _drain_fk_queue(conn, 'interrogation_reports', self._retry_ir_record)
"""
content = re.sub(r"    def queue_pending_fk\(self.*?logger\.error\(f\"❌ Error during pending FK retry: \{e\}\"\)\n", queue_replacement, content, flags=re.DOTALL)

# Modify run() to replace PENDING_FK_TABLE with etl_bookkeeping
run_stats_replacement = """                    cur.execute(f"SELECT COUNT(*) FROM {IR_TABLE}")
                    db_ir_count = cur.fetchone()[0]
                    cur.execute("SELECT COUNT(*) FROM etl_bookkeeping WHERE kind = 'fk_retry' AND module_name = 'interrogation_reports' AND resolved = FALSE")
                    pending_count = cur.fetchone()[0]"""
content = re.sub(r"                    cur\.execute\(f\"SELECT COUNT\(\*\) FROM \{IR_TABLE\}\"\)\n                    db_ir_count = cur\.fetchone\(\)\[0\]\n                    cur\.execute\(f\"SELECT COUNT\(\*\) FROM \{PENDING_FK_TABLE\} WHERE resolved = FALSE\"\)\n                    pending_count = cur\.fetchone\(\)\[0\]", run_stats_replacement, content)

with open('etl-ir/ir_etl.py', 'w') as f:
    f.write(content)
print("Done base replacements")
