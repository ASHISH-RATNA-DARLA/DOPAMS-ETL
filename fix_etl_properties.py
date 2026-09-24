import re

with open('etl-properties/etl_properties.py', 'r') as f:
    content = f.read()

# Add imports
imports = """
try:
    from etl_fk_retry_queue import push_fk_failure, drain_fk_queue as _drain_fk_queue
except ImportError:
    push_fk_failure = None
    _drain_fk_queue = None
"""
content = re.sub(r"from env_utils import get_etl_run_id", "from env_utils import get_etl_run_id\n" + imports, content)

# Remove PENDING_FK_TABLE
content = re.sub(r"PENDING_FK_TABLE = 'properties_pending_fk'\n", "", content)

# Remove create_pending_fk_table
content = re.sub(r"    def create_pending_fk_table\(self\).*?raise\n\n", "", content, flags=re.DOTALL)
# Remove self.create_pending_fk_table() call
content = re.sub(r"        self\.create_pending_fk_table\(\)\n", "", content)

# Replace queue_pending_fk
queue_replacement = """    def queue_pending_fk(self, property_raw: Dict, crime_id: str, conn, cursor):
        \"\"\"Insert a property record into the pending FK retry queue.\"\"\"
        property_id = property_raw.get('PROPERTY_ID', 'unknown')
        if push_fk_failure:
            push_fk_failure(conn, source_table='properties', record_id=property_id,
                            record_json=json.dumps(property_raw, default=str),
                            missing_fk_column='crime_id', missing_fk_value=crime_id)
            with self.stats_lock:
                self.stats['total_pending_fk'] += 1
"""
content = re.sub(r"    def queue_pending_fk\(self, property_raw: Dict, crime_id: str, conn, cursor\):.*?logger\.error\(f\"Failed to queue pending FK for property \{property_id\}: \{e\}\"\)\n", queue_replacement, content, flags=re.DOTALL)

# Replace retry_pending_fk
retry_replacement = """    def _retry_property_record(self, conn, record_json_str: str) -> bool:
        try:
            raw_data = json.loads(record_json_str)
            crime_id = raw_data.get('CRIME_ID')
            # Check if crime_id exists in our in-memory set (or reload if needed)
            if crime_id not in self.crime_ids:
                return False
            
            prop = self.transform_property(raw_data)
            with conn.cursor() as cur:
                success = self.insert_property(prop, conn, cur)
            return success
        except Exception as e:
            logger.error(f"Error retrying property record: {e}")
            return False

    def retry_pending_fk(self):
        \"\"\"Retry all unresolved pending FK records.\"\"\"
        if _drain_fk_queue:
            with self.db_pool.get_connection_context() as conn:
                _drain_fk_queue(conn, 'properties', self._retry_property_record)
"""
content = re.sub(r"    def retry_pending_fk\(self\):.*?logger\.error\(f\"❌ Error during pending FK retry: \{e\}\"\)\n", retry_replacement, content, flags=re.DOTALL)

# In the run() method, change the SELECT COUNT(*) FROM PENDING_FK_TABLE
run_stats_replacement = """                    cursor.execute(f"SELECT COUNT(*) FROM {PROPERTIES_TABLE}")
                    db_properties_count = cursor.fetchone()[0]
                    cursor.execute("SELECT COUNT(*) FROM etl_bookkeeping WHERE kind = 'fk_retry' AND module_name = 'properties' AND resolved = FALSE")
                    pending_count = cursor.fetchone()[0]"""
content = re.sub(r"                    cursor\.execute\(f\"SELECT COUNT\(\*\) FROM \{PROPERTIES_TABLE\}\"\)\n                    db_properties_count = cursor\.fetchone\(\)\[0\]\n                    cursor\.execute\(f\"SELECT COUNT\(\*\) FROM \{PENDING_FK_TABLE\} WHERE resolved = FALSE\"\)\n                    pending_count = cursor\.fetchone\(\)\[0\]", run_stats_replacement, content)

with open('etl-properties/etl_properties.py', 'w') as f:
    f.write(content)
print("Done etl_properties.py")
