# QUICK REFERENCE CARD
# Advanced KB Matching for DOPAMS ETL Drug Extraction

## 🎯 What Changed?

**OLD (Broken):**
```
LLM extracts "ganaj" → Stored as "ganaj" in DB ❌
No fuzzy matching against KB
No edge case validation
```

**NEW (Production-Grade):**
```
LLM extracts "ganaj" → Fuzzy matches to "Ganja" (92% similar)
Validates form/unit consistency
Checks quantity sanity
Stores full audit trail ✅
```

---

## ⚡ Quick Start (5 min)

```bash
# 1. Check prerequisites
python deploy_kb_matching.py --check

# 2. Create backups
python deploy_kb_matching.py --backup

# 3. Generate SQL
python deploy_kb_matching.py --sql-only deploy_migrations.sql

# 4. Apply database changes
psql -U dopams_user -d dopams_db -f deploy_migrations.sql

# 5. Run tests
pytest test_kb_matcher_advanced.py -v

# 6. Follow integration guide
cat PRODUCTION_DEPLOYMENT_GUIDE.md | less
```

---

## 📊 Monitor in Real-Time

```bash
# Terminal 1: Watch ETL logs
tail -f /var/log/dopams/etl.log | grep -E "KB|refinement|commercial"

# Terminal 2: Monitor DB audit table
watch -n 5 'psql -qAtX -d dopams_db -c "SELECT match_type, COUNT(*) FROM drug_kb_match_audit WHERE created_at > NOW() - INTERVAL 5m GROUP BY match_type ORDER BY COUNT DESC"'

# Terminal 3: Database performance
watch -n 10 'psql -qAtX -d dopams_db -c "SELECT table_name, pg_size_pretty(pg_total_relation_size(schemaname||'"'"'."'"'"'||table_name)) FROM information_schema.tables WHERE table_name IN ('"'"'drug_kb_match_audit'"'"', '"'"'drug_extraction_rejections'"'"')"'
```

---

## 🔍 Key Queries (Ctrl+C in terminal to interrupt)

### **Performance Check**
```sql
SELECT 
    COUNT(*) as total_processed,
    ROUND(AVG(EXTRACT(EPOCH FROM (NOW() - created_at)))::numeric, 2) as age_seconds
FROM drug_kb_match_audit
WHERE created_at > NOW() - INTERVAL '5 minutes';
```

### **Match Quality**
```sql
SELECT 
    match_type,
    COUNT(*) as count,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as percent,
    ROUND(AVG(match_ratio)::numeric, 2) as avg_ratio
FROM drug_kb_match_audit
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY match_type
ORDER BY count DESC;
```

### **Rejection Analysis** (Why are drugs rejected?)
```sql
SELECT 
    rejection_reason,
    COUNT(*) as count,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as percent
FROM drug_extraction_rejections
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY rejection_reason
ORDER BY count DESC;
```

### **Confidence Improvement** (Is KB matching helping?)
```sql
SELECT 
    ROUND(confidence_original, 1) as before,
    ROUND(confidence_adjusted, 1) as after,
    COUNT(*) as drugs
FROM drug_kb_match_audit
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY 
    ROUND(confidence_original, 1),
    ROUND(confidence_adjusted, 1)
ORDER BY drugs DESC;
```

### **Commercial Accuracy** (Are we catching commercial quantities?)
```sql
SELECT 
    is_commercial,
    COUNT(*) as count,
    ROUND(AVG(match_ratio)::numeric, 2) as avg_match
FROM drug_kb_match_audit
WHERE created_at > NOW() - INTERVAL '1 hour'
GROUP BY is_commercial;
```

### **False Positive Check** (Are any real drugs rejected?)
```sql
SELECT 
    raw_drug_name,
    COUNT(*) as rejections,
    was_false_positive,
    rejection_reason
FROM drug_extraction_rejections
WHERE created_at > NOW() - INTERVAL '24 hours'
AND was_false_positive = TRUE
GROUP BY raw_drug_name, was_false_positive, rejection_reason
ORDER BY rejections DESC;
```

---

## 🚨 When Something Goes Wrong

### **Symptom:** Very low match rate (<30%)

```bash
# Diagnosis
psql -d dopams_db -c "SELECT COUNT(*) as no_matches FROM drug_kb_match_audit WHERE match_type = 'no_match';"

# Check KB
psql -d dopams_db -c "SELECT COUNT(*) FROM drug_categories WHERE is_verified=true;"

# Fix: Ensure KB is populated
```

### **Symptom:** Very high rejection rate (>30%)

```bash
# See what's being rejected
psql -d dopams_db -c "SELECT rejection_reason, COUNT(*) FROM drug_extraction_rejections GROUP BY 1 ORDER BY 2 DESC;"

# Adjust thresholds in config.py KB_MATCH_CONFIG:
# MIN_CONFIDENCE_UNKNOWN = 0.50  # Lower this if too strict
# Then restart ETL
```

### **Symptom:** Slow processing (>100ms per drug)

```bash
# Check DB performance
psql -d dopams_db -c "SELECT * FROM pg_stat_statements WHERE query LIKE '%drug_kb%' ORDER BY total_time DESC;"

# Possible fixes:
# 1. Analyze tables: ANALYZE drug_kb_match_audit;
# 2. Rebuild indexes: REINDEX TABLE drug_kb_match_audit;
# 3. Archive old records (>30 days)
```

### **Symptom:** Database disk full

```bash
# Check table size
psql -d dopams_db -c "SELECT table_name, pg_size_pretty(pg_total_relation_size(schemaname||'.'||table_name)) FROM information_schema.tables ORDER BY pg_total_relation_size DESC LIMIT 10;"

# Archive old records
psql -d dopams_db << 'EOF'
CREATE TABLE drug_kb_match_audit_archive_2024q1 AS
SELECT * FROM drug_kb_match_audit 
WHERE created_at < NOW() - INTERVAL '90 days';

DELETE FROM drug_kb_match_audit 
WHERE created_at < NOW() - INTERVAL '90 days';

VACUUM ANALYZE drug_kb_match_audit;
EOF
```

---

## ✅ Success Checklist

After deployment, verify:

- [ ] Tests passing: `pytest test_kb_matcher_advanced.py` shows >95% pass rate
- [ ] Audit tables have data: `SELECT COUNT(*) FROM drug_kb_match_audit;` > 0
- [ ] Match rate healthy: `SELECT match_type FROM drug_kb_match_audit;` mostly non-null
- [ ] Performance good: Average <50ms per drug observed
- [ ] No errors in logs: `grep ERROR /var/log/dopams/etl.log` returns nothing
- [ ] Confidence improving: Average confidence_adjusted > confidence_original
- [ ] Processing speed: >100 drugs/sec throughput

---

## 📝 Files You'll Touch

| File | Change | Effort | Risk |
|------|--------|--------|------|
| extractor.py | Add KB matcher initialization & call refine_drugs function | 30 min | Medium |
| db.py | Add audit logging functions | 10 min | Low |
| main.py | Replace confidence check with apply_validation_rules | 5 min | Low |
| config.py | Add KB_MATCH_CONFIG dictionary | 5 min | Low |
| Database | Create 2 audit tables + 4 indexes | 2 min | Low |

**Total Integration Time:** ~1 hour (mostly following the guide)

---

## 🔄 Rollback (If Needed)

```bash
# Stop ETL
systemctl stop dopams-etl

# Restore from backups
cp brief_facts_drugs/extractor.py.backup brief_facts_drugs/extractor.py
cp brief_facts_drugs/db.py.backup brief_facts_drugs/db.py

# Clear audit tables
psql -d dopams_db -c "TRUNCATE drug_extraction_rejections; TRUNCATE drug_kb_match_audit;"

# Restart
systemctl start dopams-etl

# Verify
tail -f /var/log/dopams/etl.log
```

**Rollback time:** ~5 minutes

---

## 📞 Help

- **Integration Questions:** See `PRODUCTION_DEPLOYMENT_GUIDE.md`
- **Test Failures:** Check `test_kb_matcher_advanced.py` for expected behavior
- **SQL Issues:** Run diagnostic queries above to identify problem
- **Performance:** Check logs for bottleneck, see troubleshooting section above

---

## 🎓 Understanding the Solution

### **The 4 Matching Thresholds**

```
Exact (95%+) → Confidence +15%
  "Ganja" matches "Ganja" perfectly

Fuzzy High (82-94%) → Confidence +8%
  "ganaj" matches "Ganja" at 92%

Fuzzy Medium (72-81%) → No change
  "plant material" matches "Cannabis" at 75%

Low (<60%) or No Match → Confidence -10%/Rejected
  "xyz" doesn't match anything
```

### **The 7 Validation Stages**

1. KB Fuzzy Match: `DrugKBMatcherAdvanced.match()` returns MatchResult
2. Form-Unit Check: Liquid drugs must use ml/L, solid uses kg/g
3. Qty Sanity: Flag outliers (5000kg very unrealistic)
4. Commercial Check: ≥threshold = is_commercial=true
5. NDPS Validation: Check drug against NDPS Act sections
6. Confidence Adjust: original ± boost based on KB match
7. Audit Trail: Store everything for compliance review

### **The NDPS Rules**

```python
Ganja 20kg = commercial
Heroin 250g = commercial
Cocaine 500g = commercial
MDMA 50g = commercial
LSD 100 blots = commercial
```

All encoded in `COMMERCIAL_QUANTITY_NDPS` dict in kb_matcher_advanced.py

---

## 🚀 Quick Deploy Command

```bash
# All-in-one (with backups + tests)
cd brief_facts_drugs && \
python deploy_kb_matching.py --backup && \
python deploy_kb_matching.py --sql-only deploy_migrations.sql && \
psql -U dopams_user -d dopams_db -f deploy_migrations.sql && \
pytest test_kb_matcher_advanced.py -v && \
echo "✅ Ready for integration!" && \
python deploy_kb_matching.py --guide | head -50
```

---

**Version:** 1.0  
**Confidence:** Production-Ready ✅  
**Last Updated:** 2024
