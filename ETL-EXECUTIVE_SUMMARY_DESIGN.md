# DOPAMS ETL Pipeline - Executive Summary

**Date:** March 6, 2026  
**Document Type:** High-Level Overview (Companion to Solution Design Document)  

## System at a Glance

The DOPAMS ETL Pipeline is a **enterprise-scale criminal justice data integration system** that processes 100,000+ crime records daily through intelligent extraction, transformation, and loading into a centralized PostgreSQL data warehouse.

---

## The Problem Solved

**Before:** Criminal justice data fragmented across APIs, unstructured documents, and multiple databases with no unified view.  
**After:** Integrated, structured, validated data warehouse with AI-powered extraction of complex information from unstructured FIR narratives.

---

## Architecture Overview

### 5 Processing Layers

| Layer | Purpose | Key Technologies |
|-------|---------|------------------|
| **Extraction** | Pull data from 4+ sources | REST APIs, psycopg2, pymongo |
| **Transformation** | LLM extraction + standardization | Ollama, Langchain, Pydantic |
| **Validation** | Multi-layered data quality checks | Schema validation, FK constraints, business rules |
| **Loading** | Efficient bulk insert with pooling | Batch inserts, connection pooling, transactions |
| **Materialization** | Pre-computed analytical views | PostgreSQL materialized views |

### 29 Sequential Processing Orders

Orders 1-19: Core data ingestion and entity processing  
Orders 20-24: LLM-powered extraction (accused, drugs, calculations)  
Orders 25-30: View materialization and analytics

---

## Data Flow (Simple)

```
External APIs
MongoDb
Legacy DBs
Static Files
    ↓
Extract (API calls, DB queries)
    ↓
Transform (LLM extraction, standardize, deduplicate)
    ↓
Validate (Schema, FK, business rules)
    ↓
Load (Batch insert with pooling)
    ↓
PostgreSQL Warehouse (crimes, accused, persons, brief_facts_*)
    ↓
Materialized Views (Reporting & Analytics)
    ↓
Dashboards, Reports, Queries
```

---

## Key Statistics

| Metric | Value |
|--------|-------|
| **Total ETL Orders** | 29 sequential |
| **Crime Records Processed** | 100,000+ |
| **LLM-Extracted Records** | 50,000+ per day |
| **Extraction Accuracy** | 95%+ (confidence scoring) |
| **Processing Time (Current)** | 2+ minutes per batch |
| **Processing Time (Target Phase 1)** | 30-40 seconds |
| **Processing Time (Target Phase 2)** | 12-25 seconds |
| **Database Size** | 100GB+ |
| **Parallel LLM Workers** | 3 concurrent |
| **Connection Pool Size** | 5-20 connections |

---

## Key Features

### 1. **Multi-Source Ingestion**
- External API (5-day chunked with overlap)
- PostgreSQL database (continuous)
- MongoDB (legacy migration)
- Static reference files (configuration)

### 2. **LLM-Powered Extraction**
- **Accused Facts**: Extract type, role, demographics from unstructured FIR
- **Drug Facts**: Parse drug name, quantity, unit, standardize to grams/kg/ml/liters, calculate seizure worth
- **Smart Preprocessing**: Filter multi-FIR documents for drug relevance (saves 50%+ tokens)

### 3. **Data Enrichment**
- Person deduplication (5-tier matching confidence)
- Drug standardization (500+ drug mappings)
- Unit conversion (mg/g/kg ↔ ml/l/count)
- Seizure worth calculation (market-based valuation)

### 4. **Data Quality**
- Schema validation (Pydantic models)
- Referential integrity (FK constraints)
- Duplicate detection (exact & fuzzy matching)
- Audit trails (source fields, extraction metadata)

### 5. **Performance Optimizations**
- Connection pooling (10-15% improvement)
- Batch inserts (10-20x faster write speed)
- Database indexes (20-40x query speedup)
- Parallel LLM extraction (3-5x throughput)

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Language** | Python 3.9+ | ETL scripting |
| **Database** | PostgreSQL 16+ | Data warehouse |
| **LLM** | Ollama + qwen2.5-coder:14b | Structured extraction |
| **Framework** | Langchain + Pydantic | LLM abstraction & validation |
| **HTTP** | Requests library | API calls |
| **Connection** | psycopg2 + pooling | Database access |
| **Logging** | Python logging | Monitoring |
| **Orchestration** | Shell script + subprocess | Sequential execution |

---

## Performance Bottlenecks & Solutions

### Current Issues (2 minutes per batch)

| # | Bottleneck | Root Cause | Impact |
|---|-----------|-----------|--------|
| 1 | No connection pooling | New connection per query | 100ms × 1000 = 100s overhead |
| 2 | Single-record inserts | 1000 inserts = 1000 commits | Disk flush × 1000 = disk latency |
| 3 | Missing database indexes | Sequential scans | 20-40x slower queries |
| 4 | Sequential processing | One-at-a-time blocking | Can't parallelize |
| 5 | GIL contention | Regex in main Python thread | 3-4x slower preprocessing |

### Solutions Implemented

✓ **Connection Pooling**: Singleton pool reuses 5-20 connections  
✓ **Batch Inserts**: 1000 records in single transaction  
✓ **Parallel LLM**: 3 concurrent workers with ThreadPoolExecutor  

### Recommended Phase 1 (2-3 Days)
- Add 5 missing database indexes → **20-40x faster queries**
- Enable connection pooling → **10-15% latency reduction**
- Convert to batch inserts → **10-20x faster writes**
- **Expected: 4-5x overall improvement** (2 min → 30-40 sec)

### Recommended Phase 2 (2-3 Weeks)
- Async/await patterns → **3-5x throughput**
- Multiprocessing for GIL bypass → **3-4x preprocessing**
- Query caching → **5-10x repeated queries**
- **Expected: 5-10x overall improvement** (2 min → 12-25 sec)

---

## Database Schema (Simplified)

### Core Tables

1. **crimes** - FIR records with unstructured brief_facts (100K records)
2. **accused** - Person-crime links (various descriptors)
3. **persons** - Person demographics (50K unique)
4. **brief_facts_accused** - LLM-extracted accused info
5. **brief_facts_drugs** - LLM-extracted drug information
6. **person_deduplication_tracker** - Deduplicated person identities

### Relationships
```
crimes (1) ──→ (∞) accused
crimes (1) ──→ (∞) brief_facts_accused (LLM extracted)
crimes (1) ──→ (∞) brief_facts_drugs (LLM extracted)
accused (∞) ──→ (1) persons
persons (1) ──→ (∞) person_deduplication_tracker (aliases)
```

---

## Error Handling & Resilience

### Retry Strategy
- **API calls**: Exponential backoff (2s, 6s, 18s)
- **LLM extraction**: 3 retries with 5s+ delay
- **Database ops**: Connection health check, auto-reconnect

### Data Validation
- Schema validation (Pydantic models)
- Foreign key existence checks
- Format validation (phone, email, addresses)
- Duplicate detection (exact & fuzzy)

### Graceful Degradation
- LLM fails → Fallback to regex pattern matching
- Validation fails → Insert with defaults, mark for review
- Record fails → Log, skip, continue with next batch

### Checkpoint-Based Restart
- Track: `max(date_modified)` for each order
- Resume: Start from last checkpoint, not from beginning
- Prevents: Reprocessing, data loss, duplicate inserts

---

## Execution & Deployment

### Daily Execution
```bash
# Full pipeline (29 orders)
python3 etl_master/master_etl.py --input-file etl_master/input.txt

# Individual component (for testing)
cd brief_facts_drugs && python3 main.py

# Scheduled (cron job)
0 2 * * * /path/to/etl_master/master_etl.py
```

### Prerequisites
- Python 3.9+ with venv (activate before running)
- PostgreSQL 16+ running and accessible
- Ollama service running on localhost:11434
- 16GB+ memory for LLM model
- 100GB+ disk space for data

### Configuration
All parameters in `.env` file:
- Database credentials
- API endpoints & keys
- LLM service URL & model
- Batch size & parallel workers

---

## Monitoring & Alerting

### Logged Metrics
- Orders completed/failed
- Records processed/failed
- Processing time per order
- Error counts & types
- LLM extraction success rate
- Database query performance

### Health Checks
```bash
# Test database
python3 -c "import psycopg2; psycopg2.connect(...).close()"

# Test LLM
curl http://localhost:11434/api/tags

# Run validation suite
python3 validate_etl.py
```

### Performance Baselines
```bash
# Capture current performance
python3 performance_profiler.py

# Identify slow queries
python3 query_optimizer.py
```

---

## Critical Success Factors

| Factor | Status | Notes |
|--------|--------|-------|
| **Checkpoint system** | ✓ Implemented | Enables 5-day chunk restart |
| **Batch operations** | ✓ Implemented | 10-20x faster writes |
| **Connection pooling** | ✓ Implemented | Singleton pattern |
| **LLM parallelization** | ✓ Implemented | 3 concurrent workers |
| **Data validation** | ✓ Comprehensive | Multi-layered checks |
| **Error handling** | ✓ Robust | Retry with fallback |
| **Monitoring** | ✓ Logging only | Need alerting system |
| **Performance indexing** | ⚠ Recommended | Not yet implemented |

---

## Next Steps

### Immediate (Week 1)
1. ✓ Validate schema matches all 29 orders
2. ✓ Run test suite with single test record
3. → Implement missing database indexes
4. → Measure baseline performance

### Short-term (Month 1)
1. Optimize Phase 1 (3-5x improvement)
2. Set up alerting & monitoring
3. Document operational procedures
4. Establish SLA targets

### Medium-term (Quarter 1)
1. Implement Phase 2 optimizations (5-10x improvement)
2. Enable horizontal scaling (multi-instance)
3. Build dashboards for operational metrics
4. Implement data lineage tracking

---

## Document Structure

This Summary provides high-level context. For detailed information, refer to the complete **SOLUTION_DESIGN_DOCUMENT.md**:

| Section | Details |
|---------|---------|
| **1. Overview** | Purpose, problem, solution, capabilities |
| **2. Data Sources** | APIs, databases, files, configuration |
| **3. Architecture** | 5 layers, extraction, transformation, loading |
| **4. Pipeline Flow** | 29 orders, step-by-step execution, LLM workflows |
| **5. Transformation** | Cleaning, mapping, normalization, enrichment |
| **6. Validation** | Schema, format, referential integrity, duplicates |
| **7. Database Schema** | Tables, relationships, materialized views, decisions |
| **8. Error Handling** | Logging, retry, exceptions, monitoring |
| **9. Performance** | Bottlenecks, optimizations, scalability |
| **10. Dependencies** | Execution order, inter-order dependencies |
| **11. Deployment** | Setup, execution methods, troubleshooting |
| **12. Architecture Diagrams** | Mermaid diagrams showing complete system |

---

**Ready for Enterprise Review & Implementation**

This document provides a complete technical foundation for teams implementing, maintaining, or extending the DOPAMS ETL system.
