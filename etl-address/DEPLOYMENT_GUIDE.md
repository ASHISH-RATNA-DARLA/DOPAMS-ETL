# ETL Address - Intelligent LLM Queueing Deployment Guide

## Quick Start

### Scenario 1: Single Ollama Instance (Minimal Setup)
```bash
# No env vars needed - uses defaults
python3 etl_address.py

# Or explicitly:
export ADDRESS_LLM_WORKER_COUNT=1
export ADDRESS_LLM_QUEUE_MAX=100
python3 etl_address.py
```

### Scenario 2: Multiple Ollama Instances (Optimal)
```bash
# For 3 Ollama instances on different hosts:
export ADDRESS_LLM_WORKER_COUNT=3
export ADDRESS_LLM_QUEUE_MAX=200
export OLLAMA_HOST=http://192.168.102.21:11434
python3 etl_address.py
```

### Scenario 3: Distributed Ollama (Advanced)
```bash
# For 5+ Ollama instances with high volume:
export ADDRESS_LLM_WORKER_COUNT=5
export ADDRESS_LLM_QUEUE_MAX=500
export ADDRESS_BATCH_SIZE=500
python3 etl_address.py
```

## Configuration Reference

### Environment Variables

| Variable | Default | Range | Purpose |
|----------|---------|-------|---------|
| `ADDRESS_LLM_WORKER_COUNT` | 1 | 1-32 | Concurrent LLM workers (match Ollama count) |
| `ADDRESS_LLM_QUEUE_MAX` | 100 | 10-1000 | Max records queued for LLM |
| `ADDRESS_MAX_WORKERS` | 32 | 1-64 | Total worker threads |
| `ADDRESS_BATCH_SIZE` | 500 | 10-2000 | KB processing batch size |
| `OLLAMA_HOST` | localhost:11434 | URL | Ollama service address |

### Tuning Guide

**If you see:**
- `llm_queue_full_skipped > 0` → Increase `ADDRESS_LLM_QUEUE_MAX`
- `queued_for_later > 50` → Increase `ADDRESS_LLM_WORKER_COUNT` (more instances)
- Processing very slow → Increase `ADDRESS_MAX_WORKERS`
- Memory issues → Decrease `ADDRESS_BATCH_SIZE` or `ADDRESS_MAX_WORKERS`

## Architecture

```
Main Batch Loop (32 workers)
  │
  ├─ Worker 1-29: Process KB-only records (instant)
  │  └─ Skip if LLM needed + queue full
  │
  ├─ Worker 30-32: Process with LLM (if needed)
  │  └─ Uses semaphore (only 3 concurrent)
  │
  └─ LLM Queue: Holds records needing LLM
     └─ After batches: Dedicated processing phase
```

## Monitoring

### Key Metrics to Watch

```log
processed=2500/12988 updated=1800 unchanged=400
llm=150 llm_skip=200 
enriched_locality=75 
failed=50
queued_for_later=120 queue_full_skip=0
```

**Interpretation:**
- `queued_for_later=120`: 120 records waiting for LLM capacity
- `queue_full_skip=0`: Good! Queue not overflowing
- `llm=150`: 150 records successfully resolved with LLM
- `failed=50`: Investigation needed if too high

### Heartbeat (every 30 seconds)

```
processed=5000/12988 updated=3500 unchanged=800 
llm=250 llm_skip=450 
enriched_locality=150 enriched_village=50 
country_propagated=200 country_from_nat=30 
failed=100 last_seen=62b081ce6e2f95e3e4479b52
```

## Troubleshooting

### Timeout Still Occurring?

1. **Check Ollama health:**
   ```bash
   curl -s http://192.168.102.21:11434/api/tags
   ```

2. **Increase timeout if needed:**
   ```bash
   export STEP_TIMEOUT_SEC=14400  # 4 hours instead of 2
   ```

3. **Check worker count:**
   ```bash
   export ADDRESS_MAX_WORKERS=64
   export ADDRESS_LLM_WORKER_COUNT=4
   ```

### High Failure Rate?

1. **Check KB health:**
   - Verify geo_reference table is populated
   - Check if district/mandal names are correct

2. **Reduce LLM dependency:**
   ```bash
   export ADDRESS_DISABLE_LLM=1  # KB-only mode for debugging
   ```

3. **Check logs:**
   ```bash
   tail -f logs/20260422_130720/etl-address/execution.log
   ```

## Performance Expectations

### Estimated Times (12,988 records)

| Config | Time | Notes |
|--------|------|-------|
| KB-only (no LLM) | 10-15 min | Fast, KB coverage ~60% |
| 1 Ollama instance | 2-3 hours | Sequential LLM calls |
| 3 Ollama instances | 45-60 min | 3 parallel LLM calls |
| 5 Ollama instances | 30-45 min | 5 parallel LLM calls |

## Running from Master Orchestrator

```bash
cd etl_master/
export ADDRESS_LLM_WORKER_COUNT=3
python3 master_etl.py --config input.txt
```

## Rollback

If issues arise:
```bash
# Run KB-only (safe fallback)
export ADDRESS_DISABLE_LLM=1
python3 etl_address.py

# Or revert env to defaults
unset ADDRESS_LLM_WORKER_COUNT
unset ADDRESS_LLM_QUEUE_MAX
python3 etl_address.py  # Uses defaults
```

## Notes

- Implementation is **backwards compatible**
- No database migrations required
- Resumable from checkpoints
- Safe for production use
- No schema changes
