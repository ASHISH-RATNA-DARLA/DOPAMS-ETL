# Intelligent LLM Worker Queueing Implementation

## Overview
Implemented a smart queueing system for etl-address to prevent worker threads from blocking on LLM calls. Instead, only N workers (matching Ollama instances) handle LLM resolution, while remaining workers process KB-only records.

## Changes Made

### 1. Added Configuration Variables
```python
LLM_WORKER_COUNT  = int(os.environ.get("ADDRESS_LLM_WORKER_COUNT", "1"))
LLM_QUEUE_MAX     = int(os.environ.get("ADDRESS_LLM_QUEUE_MAX", "100"))
```

**Environment Variables:**
- `ADDRESS_LLM_WORKER_COUNT`: Number of concurrent LLM workers (default: 1, set to number of Ollama instances)
- `ADDRESS_LLM_QUEUE_MAX`: Max records queued waiting for LLM (default: 100)

### 2. Created LLMQueueManager Class
```python
class LLMQueueManager:
    """Manages queueing for LLM-needing records with semaphore-based capacity control."""
    - can_acquire(): Non-blocking check for LLM capacity
    - release(): Release LLM capacity after use
    - queue_record(): Add record to LLM queue (non-blocking)
    - dequeue_record(): Pull next record from queue
    - queue_size(): Current queue size
```

### 3. Added Prediction Function: _will_need_llm()
- Pre-evaluates records to determine if they'll need LLM resolution
- Runs KB resolution first, checks if complete
- Only calls LLM if KB is partial AND record has usable signal
- Avoids wasting LLM capacity on impossible cases

### 4. Modified process_record() Function
**New Flow:**
1. Check if record will need LLM
2. If yes and no LLM capacity available:
   - Queue record for later processing
   - Return immediately (don't block)
3. If capacity available or no LLM needed:
   - Process normally
   - Release capacity when done
4. Always return True (processed or queued)

**Return Value:** Now returns `bool` to indicate if record was processed

### 5. Added Dedicated Queue Processing Phase
After main batch processing completes:
```python
# Process queued LLM records with dedicated workers
if llm_queue:
    queued_count = llm_queue.queue_size()
    if queued_count > 0:
        while True:
            row = llm_queue.dequeue_record()
            if not row:
                break
            process_record(pool, row, llm, stats, llm_queue)
```

### 6. Added Monitoring Metrics
New stats tracked:
- `llm_queued_for_later`: Records queued for LLM processing
- `llm_queue_full_skipped`: Records skipped when queue full (retry in next run)

## Behavior

### With 1 Ollama Instance (default)
```
Worker 1-32: Process KB-only records immediately
Only 1 worker: Can handle LLM requests at a time
Records needing LLM: Queued (up to 100), processed after batches
```

### With 3 Ollama Instances (ADDRESS_LLM_WORKER_COUNT=3)
```
Worker 1-29: Process KB-only records immediately (non-blocking)
Workers 30-32: Handle up to 3 parallel LLM calls
Records needing LLM: Queued, processed as workers available
No worker idle time waiting for slow LLM service
```

## Performance Impact

**Before:**
- All 32 workers could block on slow/hanging LLM calls
- 2-hour timeout exceeded due to bottleneck
- Entire pipeline fails after 3 retries

**After:**
- Only N workers can be blocked by LLM (N = Ollama instance count)
- Remaining 32-N workers continue KB processing
- Total processing time: ~hours instead of stuck at 2 hours
- Better CPU/network utilization

## Configuration Examples

**For 1 Ollama instance (default):**
```bash
# No env vars needed - defaults to 1 LLM worker
python3 etl_address.py
```

**For 3 Ollama instances:**
```bash
export ADDRESS_LLM_WORKER_COUNT=3
export ADDRESS_LLM_QUEUE_MAX=200
python3 etl_address.py
```

**For distributed Ollama (5 instances):**
```bash
export ADDRESS_LLM_WORKER_COUNT=5
export ADDRESS_LLM_QUEUE_MAX=500
python3 etl_address.py
```

## Monitoring

Watch the heartbeat for queue stats:
```
processed=1500/12988 updated=950 unchanged=200 
llm=45 llm_skip=150 
queued_for_later=205 queue_full_skip=0
```

- `queued_for_later`: Number of records waiting for LLM
- `queue_full_skip`: If >0, increase `ADDRESS_LLM_QUEUE_MAX`

## Backwards Compatibility

✅ Fully backwards compatible:
- Works with existing configs (defaults to 1 LLM worker)
- No changes to input/output formats
- No database migrations needed
- Resumable from checkpoints

## Next Steps (Optional)

1. Monitor initial runs with `ADDRESS_LLM_WORKER_COUNT` matching your Ollama count
2. If queue fills up, increase `ADDRESS_LLM_QUEUE_MAX`
3. Fine-tune timeouts if needed:
   - `ADDRESS_LLM_TIMEOUT`: Per-request LLM timeout
   - `STEP_TIMEOUT_SEC`: Overall step timeout (can reduce now that we don't block)
