#!/usr/bin/env python3
"""etl-address — unified address ETL (country/state/district/mandal).

Replaces legacy:
  - update-mandal/mandal_imputation_from_address.py
  - update-state-country/update-state-country.py

Single pass per record:
  read → normalize → kb_resolve → (llm_resolve if partial) → validate → idempotent write.

Pagination: keyset on (person_id::text, ctid).
Checkpointing: etl_checkpoint table.
Failures: etl_address_failures table (no silent drops).
LLM: Ollama (primary + fallback) via core.llm_service settings.
"""
from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

# Resolve project root on sys.path so we can import repo-wide modules.
HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
for p in (PROJECT_ROOT, HERE):
    if p not in sys.path:
        sys.path.insert(0, p)

from env_utils import load_repo_environment  # noqa: E402
load_repo_environment()

from db_pooling import PostgreSQLConnectionPool, compute_safe_workers  # noqa: E402

from resolver.kb_cache import GeoKB  # noqa: E402
from resolver.kb_resolver import resolve_kb  # noqa: E402
from resolver.llm_resolver import LLMAddressResolver  # noqa: E402
from resolver.normalize import build_candidates  # noqa: E402
from resolver.types import PersonRow, ResolvedAddress  # noqa: E402

from io_layer.reader import count_pending, fetch_batch  # noqa: E402
from io_layer.writer import apply_resolution  # noqa: E402
from io_layer.checkpoint import read_checkpoint, write_checkpoint  # noqa: E402
from io_layer.failures import record_failure  # noqa: E402

from obs.logger import setup_logger, run_id  # noqa: E402
from obs.heartbeat import Heartbeat  # noqa: E402


logger = setup_logger("etl-address")


# --------------------------------------------------------------------
# Config (env-driven)
# --------------------------------------------------------------------

BATCH_SIZE        = int(os.environ.get("ADDRESS_BATCH_SIZE", "500"))
REQ_WORKERS       = int(os.environ.get("ADDRESS_MAX_WORKERS", str(min(32, (os.cpu_count() or 1) * 4))))
POOL_MINCONN      = int(os.environ.get("ADDRESS_POOL_MINCONN", "5"))
POOL_RESERVED     = int(os.environ.get("ADDRESS_POOL_RESERVED", "5"))
HEARTBEAT_SEC     = int(os.environ.get("ADDRESS_HEARTBEAT_SEC", "30"))
CHECKPOINT_EVERY  = int(os.environ.get("ADDRESS_CHECKPOINT_EVERY", "10"))   # batches
FAIL_RATE_ABORT   = float(os.environ.get("ADDRESS_FAIL_RATE_ABORT", "0.25"))
MAX_RETRIES_ROW   = int(os.environ.get("ADDRESS_ROW_RETRIES", "3"))
DRY_RUN           = os.environ.get("ADDRESS_DRY_RUN", "0") == "1"
RESUME            = os.environ.get("ADDRESS_RESUME", "1") == "1"
LIMIT             = int(os.environ.get("ADDRESS_LIMIT", "0")) or None


# --------------------------------------------------------------------
# Stats (lock-guarded)
# --------------------------------------------------------------------

class Stats:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.c = Counter()

    def inc(self, key: str, n: int = 1) -> None:
        with self.lock:
            self.c[key] += n

    def snapshot(self) -> dict:
        with self.lock:
            return dict(self.c)


# --------------------------------------------------------------------
# Per-record resolution
# --------------------------------------------------------------------

def _pick_best(a: Optional[ResolvedAddress], b: Optional[ResolvedAddress]) -> Optional[ResolvedAddress]:
    if a is None:
        return b
    if b is None:
        return a
    # prefer complete; else more fields; else higher confidence
    if a.is_complete and not b.is_complete:
        return a
    if b.is_complete and not a.is_complete:
        return b
    a_count = sum(bool(x) for x in (a.country, a.state, a.district, a.mandal))
    b_count = sum(bool(x) for x in (b.country, b.state, b.district, b.mandal))
    if a_count != b_count:
        return a if a_count > b_count else b
    return a if a.confidence >= b.confidence else b


def resolve_one(pool, row: PersonRow, llm: Optional[LLMAddressResolver]) -> tuple[
    Optional[ResolvedAddress], Optional[ResolvedAddress], str
]:
    """Return (perm_resolved, pres_resolved, path_summary)."""
    perm_cand, pres_cand = build_candidates(row)

    perm_out: Optional[ResolvedAddress] = None
    pres_out: Optional[ResolvedAddress] = None
    paths = []

    if perm_cand.has_any_signal:
        kb_p = resolve_kb(pool, perm_cand)
        if not kb_p.is_complete and llm is not None:
            kb_p = llm.resolve(perm_cand, kb_p)
        perm_out = kb_p
        paths.append("P:" + kb_p.path)

    if pres_cand.has_any_signal:
        kb_r = resolve_kb(pool, pres_cand)
        if not kb_r.is_complete and llm is not None:
            kb_r = llm.resolve(pres_cand, kb_r)
        pres_out = kb_r
        paths.append("R:" + kb_r.path)

    # If permanent has nothing but present has something, mirror present → permanent
    # so the primary slot is always resolved when any data exists. This mirrors legacy
    # Phase-1B intent without re-queuing the record.
    if (perm_out is None or not perm_out.has_any) and pres_out is not None and pres_out.has_any:
        mirror = ResolvedAddress(
            slot="permanent",
            country=pres_out.country,
            state=pres_out.state,
            district=pres_out.district,
            mandal=pres_out.mandal,
            path=pres_out.path + "+mirror",
            confidence=pres_out.confidence,
        )
        perm_out = _pick_best(perm_out, mirror)

    return perm_out, pres_out, "|".join(paths) or "none"


def _is_worth_writing(r: Optional[ResolvedAddress]) -> bool:
    return r is not None and r.has_any


# --------------------------------------------------------------------
# Worker
# --------------------------------------------------------------------

def process_record(
    pool,
    row: PersonRow,
    llm: Optional[LLMAddressResolver],
    stats: Stats,
) -> None:
    last_exc: Optional[BaseException] = None
    for attempt in range(1, MAX_RETRIES_ROW + 1):
        try:
            perm, pres, path = resolve_one(pool, row, llm)

            if not _is_worth_writing(perm) and not _is_worth_writing(pres):
                record_failure(pool, row.person_id, "no_resolution", {"path": path})
                stats.inc("failed_no_resolution")
                return

            if DRY_RUN:
                stats.inc("dry_run_resolved")
                logger.info("[DRY] %s path=%s perm=%s/%s/%s/%s pres=%s/%s/%s/%s",
                            row.person_id, path,
                            getattr(perm, "country", None), getattr(perm, "state", None),
                            getattr(perm, "district", None), getattr(perm, "mandal", None),
                            getattr(pres, "country", None), getattr(pres, "state", None),
                            getattr(pres, "district", None), getattr(pres, "mandal", None))
                return

            wrote, unchanged = apply_resolution(
                pool, row.person_id,
                perm if _is_worth_writing(perm) else None,
                pres if _is_worth_writing(pres) else None,
            )

            if wrote:
                stats.inc("updated")
            else:
                stats.inc("unchanged")

            if "llm" in path:
                stats.inc("llm_used")
            return

        except Exception as exc:
            last_exc = exc
            backoff = min(2 ** (attempt - 1), 5)
            logger.warning("row %s attempt %d/%d failed: %s (sleep %ds)",
                            row.person_id, attempt, MAX_RETRIES_ROW, exc, backoff)
            time.sleep(backoff)

    # all retries exhausted
    try:
        record_failure(pool, row.person_id, "unexpected",
                       {"error": str(last_exc) if last_exc else "unknown"})
    except Exception as exc:
        logger.error("failed to record_failure for %s: %s", row.person_id, exc)
    stats.inc("failed_unexpected")


# --------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------

def _emit_heartbeat(state: dict) -> str:
    st = state["stats"].snapshot()
    return (
        f"processed={state['processed']}/{state['total']} "
        f"updated={st.get('updated',0)} unchanged={st.get('unchanged',0)} "
        f"llm={st.get('llm_used',0)} "
        f"failed={st.get('failed_no_resolution',0)+st.get('failed_unexpected',0)} "
        f"last_seen={state.get('last_seen_id') or '-'}"
    )


def run() -> int:
    rid = run_id()
    logger.info("=" * 80)
    logger.info("etl-address starting run_id=%s dry_run=%s resume=%s", rid, DRY_RUN, RESUME)
    logger.info("=" * 80)

    # pool: reuse master's singleton; do NOT reset here
    pool = PostgreSQLConnectionPool(
        minconn=POOL_MINCONN,
        maxconn=REQ_WORKERS + POOL_RESERVED,
    )

    # build KB once
    t0 = time.time()
    GeoKB.instance().build(pool)
    logger.info("KB build: %.2fs", time.time() - t0)

    # LLM resolver (lazy-loaded singleton)
    llm: Optional[LLMAddressResolver] = None
    if os.environ.get("ADDRESS_DISABLE_LLM", "0") != "1":
        try:
            llm = LLMAddressResolver.instance()
            logger.info("LLM ready: model=%s fallback=%s host=%s budget=%d",
                        llm.model, llm.fallback, llm.host, llm.budget.limit)
        except Exception as exc:
            logger.warning("LLM init failed (continuing KB-only): %s", exc)
            llm = None
    else:
        logger.info("LLM disabled via ADDRESS_DISABLE_LLM=1")

    total = count_pending(pool)
    if LIMIT:
        total = min(total, LIMIT)
    logger.info("Pending: %d (limit=%s)", total, LIMIT or "none")
    if total == 0:
        logger.info("Nothing to process.")
        return 0

    workers = compute_safe_workers(pool, REQ_WORKERS, reserved=POOL_RESERVED)
    logger.info("workers=%d batch_size=%d pool_max=%d", workers, BATCH_SIZE, pool.maxconn)

    last_seen_id: Optional[str] = read_checkpoint(pool) if RESUME else None
    if last_seen_id:
        logger.info("Resuming from checkpoint last_seen_id=%s", last_seen_id)

    processed = 0
    batch_ix = 0
    stats = Stats()
    state = {"processed": 0, "total": total, "stats": stats, "last_seen_id": last_seen_id}

    hb = Heartbeat(HEARTBEAT_SEC, lambda: _emit_heartbeat(state))
    hb.start()
    t_start = time.time()

    try:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="addr") as ex:
            while processed < total:
                take = min(BATCH_SIZE, total - processed)
                batch = fetch_batch(pool, last_seen_id, take)
                if not batch:
                    break

                batch_ix += 1
                logger.info("Batch #%d after_id=%s size=%d", batch_ix, last_seen_id or "<start>", len(batch))

                futures = [ex.submit(process_record, pool, row, llm, stats) for row in batch]
                for f in as_completed(futures):
                    try:
                        f.result()
                    except Exception as exc:
                        logger.error("worker crashed: %s", exc)
                        stats.inc("failed_unexpected")

                processed += len(batch)
                last_seen_id = batch[-1].person_id
                state["processed"] = processed
                state["last_seen_id"] = last_seen_id

                if batch_ix % CHECKPOINT_EVERY == 0:
                    try:
                        write_checkpoint(pool, last_seen_id, rid)
                        logger.info("checkpoint written last_seen_id=%s", last_seen_id)
                    except Exception as exc:
                        logger.warning("checkpoint write failed: %s", exc)

                # abort if fail rate too high
                snap = stats.snapshot()
                failed = snap.get("failed_no_resolution", 0) + snap.get("failed_unexpected", 0)
                if processed > 200 and (failed / max(1, processed)) > FAIL_RATE_ABORT:
                    logger.error("aborting: fail_rate %.2f > %.2f", failed / processed, FAIL_RATE_ABORT)
                    return 2
    finally:
        hb.stop()
        try:
            if last_seen_id:
                write_checkpoint(pool, last_seen_id, rid)
        except Exception:
            pass

    duration = time.time() - t_start
    snap = stats.snapshot()
    logger.info("=" * 80)
    logger.info("etl-address complete in %.2fs", duration)
    logger.info("  processed             : %d", processed)
    logger.info("  updated               : %d", snap.get("updated", 0))
    logger.info("  unchanged (idempotent): %d", snap.get("unchanged", 0))
    logger.info("  llm_used              : %d", snap.get("llm_used", 0))
    logger.info("  dry_run_resolved      : %d", snap.get("dry_run_resolved", 0))
    logger.info("  failed_no_resolution  : %d", snap.get("failed_no_resolution", 0))
    logger.info("  failed_unexpected     : %d", snap.get("failed_unexpected", 0))
    logger.info("=" * 80)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="etl-address: unified address ETL")
    parser.add_argument("--dry-run", action="store_true", help="Resolve but do not write")
    parser.add_argument("--no-resume", action="store_true", help="Ignore existing checkpoint")
    parser.add_argument("--limit", type=int, default=None, help="Max records this run")
    parser.add_argument("--disable-llm", action="store_true", help="KB-only mode")
    args = parser.parse_args()

    if args.dry_run:
        os.environ["ADDRESS_DRY_RUN"] = "1"
    if args.no_resume:
        os.environ["ADDRESS_RESUME"] = "0"
    if args.limit is not None:
        os.environ["ADDRESS_LIMIT"] = str(args.limit)
    if args.disable_llm:
        os.environ["ADDRESS_DISABLE_LLM"] = "1"

    # reload module-level constants after env override
    global DRY_RUN, RESUME, LIMIT
    DRY_RUN = os.environ.get("ADDRESS_DRY_RUN", "0") == "1"
    RESUME = os.environ.get("ADDRESS_RESUME", "1") == "1"
    LIMIT = int(os.environ.get("ADDRESS_LIMIT", "0")) or None

    rc = run()
    sys.exit(rc)


if __name__ == "__main__":
    main()
