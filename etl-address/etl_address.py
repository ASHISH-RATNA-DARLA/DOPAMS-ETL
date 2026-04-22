#!/usr/bin/env python3
"""etl-address — unified address ETL (country/state/district/mandal).

Replaces legacy:
  - update-mandal/mandal_imputation_from_address.py
  - update-state-country/update-state-country.py

Single pass per record:
  read → normalize → kb_resolve → (llm_resolve if partial) → validate → idempotent write.

Pagination: stable keyset on person_id::text.
Checkpointing: crash-only etl_checkpoint row, cleared after a clean run.
Failures: etl_address_failures table (no silent drops).
LLM: Ollama (primary + fallback) via core.llm_service settings.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import time
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Deque

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
from resolver.types import PersonRow, ResolvedAddress, AddressCandidate  # noqa: E402

from io_layer.reader import count_pending, fetch_batch  # noqa: E402
from io_layer.writer import apply_resolution  # noqa: E402
from io_layer.checkpoint import clear_checkpoint, read_checkpoint, write_checkpoint  # noqa: E402
from io_layer.failures import clear_stale_failures, record_failure  # noqa: E402

from obs.logger import setup_logger, run_id  # noqa: E402
from obs.heartbeat import Heartbeat  # noqa: E402


logger = setup_logger("etl-address")
DETAIL_LOGGING = logger.isEnabledFor(logging.DEBUG)


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
FAILURE_STALE_DAYS = int(os.environ.get("ADDRESS_FAILURE_STALE_DAYS", "30"))
RESET_CHECKPOINT  = os.environ.get("ADDRESS_RESET_CHECKPOINT", "0") == "1"
LLM_WORKER_COUNT  = int(os.environ.get("ADDRESS_LLM_WORKER_COUNT", "1"))
LLM_QUEUE_MAX     = int(os.environ.get("ADDRESS_LLM_QUEUE_MAX", "100"))


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


class LLMQueueManager:
    """Manages queueing for LLM-needing records.

    Only allows LLM_WORKER_COUNT concurrent LLM calls.
    Other workers skip records needing LLM and process KB-only records instead.
    """
    def __init__(self, worker_count: int) -> None:
        self.sem = threading.Semaphore(max(1, worker_count))
        self.queue: Deque = deque()
        self.queue_lock = threading.Lock()

    def can_acquire(self) -> bool:
        return self.sem.acquire(blocking=False)

    def release(self) -> None:
        self.sem.release()

    def queue_record(self, row) -> bool:
        with self.queue_lock:
            if len(self.queue) < LLM_QUEUE_MAX:
                self.queue.append(row)
                return True
        return False

    def dequeue_record(self):
        with self.queue_lock:
            if self.queue:
                return self.queue.popleft()
        return None

    def queue_size(self) -> int:
        with self.queue_lock:
            return len(self.queue)


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


def _mirror_resolved(source: ResolvedAddress, slot: str) -> ResolvedAddress:
    return ResolvedAddress(
        slot=slot,
        country=source.country,
        state=source.state,
        district=source.district,
        mandal=source.mandal,
        path=source.path + "+mirror",
        confidence=source.confidence,
    )


def _country_from_nationality(nationality: Optional[str]) -> Optional[str]:
    if not nationality:
        return None
    token = nationality.strip().lower()
    known = {
        "indian": "India",
        "india": "India",
        "hindu": "India",
        "pakistani": "Pakistan",
        "nepali": "Nepal",
        "nepalese": "Nepal",
        "bangladeshi": "Bangladesh",
        "sri lankan": "Sri Lanka",
        "muslim": None,
    }
    return known.get(token)


def _propagate_country(
    row: PersonRow,
    perm_out: Optional[ResolvedAddress],
    pres_out: Optional[ResolvedAddress],
) -> tuple[Optional[ResolvedAddress], Optional[ResolvedAddress], bool, bool, list[str]]:
    # Prefer resolved country from either slot, then raw country fields, then nationality fallback.
    kb = GeoKB.instance()
    nat_country = None
    if row.nationality:
        nat_country = kb.canon_country(row.nationality) or _country_from_nationality(row.nationality)
    country = (
        (perm_out.country if perm_out else None)
        or (pres_out.country if pres_out else None)
        or row.perm_country
        or row.pres_country
        or nat_country
    )
    if not country:
        return perm_out, pres_out, False, False, []

    changed = False
    used_nat = False
    mode = "cross"
    if not ((perm_out and perm_out.country) or (pres_out and pres_out.country)):
        if row.perm_country or row.pres_country:
            mode = "raw"
        elif nat_country:
            mode = "nat"
            used_nat = True

    if not row.perm_country and perm_out is None:
        perm_out = ResolvedAddress(slot="permanent", country=country, path="country-propagation", confidence=0.5)
        changed = True
    elif perm_out is not None and not perm_out.country and not row.perm_country:
        perm_out.country = country
        perm_out.path = (perm_out.path + "+country-propagation") if perm_out.path else "country-propagation"
        changed = True

    if not row.pres_country and pres_out is None:
        pres_out = ResolvedAddress(slot="present", country=country, path="country-propagation", confidence=0.5)
        changed = True
    elif pres_out is not None and not pres_out.country and not row.pres_country:
        pres_out.country = country
        pres_out.path = (pres_out.path + "+country-propagation") if pres_out.path else "country-propagation"
        changed = True

    markers = []
    if changed:
        if mode == "nat":
            markers.append("M:country_nat")
        elif mode == "raw":
            markers.append("M:country_raw")
        else:
            markers.append("M:country_cross")

    return perm_out, pres_out, changed, used_nat, markers


def resolve_one(pool, row: PersonRow, llm: Optional[LLMAddressResolver]) -> tuple[
    Optional[ResolvedAddress], Optional[ResolvedAddress], str
]:
    """Return (perm_resolved, pres_resolved, path_summary)."""
    perm_cand, pres_cand = build_candidates(row)

    if DETAIL_LOGGING:
        logger.debug(
            "row %s candidates perm={%s} pres={%s}",
            row.person_id,
            _candidate_summary(perm_cand),
            _candidate_summary(pres_cand),
        )

    perm_out: Optional[ResolvedAddress] = None
    pres_out: Optional[ResolvedAddress] = None
    paths = []

    if perm_cand.has_any_signal:
        if perm_cand.enriched_locality_hit:
            paths.append("P:enriched-locality")
        kb_p = resolve_kb(pool, perm_cand)
        if DETAIL_LOGGING:
            logger.debug("row %s permanent kb={%s}", row.person_id, _resolved_summary(kb_p))
        if not kb_p.is_complete and llm is not None:
            if _can_use_llm(perm_cand):
                if DETAIL_LOGGING:
                    logger.debug("row %s permanent invoking LLM", row.person_id)
                kb_p = llm.resolve(perm_cand, kb_p)
                if DETAIL_LOGGING:
                    logger.debug("row %s permanent llm={%s}", row.person_id, _resolved_summary(kb_p))
            else:
                paths.append("P:llm-skip-no-signal")
        perm_out = kb_p
        paths.append("P:" + kb_p.path)

    if pres_cand.has_any_signal:
        if pres_cand.enriched_locality_hit:
            paths.append("R:enriched-locality")
        kb_r = resolve_kb(pool, pres_cand)
        if DETAIL_LOGGING:
            logger.debug("row %s present kb={%s}", row.person_id, _resolved_summary(kb_r))
        if not kb_r.is_complete and llm is not None:
            if _can_use_llm(pres_cand):
                if DETAIL_LOGGING:
                    logger.debug("row %s present invoking LLM", row.person_id)
                kb_r = llm.resolve(pres_cand, kb_r)
                if DETAIL_LOGGING:
                    logger.debug("row %s present llm={%s}", row.person_id, _resolved_summary(kb_r))
            else:
                paths.append("R:llm-skip-no-signal")
        pres_out = kb_r
        paths.append("R:" + kb_r.path)

    # Keep both address slots in sync when one side has usable geo data but the
    # other side is still incomplete. apply_resolution() only fills NULL fields.
    if pres_out is not None and pres_out.has_any and (perm_out is None or not perm_out.is_complete):
        perm_out = _pick_best(perm_out, _mirror_resolved(pres_out, "permanent"))
        paths.append("M:P<-R")

    if perm_out is not None and perm_out.has_any and (pres_out is None or not pres_out.is_complete):
        pres_out = _pick_best(pres_out, _mirror_resolved(perm_out, "present"))
        paths.append("M:R<-P")

    perm_out, pres_out, country_propagated, used_nat, country_markers = _propagate_country(row, perm_out, pres_out)
    if country_propagated:
        paths.append("C:propagated")
    if used_nat:
        paths.append("C:from-nat")
    paths.extend(country_markers)

    if DETAIL_LOGGING:
        logger.debug(
            "row %s resolved path=%s perm={%s} pres={%s}",
            row.person_id,
            "|".join(paths) or "none",
            _resolved_summary(perm_out),
            _resolved_summary(pres_out),
        )

    return perm_out, pres_out, "|".join(paths) or "none"


def _is_worth_writing(r: Optional[ResolvedAddress]) -> bool:
    return r is not None and r.has_any


def _is_partial_resolution(r: Optional[ResolvedAddress]) -> bool:
    return r is not None and r.has_any and not r.is_complete


def _summary_value(value: Optional[str]) -> str:
    return value if value else "-"


def _candidate_summary(cand) -> str:
    return (
        f"slot={cand.slot} "
        f"raw[state={_summary_value(cand.raw_state)},district={_summary_value(cand.raw_district)},"
        f"mandal={_summary_value(cand.raw_mandal)},country={_summary_value(cand.raw_country)}] "
        f"norm[state={_summary_value(cand.state)},district={_summary_value(cand.district)},"
        f"mandal={_summary_value(cand.mandal)},country={_summary_value(cand.country)}] "
        f"locality={_summary_value(cand.locality)} pin={_summary_value(cand.pin)} "
        f"signal={cand.has_any_signal} enriched_locality={cand.enriched_locality_hit}"
    )


def _resolved_summary(resolved: Optional[ResolvedAddress]) -> str:
    if resolved is None:
        return "none"
    return (
        f"slot={resolved.slot} country={_summary_value(resolved.country)} "
        f"state={_summary_value(resolved.state)} district={_summary_value(resolved.district)} "
        f"mandal={_summary_value(resolved.mandal)} path={resolved.path or '-'} "
        f"confidence={resolved.confidence:.2f} notes={resolved.notes or '-'}"
    )


def _can_use_llm(cand) -> bool:
    return bool(
        cand.district or cand.mandal or cand.locality or cand.landmark or
        cand.ward or cand.street or cand.pin
    )


def _will_need_llm(pool, row: PersonRow) -> bool:
    """Quick check: will this record likely need LLM based on KB resolution.

    Returns True if KB resolution is incomplete and LLM might help.
    """
    perm_cand, pres_cand = build_candidates(row)

    if perm_cand.has_any_signal:
        kb_p = resolve_kb(pool, perm_cand)
        if not kb_p.is_complete and _can_use_llm(perm_cand):
            return True

    if pres_cand.has_any_signal:
        kb_r = resolve_kb(pool, pres_cand)
        if not kb_r.is_complete and _can_use_llm(pres_cand):
            return True

    return False


def _classify_failure_reason(perm_cand, pres_cand, perm_out, pres_out) -> str:
    has_actionable_signal = bool(
        perm_cand.state or perm_cand.district or perm_cand.mandal or
        perm_cand.locality or perm_cand.landmark or perm_cand.ward or perm_cand.street or
        pres_cand.state or pres_cand.district or pres_cand.mandal or
        pres_cand.locality or pres_cand.landmark or pres_cand.ward or pres_cand.street
    )

    has_state = bool((perm_out and perm_out.state) or (pres_out and pres_out.state))
    has_district = bool((perm_out and perm_out.district) or (pres_out and pres_out.district))
    has_mandal = bool((perm_out and perm_out.mandal) or (pres_out and pres_out.mandal))

    if not has_actionable_signal:
        return "insufficient_geo_signal"
    if has_state and has_district and not has_mandal:
        return "partial_no_mandal_signal"
    if has_state and not has_district:
        return "state_only_no_locality"
    return "kb_and_llm_rejected"


def _inc_failure_metric(stats: Stats, reason: str) -> None:
    key_map = {
        "partial_no_mandal_signal": "failed_partial_no_mandal",
        "insufficient_geo_signal": "failed_insufficient_geo",
        "state_only_no_locality": "failed_state_only_no_locality",
        "kb_and_llm_rejected": "failed_kb_llm_rejected",
    }
    stats.inc(key_map.get(reason, "failed_no_resolution"))


def _record_classified_failure(
    pool,
    row: PersonRow,
    stats: Stats,
    perm: Optional[ResolvedAddress],
    pres: Optional[ResolvedAddress],
    path: str,
    extra_details: Optional[dict] = None,
) -> None:
    perm_cand, pres_cand = build_candidates(row)
    reason = _classify_failure_reason(perm_cand, pres_cand, perm, pres)
    details = {"path": path}
    if extra_details:
        details.update(extra_details)
    record_failure(pool, row.person_id, reason, details)
    _inc_failure_metric(stats, reason)


# --------------------------------------------------------------------
# Worker
# --------------------------------------------------------------------

def process_record(
    pool,
    row: PersonRow,
    llm: Optional[LLMAddressResolver],
    stats: Stats,
    llm_queue: Optional[LLMQueueManager] = None,
) -> bool:
    # Smart routing: if LLM might be needed but no capacity, queue it
    if llm_queue and llm:
        try:
            if _will_need_llm(pool, row):
                if not llm_queue.can_acquire():
                    if llm_queue.queue_record(row):
                        stats.inc("llm_queued_for_later")
                        return True
                    else:
                        stats.inc("llm_queue_full_skipped")
                        return True
                else:
                    llm_queue.release()
        except Exception:
            pass

    last_exc: Optional[BaseException] = None
    has_llm_capacity = False
    if llm_queue and llm:
        has_llm_capacity = llm_queue.can_acquire()

    try:
        for attempt in range(1, MAX_RETRIES_ROW + 1):
            try:
                if DETAIL_LOGGING:
                    logger.debug("row %s attempt=%d/%d start", row.person_id, attempt, MAX_RETRIES_ROW)
                perm, pres, path = resolve_one(pool, row, llm)

                if "llm-skip-no-signal" in path:
                    stats.inc("llm_skipped_no_signal")
                if "enriched-locality" in path:
                    stats.inc("enriched_locality")
                if "+village" in path:
                    stats.inc("enriched_village")
                if "C:propagated" in path:
                    stats.inc("country_propagated")
                if "C:from-nat" in path:
                    stats.inc("country_from_nat")

                if not _is_worth_writing(perm) and not _is_worth_writing(pres):
                    if DETAIL_LOGGING:
                        logger.debug(
                            "row %s classified as failure path=%s perm={%s} pres={%s}",
                            row.person_id,
                            path,
                            _resolved_summary(perm),
                            _resolved_summary(pres),
                        )
                    _record_classified_failure(pool, row, stats, perm, pres, path)
                    return True

                if DRY_RUN:
                    stats.inc("dry_run_resolved")
                    logger.info("[DRY] %s path=%s perm=%s/%s/%s/%s pres=%s/%s/%s/%s",
                                row.person_id, path,
                                getattr(perm, "country", None), getattr(perm, "state", None),
                                getattr(perm, "district", None), getattr(perm, "mandal", None),
                                getattr(pres, "country", None), getattr(pres, "state", None),
                                getattr(pres, "district", None), getattr(pres, "mandal", None))
                    return True

                if DETAIL_LOGGING:
                    logger.debug(
                        "row %s writing perm={%s} pres={%s} path=%s",
                        row.person_id,
                        _resolved_summary(perm),
                        _resolved_summary(pres),
                        path,
                    )
                wrote, unchanged = apply_resolution(
                    pool, row.person_id,
                    perm if _is_worth_writing(perm) else None,
                    pres if _is_worth_writing(pres) else None,
                )

                if wrote:
                    stats.inc("updated")
                else:
                    stats.inc("unchanged")
                    if _is_partial_resolution(perm) or _is_partial_resolution(pres):
                        _record_classified_failure(
                            pool,
                            row,
                            stats,
                            perm,
                            pres,
                            path,
                            {"partial": True, "unchanged": True},
                        )
                        return True

                if DETAIL_LOGGING:
                    logger.debug(
                        "row %s write_result wrote=%s unchanged=%s path=%s",
                        row.person_id,
                        wrote,
                        unchanged,
                        path,
                    )

                if "llm" in path:
                    stats.inc("llm_used")
                return True

            except Exception as exc:
                last_exc = exc
                backoff = min(2 ** (attempt - 1), 5)
                logger.warning("row %s attempt %d/%d failed: %s (sleep %ds)",
                                row.person_id, attempt, MAX_RETRIES_ROW, exc, backoff)
                if DETAIL_LOGGING:
                    logger.debug(
                        "row %s retry_context perm=%s pres=%s",
                        row.person_id,
                        _summary_value(row.perm_state),
                        _summary_value(row.pres_state),
                    )
                time.sleep(backoff)

        # all retries exhausted
        try:
            record_failure(pool, row.person_id, "unexpected",
                           {"error": str(last_exc) if last_exc else "unknown"})
        except Exception as exc:
            logger.error("failed to record_failure for %s: %s", row.person_id, exc)
        stats.inc("failed_unexpected")
        return True
    finally:
        if has_llm_capacity and llm_queue:
            llm_queue.release()


# --------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------

def _emit_heartbeat(state: dict) -> str:
    st = state["stats"].snapshot()
    failed_total = (
        st.get("failed_no_resolution", 0)
        + st.get("failed_unexpected", 0)
        + st.get("failed_partial_no_mandal", 0)
        + st.get("failed_insufficient_geo", 0)
        + st.get("failed_state_only_no_locality", 0)
        + st.get("failed_kb_llm_rejected", 0)
    )
    return (
        f"processed={state['processed']}/{state['total']} "
        f"updated={st.get('updated',0)} unchanged={st.get('unchanged',0)} "
        f"llm={st.get('llm_used',0)} llm_skip={st.get('llm_skipped_no_signal',0)} "
        f"enriched_locality={st.get('enriched_locality',0)} "
        f"enriched_village={st.get('enriched_village',0)} "
        f"country_propagated={st.get('country_propagated',0)} "
        f"country_from_nat={st.get('country_from_nat',0)} "
        f"failed={failed_total} "
        f"last_seen={state.get('last_seen_id') or '-'}"
    )


def run() -> int:
    rid = run_id()
    logger.info("=" * 80)
    logger.info("etl-address starting run_id=%s dry_run=%s resume=%s", rid, DRY_RUN, RESUME)
    logger.info("logging level=%s detailed=%s", logging.getLevelName(logger.level), DETAIL_LOGGING)
    logger.info("=" * 80)

    # pool: reuse master's singleton; do NOT reset here
    pool = PostgreSQLConnectionPool(
        minconn=POOL_MINCONN,
        maxconn=REQ_WORKERS + POOL_RESERVED,
    )

    if RESET_CHECKPOINT:
        clear_checkpoint(pool)
        logger.info("checkpoint cleared via ADDRESS_RESET_CHECKPOINT/--reset-checkpoint")

    cleared_failures = clear_stale_failures(pool, FAILURE_STALE_DAYS)
    if cleared_failures:
        logger.info(
            "cleared %d stale failure rows older than %d days",
            cleared_failures,
            FAILURE_STALE_DAYS,
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
        clear_checkpoint(pool)
        logger.info("Nothing to process.")
        return 0

    workers = compute_safe_workers(pool, REQ_WORKERS, reserved=POOL_RESERVED)
    logger.info("workers=%d batch_size=%d pool_max=%d llm_workers=%d", workers, BATCH_SIZE, pool.maxconn, LLM_WORKER_COUNT)

    last_seen_id: Optional[str] = read_checkpoint(pool) if RESUME else None
    if last_seen_id:
        logger.info("Resuming from checkpoint last_seen_id=%s", last_seen_id)

    processed = 0
    batch_ix = 0
    stats = Stats()
    state = {"processed": 0, "total": total, "stats": stats, "last_seen_id": last_seen_id}

    llm_queue = LLMQueueManager(LLM_WORKER_COUNT) if llm else None
    if llm_queue:
        logger.info("LLM queue initialized: worker_count=%d queue_max=%d", LLM_WORKER_COUNT, LLM_QUEUE_MAX)

    hb = Heartbeat(HEARTBEAT_SEC, lambda: _emit_heartbeat(state))
    hb.start()
    t_start = time.time()
    completed_successfully = False

    try:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="addr") as ex:
            while processed < total:
                take = min(BATCH_SIZE, total - processed)
                batch = fetch_batch(pool, last_seen_id, take)
                if not batch:
                    if processed < total:
                        logger.info(
                            "batch scan exhausted before the initial pending count; "
                            "a clean completion will clear the checkpoint so the next run sweeps from the start"
                        )
                    break

                batch_ix += 1
                logger.info("Batch #%d after_id=%s size=%d", batch_ix, last_seen_id or "<start>", len(batch))

                futures = [ex.submit(process_record, pool, row, llm, stats, llm_queue) for row in batch]
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
                failed = (
                    snap.get("failed_no_resolution", 0)
                    + snap.get("failed_unexpected", 0)
                    + snap.get("failed_partial_no_mandal", 0)
                    + snap.get("failed_insufficient_geo", 0)
                    + snap.get("failed_state_only_no_locality", 0)
                    + snap.get("failed_kb_llm_rejected", 0)
                )
                if processed > 200 and (failed / max(1, processed)) > FAIL_RATE_ABORT:
                    logger.error("aborting: fail_rate %.2f > %.2f", failed / processed, FAIL_RATE_ABORT)
                    return 2

            # Process queued LLM records with dedicated workers
            if llm_queue:
                queued_count = llm_queue.queue_size()
                if queued_count > 0:
                    logger.info("Processing %d queued LLM records", queued_count)
                    while True:
                        row = llm_queue.dequeue_record()
                        if not row:
                            break
                        try:
                            process_record(pool, row, llm, stats, llm_queue)
                        except Exception as exc:
                            logger.error("queued record processing failed: %s", exc)
                            stats.inc("failed_unexpected")

        completed_successfully = True
    finally:
        hb.stop()
        try:
            if completed_successfully:
                clear_checkpoint(pool)
            elif last_seen_id:
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
    logger.info("  llm_skipped_no_signal : %d", snap.get("llm_skipped_no_signal", 0))
    logger.info("  enriched_locality     : %d", snap.get("enriched_locality", 0))
    logger.info("  enriched_village      : %d", snap.get("enriched_village", 0))
    logger.info("  enriched_by_locality  : %d", snap.get("enriched_locality", 0))
    logger.info("  enriched_by_village   : %d", snap.get("enriched_village", 0))
    logger.info("  country_propagated    : %d", snap.get("country_propagated", 0))
    logger.info("  country_from_nat      : %d", snap.get("country_from_nat", 0))
    logger.info("  dry_run_resolved      : %d", snap.get("dry_run_resolved", 0))
    logger.info("  failed_no_resolution  : %d", snap.get("failed_no_resolution", 0))
    logger.info("  failed_partial_mandal : %d", snap.get("failed_partial_no_mandal", 0))
    logger.info("  failed_insufficient   : %d", snap.get("failed_insufficient_geo", 0))
    logger.info("  failure_partial       : %d", snap.get("failed_partial_no_mandal", 0))
    logger.info("  failure_insufficient  : %d", snap.get("failed_insufficient_geo", 0))
    logger.info("  failed_state_only     : %d", snap.get("failed_state_only_no_locality", 0))
    logger.info("  failed_kb_llm_reject  : %d", snap.get("failed_kb_llm_rejected", 0))
    logger.info("  failed_unexpected     : %d", snap.get("failed_unexpected", 0))
    logger.info("  llm_queued_for_later  : %d", snap.get("llm_queued_for_later", 0))
    logger.info("  llm_queue_full_skip   : %d", snap.get("llm_queue_full_skipped", 0))
    logger.info("=" * 80)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="etl-address: unified address ETL")
    parser.add_argument("--dry-run", action="store_true", help="Resolve but do not write")
    parser.add_argument("--no-resume", action="store_true", help="Ignore existing checkpoint")
    parser.add_argument("--reset-checkpoint", action="store_true", help="Delete the saved checkpoint before starting")
    parser.add_argument("--limit", type=int, default=None, help="Max records this run")
    parser.add_argument("--disable-llm", action="store_true", help="KB-only mode")
    args = parser.parse_args()

    if args.dry_run:
        os.environ["ADDRESS_DRY_RUN"] = "1"
    if args.no_resume:
        os.environ["ADDRESS_RESUME"] = "0"
    if args.reset_checkpoint:
        os.environ["ADDRESS_RESET_CHECKPOINT"] = "1"
    if args.limit is not None:
        os.environ["ADDRESS_LIMIT"] = str(args.limit)
    if args.disable_llm:
        os.environ["ADDRESS_DISABLE_LLM"] = "1"

    # reload module-level constants after env override
    global DRY_RUN, RESUME, LIMIT, RESET_CHECKPOINT
    DRY_RUN = os.environ.get("ADDRESS_DRY_RUN", "0") == "1"
    RESUME = os.environ.get("ADDRESS_RESUME", "1") == "1"
    LIMIT = int(os.environ.get("ADDRESS_LIMIT", "0")) or None
    RESET_CHECKPOINT = os.environ.get("ADDRESS_RESET_CHECKPOINT", "0") == "1"

    rc = run()
    sys.exit(rc)


if __name__ == "__main__":
    main()
