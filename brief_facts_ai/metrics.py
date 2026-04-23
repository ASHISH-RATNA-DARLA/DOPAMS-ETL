import logging
import time
from dataclasses import dataclass, field
from typing import List, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class BatchMetrics:
    """Metrics for a single batch of crimes."""
    batch_number: int
    batch_size: int
    start_time: float
    end_time: float = 0.0
    crimes_processed: int = 0
    crimes_succeeded: int = 0
    crimes_failed: int = 0
    commits_executed: int = 0
    savepoints_created: int = 0

    @property
    def duration_seconds(self) -> float:
        """Total time to process batch."""
        return max(0, self.end_time - self.start_time) if self.end_time > 0 else 0

    @property
    def throughput_per_hour(self) -> float:
        """Estimated crimes per hour based on batch processing rate."""
        if self.duration_seconds > 0:
            return (self.crimes_processed / self.duration_seconds) * 3600
        return 0

    @property
    def success_rate(self) -> float:
        """Success rate as percentage."""
        if self.crimes_processed > 0:
            return (self.crimes_succeeded / self.crimes_processed) * 100
        return 0

    def complete(self, crimes_succeeded: int, crimes_failed: int, commits: int):
        """Mark batch as complete with results."""
        self.end_time = time.time()
        self.crimes_processed = crimes_succeeded + crimes_failed
        self.crimes_succeeded = crimes_succeeded
        self.crimes_failed = crimes_failed
        self.commits_executed = commits

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging/storage."""
        return {
            'batch_number': self.batch_number,
            'batch_size': self.batch_size,
            'duration_seconds': self.duration_seconds,
            'crimes_processed': self.crimes_processed,
            'crimes_succeeded': self.crimes_succeeded,
            'crimes_failed': self.crimes_failed,
            'success_rate_percent': self.success_rate,
            'throughput_per_hour': self.throughput_per_hour,
            'commits_executed': self.commits_executed,
            'savepoints_created': self.savepoints_created,
        }


@dataclass
class MetricsCollector:
    """Collects and reports performance metrics across batches."""
    batch_metrics: List[BatchMetrics] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    end_time: float = 0.0

    def start_batch(self, batch_number: int, batch_size: int) -> BatchMetrics:
        """Start tracking a new batch."""
        batch = BatchMetrics(
            batch_number=batch_number,
            batch_size=batch_size,
            start_time=time.time()
        )
        self.batch_metrics.append(batch)
        logger.info(f"Starting batch {batch_number} (size={batch_size})")
        return batch

    def complete_batch(
        self,
        batch: BatchMetrics,
        crimes_succeeded: int,
        crimes_failed: int,
        commits: int
    ):
        """Mark a batch as complete."""
        batch.complete(crimes_succeeded, crimes_failed, commits)
        logger.info(
            f"Batch {batch.batch_number} completed: "
            f"processed={batch.crimes_processed}, success={batch.crimes_succeeded}, "
            f"failed={batch.crimes_failed}, commits={commits}, "
            f"duration={batch.duration_seconds:.1f}s, throughput={batch.throughput_per_hour:.1f}/hr"
        )

    def finalize(self):
        """Mark collection as complete."""
        self.end_time = time.time()

    @property
    def total_duration_seconds(self) -> float:
        """Total runtime from first batch start to completion."""
        if self.end_time > 0:
            return self.end_time - self.start_time
        if self.batch_metrics:
            return time.time() - self.batch_metrics[0].start_time
        return 0

    @property
    def total_crimes_processed(self) -> int:
        """Total crimes across all batches."""
        return sum(b.crimes_processed for b in self.batch_metrics)

    @property
    def total_crimes_succeeded(self) -> int:
        """Total successful crimes."""
        return sum(b.crimes_succeeded for b in self.batch_metrics)

    @property
    def total_crimes_failed(self) -> int:
        """Total failed crimes."""
        return sum(b.crimes_failed for b in self.batch_metrics)

    @property
    def overall_success_rate(self) -> float:
        """Overall success rate as percentage."""
        if self.total_crimes_processed > 0:
            return (self.total_crimes_succeeded / self.total_crimes_processed) * 100
        return 0

    @property
    def average_throughput_per_hour(self) -> float:
        """Average throughput across all batches."""
        if self.total_duration_seconds > 0:
            return (self.total_crimes_processed / self.total_duration_seconds) * 3600
        return 0

    @property
    def total_commits(self) -> int:
        """Total commit operations."""
        return sum(b.commits_executed for b in self.batch_metrics)

    def get_summary_report(self) -> str:
        """Generate human-readable summary report."""
        report = [
            "\n" + "=" * 80,
            "ETL EXECUTION METRICS SUMMARY",
            "=" * 80,
            f"Execution timestamp: {datetime.now().isoformat()}",
            "",
            "BATCH STATISTICS:",
            f"  Total batches: {len(self.batch_metrics)}",
            f"  Total crimes processed: {self.total_crimes_processed}",
            f"  Total crimes succeeded: {self.total_crimes_succeeded}",
            f"  Total crimes failed: {self.total_crimes_failed}",
            f"  Overall success rate: {self.overall_success_rate:.1f}%",
            "",
            "PERFORMANCE:",
            f"  Total execution time: {self.total_duration_seconds:.1f} seconds ({self.total_duration_seconds/3600:.2f} hours)",
            f"  Average throughput: {self.average_throughput_per_hour:.1f} crimes/hour",
            f"  Total database commits: {self.total_commits}",
            f"  Average commit frequency: 1 commit per {max(1, self.total_crimes_processed // max(1, self.total_commits))} crimes",
            "",
            "EXPECTED IMPROVEMENTS:",
            f"  Commit overhead reduction: ~50% (batch commits vs per-crime)",
            f"  Pool instantiation savings: ~5-10% (singleton reference)",
            f"  Batch fetch latency reduction: ~10-15% (larger batch size)",
            "",
        ]

        if self.batch_metrics:
            report.append("BATCH-BY-BATCH BREAKDOWN:")
            for batch in self.batch_metrics:
                report.append(f"  Batch {batch.batch_number}:")
                report.append(f"    - Size: {batch.batch_size} crimes")
                report.append(f"    - Duration: {batch.duration_seconds:.1f}s")
                report.append(f"    - Processed: {batch.crimes_processed} ({batch.crimes_succeeded} success, {batch.crimes_failed} failed)")
                report.append(f"    - Throughput: {batch.throughput_per_hour:.1f} crimes/hour")
                report.append(f"    - Commits: {batch.commits_executed}")
                report.append("")

        report.extend([
            "=" * 80,
        ])

        return "\n".join(report)

    def to_dict(self) -> Dict[str, Any]:
        """Convert all metrics to dictionary for JSON export."""
        return {
            'execution_timestamp': datetime.now().isoformat(),
            'total_duration_seconds': self.total_duration_seconds,
            'total_batches': len(self.batch_metrics),
            'total_crimes_processed': self.total_crimes_processed,
            'total_crimes_succeeded': self.total_crimes_succeeded,
            'total_crimes_failed': self.total_crimes_failed,
            'overall_success_rate_percent': self.overall_success_rate,
            'average_throughput_per_hour': self.average_throughput_per_hour,
            'total_commits': self.total_commits,
            'batches': [b.to_dict() for b in self.batch_metrics],
        }
