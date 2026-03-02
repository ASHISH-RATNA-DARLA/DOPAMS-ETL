#!/usr/bin/env python3
"""
DOPAMS ETL Performance Profiling Toolkit
========================================

Quick-start profiling setup for identifying bottlenecks.
Run this alongside your ETL pipeline to collect metrics.

Usage:
    python performance_profiler.py --pipeline brief_facts_accused --crimes 100
    python performance_profiler.py --analyze-only
"""

import time
import logging
import functools
import psutil
import os
import sys
import json
from datetime import datetime
from typing import Dict, List, Optional
from contextlib import contextmanager
from dataclasses import dataclass, asdict
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# METRICS COLLECTORS
# ============================================================================

@dataclass
class FunctionMetric:
    """Single function execution metric"""
    name: str
    module: str
    duration_ms: float
    timestamp: float
    
    def __post_init__(self):
        if self.duration_ms > 100:
            self.severity = "SLOW"
        elif self.duration_ms > 50:
            self.severity = "MEDIUM"
        else:
            self.severity = "FAST"


@dataclass
class QueryMetric:
    """Database query execution metric"""
    query_prefix: str
    duration_ms: float
    rows_affected: int
    timestamp: float
    connection_time_ms: Optional[float] = None
    

@dataclass
class MemoryMetric:
    """Memory usage at point in time"""
    label: str
    rss_mb: float  # Resident Set Size
    vms_mb: float  # Virtual Memory
    timestamp: float
    available_mb: Optional[float] = None


class PerformanceCollector:
    """Central metrics collection point"""
    
    def __init__(self):
        self.function_calls: List[FunctionMetric] = []
        self.queries: List[QueryMetric] = []
        self.memory_snapshots: List[MemoryMetric] = []
        self.process = psutil.Process(os.getpid())
        self.start_time = time.time()
        
    def record_function(self, name: str, module: str, duration_ms: float):
        """Record function execution time"""
        self.function_calls.append(
            FunctionMetric(name, module, duration_ms, time.time())
        )
    
    def record_query(self, query_prefix: str, duration_ms: float, 
                    rows_affected: int = 0, conn_time_ms: Optional[float] = None):
        """Record database query"""
        self.queries.append(
            QueryMetric(query_prefix, duration_ms, rows_affected, time.time(), conn_time_ms)
        )
    
    def record_memory(self, label: str):
        """Record current memory usage"""
        mem_info = self.process.memory_info()
        mem_percent = self.process.memory_percent()
        available = psutil.virtual_memory().available / 1024 / 1024
        
        self.memory_snapshots.append(
            MemoryMetric(
                label,
                mem_info.rss / 1024 / 1024,
                mem_info.vms / 1024 / 1024,
                time.time(),
                available
            )
        )
    
    def report_text(self) -> str:
        """Generate human-readable report"""
        report = []
        report.append("\n" + "="*80)
        report.append("PERFORMANCE PROFILING REPORT".center(80))
        report.append("="*80)
        
        elapsed = time.time() - self.start_time
        report.append(f"\nTotal profiling duration: {elapsed:.1f}s\n")
        
        # Function stats
        if self.function_calls:
            report.append("TOP 20 SLOWEST FUNCTIONS")
            report.append("-"*80)
            
            sorted_funcs = sorted(
                self.function_calls,
                key=lambda x: x.duration_ms,
                reverse=True
            )[:20]
            
            total_func_time = sum(f.duration_ms for f in self.function_calls)
            
            report.append(f"{'Function':<50} {'Time(ms)':>12} {'Severity':>10}")
            for func in sorted_funcs:
                pct = 100 * func.duration_ms / total_func_time if total_func_time else 0
                report.append(
                    f"{func.name:<50} {func.duration_ms:>10.1f}ms ({pct:>4.1f}%) [{func.severity}]"
                )
            
            report.append(f"\nTotal function time: {total_func_time:.1f}ms")
        
        # Query stats
        if self.queries:
            report.append("\n" + "-"*80)
            report.append("DATABASE QUERIES (Top 20 slowest)")
            report.append("-"*80)
            
            sorted_queries = sorted(
                self.queries,
                key=lambda x: x.duration_ms,
                reverse=True
            )[:20]
            
            total_query_time = sum(q.duration_ms for q in self.queries)
            
            report.append(f"{'Query':<40} {'Time(ms)':>12} {'Rows':>8}")
            for query in sorted_queries:
                report.append(
                    f"{query.query_prefix:<40} {query.duration_ms:>10.1f}ms {query.rows_affected:>8}"
                )
            
            report.append(f"\nTotal query time: {total_query_time:.1f}ms across {len(self.queries)} queries")
        
        # Memory stats
        if self.memory_snapshots:
            report.append("\n" + "-"*80)
            report.append("MEMORY USAGE")
            report.append("-"*80)
            
            report.append(f"{'Checkpoint':<30} {'RSS(MB)':>12} {'VMS(MB)':>12} {'Delta(MB)':>12}")
            
            for i, snap in enumerate(self.memory_snapshots):
                if i > 0:
                    delta = snap.rss_mb - self.memory_snapshots[i-1].rss_mb
                    delta_str = f"+{delta:.1f}" if delta > 0 else f"{delta:.1f}"
                else:
                    delta_str = "baseline"
                
                report.append(
                    f"{snap.label:<30} {snap.rss_mb:>12.1f} {snap.vms_mb:>12.1f} {delta_str:>12}"
                )
            
            peak_rss = max(s.rss_mb for s in self.memory_snapshots)
            report.append(f"\nPeak memory: {peak_rss:.1f}MB")
        
        report.append("\n" + "="*80)
        return "\n".join(report)
    
    def report_json(self) -> Dict:
        """Export metrics as JSON for analysis"""
        return {
            'timestamp': datetime.now().isoformat(),
            'duration_s': time.time() - self.start_time,
            'functions': [asdict(f) for f in self.function_calls],
            'queries': [asdict(q) for q in self.queries],
            'memory': [asdict(m) for m in self.memory_snapshots],
        }


# Global collector instance
_collector = PerformanceCollector()


# ============================================================================
# DECORATORS FOR INSTRUMENTATION
# ============================================================================

def profile_function(func):
    """Decorator to measure function execution time"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed = (time.perf_counter() - start) * 1000  # Convert to ms
            _collector.record_function(
                func.__name__,
                func.__module__,
                elapsed
            )
    return wrapper


@contextmanager
def profile_block(label: str):
    """Context manager for profiling code blocks"""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = (time.perf_counter() - start) * 1000
        _collector.record_function(label, "block", elapsed)
        if elapsed > 100:
            logger.warning(f"Slow block '{label}': {elapsed:.1f}ms")


@contextmanager
def profile_query(query_prefix: str):
    """Context manager for profiling database queries"""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = (time.perf_counter() - start) * 1000
        _collector.record_query(query_prefix, elapsed)
        if elapsed > 100:
            logger.warning(f"Slow query '{query_prefix}': {elapsed:.1f}ms")


# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def get_report() -> str:
    """Get the current performance report"""
    return _collector.report_text()


def save_report(filename: str = "performance_report.txt"):
    """Save report to file"""
    with open(filename, 'w') as f:
        f.write(get_report())
    logger.info(f"Report saved to {filename}")


def export_metrics(filename: str = "metrics.json"):
    """Export raw metrics as JSON"""
    with open(filename, 'w') as f:
        json.dump(_collector.report_json(), f, indent=2)
    logger.info(f"Metrics exported to {filename}")


def memory_snapshot(label: str):
    """Capture a memory snapshot"""
    _collector.record_memory(label)


# ============================================================================
# EXAMPLE USAGE SCRIPT
# ============================================================================

@profile_function
def example_cpu_bound_function(n: int = 10**6) -> int:
    """Example CPU-bound function"""
    result = 0
    for i in range(n):
        result += i ** 2
    return result


@profile_function
def example_io_bound_function():
    """Example I/O-bound function"""
    import sqlite3
    
    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    
    with profile_query("CREATE TABLE"):
        cur.execute('CREATE TABLE test (id INTEGER, value TEXT)')
    
    with profile_query("INSERT 1000"):
        for i in range(1000):
            cur.execute('INSERT INTO test VALUES (?, ?)', (i, f'value_{i}'))
    
    with profile_query("SELECT all"):
        cur.execute('SELECT COUNT(*) FROM test')
        result = cur.fetchone()[0]
    
    conn.close()
    return result


def run_example():
    """Run an example profiling session"""
    logger.info("Starting example profiling session...")
    
    memory_snapshot("Start")
    
    logger.info("Running CPU-bound function...")
    with profile_block("CPU-intensive loop"):
        example_cpu_bound_function(10**7)
    
    memory_snapshot("After CPU work")
    
    logger.info("Running I/O-bound function...")
    with profile_block("I/O operations"):
        example_io_bound_function()
    
    memory_snapshot("After I/O work")
    
    logger.info("Profiling complete!")
    print(get_report())
    
    return _collector


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DOPAMS ETL Performance Profiler')
    parser.add_argument('--example', action='store_true', help='Run example profiling')
    parser.add_argument('--save', type=str, help='Save report to file')
    parser.add_argument('--export', type=str, help='Export metrics as JSON')
    
    args = parser.parse_args()
    
    if args.example:
        collector = run_example()
        if args.save:
            save_report(args.save)
        if args.export:
            export_metrics(args.export)
    else:
        print("""
Performance Profiler Toolkit for DOPAMS ETL
============================================

To use in your pipeline:

1. Import the decorator:
   from performance_profiler import profile_function, profile_block, memory_snapshot

2. Add to functions:
   @profile_function
   def my_function():
       ...

3. Or wrap code blocks:
   with profile_block("my operation"):
       ...

4. Capture memory:
   memory_snapshot("description")

5. Get report:
   from performance_profiler import get_report
   print(get_report())

Run example: python performance_profiler.py --example
        """)
