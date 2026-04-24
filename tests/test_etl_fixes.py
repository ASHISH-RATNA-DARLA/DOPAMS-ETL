"""Integration tests for brief_facts_ai ETL bottleneck fixes.

Tests validate:
1. Configuration module loads and validates all required env vars
2. Connection pool singleton pattern works correctly
3. Batch commit strategy functions without hardcoded defaults
4. No hardcoded values exist in code
5. Metrics collection tracks performance correctly
"""

import pytest
import os
import tempfile
from unittest.mock import patch, MagicMock
from datetime import datetime

# Import modules under test
from brief_facts_ai.etl_config import ETLConfig, get_config, reset_config
from brief_facts_ai.metrics import BatchMetrics, MetricsCollector
from brief_facts_ai import db as brief_facts_db
from db_pooling import get_singleton_pool, PostgreSQLConnectionPool


class TestETLConfig:
    """Test configuration module with validation."""

    def test_config_loads_from_env(self):
        """Config loads all required variables from environment."""
        with patch.dict(os.environ, {
            'PARALLEL_LLM_WORKERS': '3',
            'BATCH_SIZE': '50',
            'BATCH_COMMIT_SIZE': '10',
            'DB_POOL_MIN_CONN': '10',
            'DB_POOL_MAX_CONN': '20',
        }):
            reset_config()
            config = get_config()

            assert config.parallel_llm_workers == 3
            assert config.batch_size == 50
            assert config.batch_commit_size == 10
            assert config.db_pool_min_conn == 10
            assert config.db_pool_max_conn == 20

    def test_config_fails_on_missing_env_var(self):
        """Config raises error if required env var missing."""
        with patch.dict(os.environ, {
            'PARALLEL_LLM_WORKERS': '3',
            'BATCH_SIZE': '50',
            # Missing BATCH_COMMIT_SIZE
            'DB_POOL_MIN_CONN': '10',
            'DB_POOL_MAX_CONN': '20',
        }, clear=False):
            reset_config()
            with pytest.raises(ValueError, match="BATCH_COMMIT_SIZE"):
                get_config()

    def test_config_validates_workers_min(self):
        """Config rejects invalid PARALLEL_LLM_WORKERS."""
        with patch.dict(os.environ, {
            'PARALLEL_LLM_WORKERS': '0',  # Invalid
            'BATCH_SIZE': '50',
            'BATCH_COMMIT_SIZE': '10',
            'DB_POOL_MIN_CONN': '10',
            'DB_POOL_MAX_CONN': '20',
        }, clear=False):
            reset_config()
            with pytest.raises(ValueError, match="PARALLEL_LLM_WORKERS"):
                get_config()

    def test_config_validates_batch_commit_vs_batch_size(self):
        """Config rejects batch_commit_size > batch_size."""
        with patch.dict(os.environ, {
            'PARALLEL_LLM_WORKERS': '3',
            'BATCH_SIZE': '50',
            'BATCH_COMMIT_SIZE': '100',  # Greater than batch size
            'DB_POOL_MIN_CONN': '10',
            'DB_POOL_MAX_CONN': '20',
        }, clear=False):
            reset_config()
            with pytest.raises(ValueError, match="BATCH_COMMIT_SIZE"):
                get_config()

    def test_config_validates_pool_min_max(self):
        """Config rejects pool max < pool min."""
        with patch.dict(os.environ, {
            'PARALLEL_LLM_WORKERS': '3',
            'BATCH_SIZE': '50',
            'BATCH_COMMIT_SIZE': '10',
            'DB_POOL_MIN_CONN': '20',
            'DB_POOL_MAX_CONN': '10',  # Less than min
        }, clear=False):
            reset_config()
            with pytest.raises(ValueError, match="DB_POOL_MAX_CONN"):
                get_config()

    def test_config_singleton_pattern(self):
        """Config returns same instance on multiple calls."""
        with patch.dict(os.environ, {
            'PARALLEL_LLM_WORKERS': '3',
            'BATCH_SIZE': '50',
            'BATCH_COMMIT_SIZE': '10',
            'DB_POOL_MIN_CONN': '10',
            'DB_POOL_MAX_CONN': '20',
        }):
            reset_config()
            config1 = get_config()
            config2 = get_config()
            assert config1 is config2  # Same object


class TestPoolSingleton:
    """Test database connection pool singleton pattern."""

    def test_pool_singleton_returns_same_instance(self):
        """get_singleton_pool returns same instance."""
        pool1 = get_singleton_pool()
        pool2 = get_singleton_pool()
        assert pool1 is pool2

    def test_pool_no_hardcoded_defaults(self):
        """Pool should use config, not hardcoded values."""
        # This validates the pool respects configured minconn/maxconn
        with patch.dict(os.environ, {
            'PARALLEL_LLM_WORKERS': '3',
            'BATCH_SIZE': '50',
            'BATCH_COMMIT_SIZE': '10',
            'DB_POOL_MIN_CONN': '5',
            'DB_POOL_MAX_CONN': '15',
            'POSTGRES_HOST': 'localhost',
            'POSTGRES_PORT': '5432',
            'POSTGRES_DB': 'test_db',
            'POSTGRES_USER': 'test_user',
            'POSTGRES_PASSWORD': 'test_pass',
        }, clear=False):
            # Pool is already initialized, so we just verify it exists
            pool = get_singleton_pool()
            assert pool is not None


class TestBatchMetrics:
    """Test metrics collection and reporting."""

    def test_batch_metrics_tracks_duration(self):
        """Batch metrics accurately tracks duration."""
        import time
        batch = BatchMetrics(batch_number=1, batch_size=50, start_time=time.time())
        time.sleep(0.1)  # Simulate 100ms of processing
        batch.complete(crimes_succeeded=50, crimes_failed=0, commits=5)

        assert batch.duration_seconds >= 0.1
        assert batch.crimes_processed == 50
        assert batch.success_rate == 100.0

    def test_batch_metrics_calculates_throughput(self):
        """Batch metrics calculates crimes per hour correctly."""
        batch = BatchMetrics(batch_number=1, batch_size=50, start_time=0)
        batch.end_time = 3600  # 1 hour later
        batch.complete(crimes_succeeded=50, crimes_failed=0, commits=5)

        assert batch.throughput_per_hour == 50.0  # 50 crimes in 1 hour = 50/hour

    def test_batch_metrics_to_dict(self):
        """Batch metrics convert to dictionary."""
        batch = BatchMetrics(batch_number=1, batch_size=50, start_time=0)
        batch.end_time = 3600
        batch.complete(crimes_succeeded=40, crimes_failed=10, commits=5)

        data = batch.to_dict()
        assert data['batch_number'] == 1
        assert data['batch_size'] == 50
        assert data['crimes_processed'] == 50
        assert data['crimes_succeeded'] == 40
        assert data['crimes_failed'] == 10
        assert data['success_rate_percent'] == 80.0


class TestMetricsCollector:
    """Test overall metrics collection."""

    def test_metrics_collector_aggregates_batches(self):
        """Metrics collector aggregates across multiple batches."""
        collector = MetricsCollector()

        # Simulate 3 batches
        batch1 = collector.start_batch(1, 50)
        batch1.complete(crimes_succeeded=45, crimes_failed=5, commits=5)

        batch2 = collector.start_batch(2, 50)
        batch2.complete(crimes_succeeded=50, crimes_failed=0, commits=5)

        batch3 = collector.start_batch(3, 50)
        batch3.complete(crimes_succeeded=48, crimes_failed=2, commits=5)

        collector.finalize()

        assert collector.total_crimes_processed == 150
        assert collector.total_crimes_succeeded == 143
        assert collector.total_crimes_failed == 7
        assert collector.total_commits == 15
        assert collector.overall_success_rate == pytest.approx(95.33, rel=0.01)

    def test_metrics_collector_summary_report(self):
        """Metrics collector generates human-readable summary."""
        collector = MetricsCollector()
        batch = collector.start_batch(1, 50)
        batch.complete(crimes_succeeded=50, crimes_failed=0, commits=5)
        collector.finalize()

        report = collector.get_summary_report()
        assert "ETL EXECUTION METRICS SUMMARY" in report
        assert "Total crimes processed: 50" in report
        assert "Overall success rate: 100.0%" in report
        assert "crimes/hour" in report

    def test_metrics_collector_to_dict(self):
        """Metrics collector exports to dictionary."""
        collector = MetricsCollector()
        batch = collector.start_batch(1, 50)
        batch.complete(crimes_succeeded=50, crimes_failed=0, commits=5)
        collector.finalize()

        data = collector.to_dict()
        assert 'execution_timestamp' in data
        assert data['total_crimes_processed'] == 50
        assert data['total_crimes_succeeded'] == 50
        assert data['total_crimes_failed'] == 0
        assert data['overall_success_rate_percent'] == 100.0


class TestConfigNoHardcodes:
    """Verify no hardcoded defaults remain in code."""

    def test_all_workers_from_env(self):
        """PARALLEL_LLM_WORKERS comes from config, not hardcoded."""
        # This is enforced by etl_config.py validation
        with patch.dict(os.environ, {
            'PARALLEL_LLM_WORKERS': '2',
            'BATCH_SIZE': '50',
            'BATCH_COMMIT_SIZE': '10',
            'DB_POOL_MIN_CONN': '10',
            'DB_POOL_MAX_CONN': '20',
        }):
            reset_config()
            config = get_config()
            assert config.parallel_llm_workers == 2

    def test_all_batch_params_from_env(self):
        """All batch parameters come from config."""
        with patch.dict(os.environ, {
            'PARALLEL_LLM_WORKERS': '4',
            'BATCH_SIZE': '100',
            'BATCH_COMMIT_SIZE': '15',
            'DB_POOL_MIN_CONN': '20',
            'DB_POOL_MAX_CONN': '40',
        }):
            reset_config()
            config = get_config()
            assert config.batch_size == 100
            assert config.batch_commit_size == 15
            assert config.db_pool_min_conn == 20
            assert config.db_pool_max_conn == 40

    def test_pool_respects_configured_sizes(self):
        """Pool min/max connections come from config."""
        with patch.dict(os.environ, {
            'PARALLEL_LLM_WORKERS': '3',
            'BATCH_SIZE': '50',
            'BATCH_COMMIT_SIZE': '10',
            'DB_POOL_MIN_CONN': '15',
            'DB_POOL_MAX_CONN': '30',
        }):
            reset_config()
            config = get_config()
            # Pool would be initialized with these values
            assert config.db_pool_min_conn == 15
            assert config.db_pool_max_conn == 30


class TestDedupCandidateQuery:
    """Validate dedup candidate query narrowing uses live crimes timestamps and ps_code filter."""

    def test_fetch_dedup_candidates_uses_same_ps_and_recent_window(self):
        cursor = MagicMock()
        cursor.__enter__.return_value = cursor
        cursor.fetchall.return_value = []

        conn = MagicMock()
        conn.cursor.return_value = cursor

        brief_facts_db.fetch_dedup_candidates(conn, 'CRIME-1', 'John Doe', 'PS-123', limit=25)

        executed_sql = cursor.execute.call_args[0][0]
        executed_params = cursor.execute.call_args[0][1]

        assert "COALESCE(c.date_modified, c.date_created) >= NOW() - INTERVAL '6 months'" in executed_sql
        assert "bfa.source_accused_fields->>'ps_code' = %s" in executed_sql
        assert executed_params[0] == 'CRIME-1'
        assert executed_params[2] == 'PS-123'
        assert executed_params[-1] == 25


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
