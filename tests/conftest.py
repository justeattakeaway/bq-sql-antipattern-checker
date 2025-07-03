"""Test configuration and fixtures."""

from pathlib import Path

import pytest

from bq_sql_antipattern_checker.config import Config


@pytest.fixture
def sample_config() -> Config:
    """Create a sample configuration for testing."""
    return Config(
        bigquery_job_project="test-project",
        bigquery_dataset_project="test-dataset-project",
        bigquery_dataset="test_dataset",
        bigquery_region="US",
        information_schema_project="test-dataset-project",
        query_project="test-project",
        results_table_name="test_antipattern_results",
        large_table_row_count=1000,
        distinct_function_row_count=10000,
        days_back=1,
    )


@pytest.fixture
def sample_config_file(tmp_path: Path) -> Path:
    """Create a temporary config file for testing."""
    config_content = """
bigquery_job_project: "test-project"
bigquery_dataset_project: "test-dataset-project"
bigquery_dataset: "test_dataset"
bigquery_region: "US"
information_schema_project: "test-dataset-project"
query_project: "test-project"
results_table_name: "test_antipattern_results"
large_table_row_count: 1000
distinct_function_row_count: 10000
days_back: 1

antipatterns:
  select_star:
    enabled: true
    description: "Test description"
  partition_used:
    enabled: false
    description: "Test disabled"
"""
    config_file = tmp_path / "test-config.yaml"
    config_file.write_text(config_content)
    return config_file


@pytest.fixture
def sample_sql_queries():
    """Sample SQL queries for testing antipattern detection."""
    return {
        "select_star": "SELECT * FROM `project.dataset.table`",
        "order_without_limit": "SELECT col1, col2 FROM `project.dataset.table` ORDER BY col1",
        "clean_query": "SELECT col1, col2 FROM `project.dataset.table` WHERE date = '2023-01-01'",
    }
