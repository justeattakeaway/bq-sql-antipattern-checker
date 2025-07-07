"""Configuration management for BigQuery SQL Antipattern Checker.

This module provides configuration classes and utilities for managing application
settings, including BigQuery connection parameters, antipattern configurations,
and runtime options.

The configuration system supports:
- YAML file-based configuration
- Environment variable overrides
- Individual antipattern enable/disable
- Backwards compatibility with legacy configuration

Example:
    Load configuration from YAML file:

    >>> config = Config.from_yaml(Path("config.yaml"))
    >>> print(config.get_enabled_antipatterns())

    Load from environment variables:

    >>> config = Config.from_env()
    >>> if config.is_antipattern_enabled("select_star"):
    ...     print("SELECT * detection is enabled")
"""

import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import yaml


@dataclass
class AntipatternConfig:
    """Configuration for individual antipattern checks"""

    enabled: bool = True
    description: str | None = None


@dataclass
class Config:
    """Main configuration class for BQ SQL Antipattern Checker"""

    # BigQuery Configuration
    bigquery_job_project: str
    bigquery_dataset_project: str
    bigquery_dataset: str
    bigquery_region: str
    information_schema_project: list[str]
    query_project: list[str]

    # Table Configuration
    results_table_name: str

    # Thresholds
    large_table_row_count: int
    distinct_function_row_count: int

    # Date Configuration
    days_back: int = 1

    # Antipattern Configuration
    antipatterns: dict[str, AntipatternConfig] | None = None

    def __post_init__(self) -> None:
        """Initialize computed values and defaults"""
        if self.antipatterns is None:
            self.antipatterns = self._get_default_antipatterns()

        # Compute date values
        self.job_run_date = date.today() - timedelta(days=self.days_back)
        self.date_values = {
            "partition_date": str(self.job_run_date.strftime("%Y%m%d")),
            "query_run_date": self.job_run_date,
            "query_run_date_str": "'" + str(self.job_run_date) + "'",
        }

        self.run_dates = []
        run_date = self.job_run_date
        while run_date < date.today():
            self.run_dates.append(str(run_date))
            run_date += timedelta(days=1)

        # Table names dictionary for backwards compatibility
        self.table_names = {"results": self.results_table_name}

    def _get_default_antipatterns(self) -> dict[str, AntipatternConfig]:
        """Get default antipattern configurations"""
        return {
            "select_star": AntipatternConfig(
                enabled=True, description="Check for SELECT * statements"
            ),
            "semi_join_without_aggregation": AntipatternConfig(
                enabled=True, description="Check for semi-joins without aggregation"
            ),
            "order_without_limit": AntipatternConfig(
                enabled=True, description="Check for ORDER BY without LIMIT"
            ),
            "regexp_in_where": AntipatternConfig(
                enabled=True, description="Check for REGEXP functions in WHERE clauses"
            ),
            "like_before_more_selective": AntipatternConfig(
                enabled=True,
                description="Check for LIKE conditions before more selective conditions",
            ),
            "multiple_cte_reference": AntipatternConfig(
                enabled=True, description="Check for CTEs referenced multiple times"
            ),
            "partition_used": AntipatternConfig(
                enabled=True, description="Check if partitioned tables are properly filtered"
            ),
            "big_date_range": AntipatternConfig(
                enabled=True, description="Check for date ranges larger than 365 days"
            ),
            "big_table_no_date": AntipatternConfig(
                enabled=True, description="Check for queries on large tables without date filters"
            ),
            "unpartitioned_tables": AntipatternConfig(
                enabled=True, description="Check for queries on large unpartitioned tables"
            ),
            "distinct_on_big_table": AntipatternConfig(
                enabled=True, description="Check for DISTINCT on large tables"
            ),
            "count_distinct_on_big_table": AntipatternConfig(
                enabled=True, description="Check for COUNT DISTINCT on large tables"
            ),
        }

    def is_antipattern_enabled(self, antipattern_name: str) -> bool:
        """Check if an antipattern is enabled"""
        if self.antipatterns is None:
            return True
        return self.antipatterns.get(antipattern_name, AntipatternConfig()).enabled

    def get_enabled_antipatterns(self) -> list[str]:
        """Get list of enabled antipattern names"""
        if self.antipatterns is None:
            return []
        return [name for name, config in self.antipatterns.items() if config.enabled]

    @classmethod
    def from_yaml(cls, config_path: Path) -> "Config":
        """Load configuration from YAML file"""
        with open(config_path) as f:
            data = yaml.safe_load(f)

        # Convert antipatterns dict to AntipatternConfig objects
        antipatterns = {}
        if "antipatterns" in data:
            for name, config_data in data["antipatterns"].items():
                if isinstance(config_data, dict):
                    antipatterns[name] = AntipatternConfig(**config_data)
                else:
                    # Handle simple boolean values
                    antipatterns[name] = AntipatternConfig(enabled=config_data)

        # Remove antipatterns from data to avoid duplication
        data.pop("antipatterns", None)

        return cls(antipatterns=antipatterns, **data)

    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables (backwards compatibility)"""
        return cls(
            bigquery_job_project=os.getenv("BIGQUERY_JOB_PROJECT", "dev-project"),
            bigquery_dataset_project=os.getenv(
                "DESTINATION_DATASET_PROJECT", "dev-dataset-project"
            ),
            bigquery_dataset=os.getenv("DESTINATION_DATASET", "dev_dataset"),
            bigquery_region=os.getenv("BIGQUERY_REGION", "region-EU"),
            information_schema_project=os.getenv(
                "INFORMATION_SCHEMA_PROJECT",
                os.getenv("DESTINATION_DATASET_PROJECT", "dev-dataset-project"),
            ).split(","),
            query_project=os.getenv(
                "QUERY_PROJECT", os.getenv("BIGQUERY_JOB_PROJECT", "dev-project")
            ).split(","),
            results_table_name=os.getenv("RESULTS_TABLE_NAME", "antipattern_results"),
            large_table_row_count=int(os.getenv("LARGE_TABLE_ROW_COUNT", "1000")),
            distinct_function_row_count=int(os.getenv("DISTINCT_FUNCTION_ROW_COUNT", "10000")),
            days_back=int(os.getenv("DAYS_BACK", "1")),
        )


# Backwards compatibility - create default instance
_default_config = None


def get_default_config() -> Config:
    """Get the default configuration instance"""
    global _default_config
    if _default_config is None:
        _default_config = Config.from_env()
    return _default_config


# Export the old interface for backwards compatibility
def __getattr__(name: str) -> Any:
    """Provide backwards compatibility for old config access"""
    config = get_default_config()
    if hasattr(config, name):
        return getattr(config, name)
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
