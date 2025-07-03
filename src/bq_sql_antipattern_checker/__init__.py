"""BigQuery SQL Antipattern Checker.

A Python package for detecting SQL antipatterns in BigQuery queries to help optimize
performance and reduce costs.

This package provides tools to:
- Analyze BigQuery job history
- Detect common SQL antipatterns
- Generate actionable optimization recommendations
- Export results for further analysis

Example:
    Basic usage with default configuration:

    >>> from bq_sql_antipattern_checker.main import main
    >>> main()

    Or use the CLI:

    $ bq-antipattern-checker run --config my-config.yaml
"""

__version__ = "0.1.0"
__author__ = "Platform Engineering Team"
__email__ = "platform-engineering@company.com"

from .config import AntipatternConfig, Config
from .main import app

__all__ = ["Config", "AntipatternConfig", "app"]
