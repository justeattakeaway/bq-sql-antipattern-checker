"""Core classes for BigQuery SQL Antipattern Checker.

This module contains the main Job class that represents a BigQuery job
and provides methods for antipattern detection and analysis.

The Job class encapsulates:
- Job metadata from BigQuery INFORMATION_SCHEMA
- SQL query parsing and AST generation
- Antipattern detection orchestration
- Result tracking and formatting

Example:
    Create and analyze a job:

    >>> job_data = {...}  # From BigQuery INFORMATION_SCHEMA
    >>> job = Job(job_data)
    >>> job.check_antipatterns(columns_dict, config)
    >>> print(f"SELECT * detected: {job.select_star}")
"""

from datetime import datetime
from typing import Any

import sqlparse
from sqlglot import exp, parse_one

from bq_sql_antipattern_checker.antipatterns import Antipatterns
from bq_sql_antipattern_checker.config import Config


class Job:
    """Represents a BigQuery job for antipattern analysis.

    This class encapsulates a BigQuery job's metadata and provides methods
    for detecting SQL antipatterns in the job's query. It maintains state
    for all detected antipatterns and provides a unified interface for
    antipattern analysis.

    Attributes:
        creation_date: Date when the job was created
        creation_time: Timestamp when the job was created
        project_id: BigQuery project where the job ran
        user_email: Email of the user who submitted the job
        query: SQL query text for analysis
        Various antipattern flags (select_star, partition_not_used, etc.)
    """

    def __init__(self, v: dict[str, Any], antipatterns: Antipatterns) -> None:
        """Initialize Job with BigQuery job metadata.

        Args:
            v: Dictionary containing job metadata from INFORMATION_SCHEMA.JOBS
            antipatterns: Antipatterns object
        """
        self.creation_date = v["creation_date"]
        self.creation_time = v["creation_time"].strftime("%Y-%m-%d %H:%M:%S")
        self.project_id = v["project_id"]
        self.user_email = v["user_email"]
        self.reservation_id = v["reservation_id"]
        self.total_process_gb = v["total_process_gb"]
        self.total_slot_hrs = v["total_slot_hrs"]
        self.total_duration_mins = v["total_duration_mins"]
        self.query = v["query"]

        # Initialize antipattern result attributes
        self.partition_not_used = False
        self.available_partitions: list[dict[str, str]] = []
        self.big_date_range = False
        self.no_date_on_big_table = False
        self.tables_without_date_filter: list[str] = []
        self.select_star = False
        self.references_cte_multiple_times = False
        self.semi_join_without_aggregation = False
        self.order_without_limit = False
        self.like_before_more_selective = False
        self.regexp_in_where = False
        self.queries_unpartitioned_table = False
        self.unpartitioned_tables: list[str] = []
        self.distinct_on_big_table = False
        self.count_distinct_on_big_table = False

        self.antipattern_run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.antipatterns = antipatterns

    def get_statements(self) -> list[str]:
        """Split the job query into individual SQL statements.

        Uses sqlparse to split multi-statement queries into individual
        statements for separate analysis.

        Returns:
            list: List of individual SQL statement strings
        """
        statements: list[str] = sqlparse.split(self.query)
        return statements

    def check_antipatterns(
        self, columns_dict: dict[str, Any], config: Config | None = None
    ) -> None:
        """
        Check antipatterns based on the provided configuration.
        If no config is provided, checks all antipatterns (backwards compatibility).
        """
        # For backwards compatibility, if no config is provided, check all antipatterns
        if config is None:
            config = Config.from_env()

        statements = self.get_statements()
        for i in statements:
            try:
                if "declare" not in i.lower():
                    ast = parse_one(i, dialect="bigquery")

                    if not ast.find(exp.UserDefinedFunction) and not ast.find(exp.SetItem):
                        # Check partition usage
                        if config.is_antipattern_enabled("partition_used"):
                            try:
                                partition_not_used, available_partitions = (
                                    self.antipatterns.check_partition_used(ast, columns_dict)
                                )
                                if partition_not_used:
                                    self.partition_not_used = partition_not_used
                                    self.available_partitions += available_partitions
                                    self.available_partitions = [
                                        dict(t)
                                        for t in {
                                            tuple(d.items()) for d in self.available_partitions
                                        }
                                    ]
                                else:
                                    self.available_partitions = [
                                        {"table_name": "-", "partitioned_column": "-"}
                                    ]
                            except Exception as e:
                                print(f"Error in check_partition_used: {e!s}")

                        # Check big date range
                        if config.is_antipattern_enabled("big_date_range"):
                            try:
                                big_date_range = self.antipatterns.check_big_date_range(ast)
                                self.big_date_range = big_date_range
                            except Exception as e:
                                print(f"Error in check_big_date_range: {e!s}")

                        # Check big table without date filter
                        if config.is_antipattern_enabled("big_table_no_date"):
                            try:
                                no_date_on_big_table, tables_without_date_filter = (
                                    self.antipatterns.check_big_table_no_date(ast, columns_dict)
                                )
                                if no_date_on_big_table:
                                    self.no_date_on_big_table = no_date_on_big_table
                                    self.tables_without_date_filter += tables_without_date_filter
                                else:
                                    self.tables_without_date_filter = ["-"]
                            except Exception as e:
                                print(f"Error in check_big_table_no_date: {e!s}")

                        # Check select star
                        if config.is_antipattern_enabled("select_star"):
                            try:
                                # A job can have multiple SELECT statements executed. One case is enough to flag as True, hence max function
                                self.select_star = max(
                                    self.antipatterns.check_select_star(ast), self.select_star
                                )
                            except Exception as e:
                                print(f"Error in check_select_star: {e!s}")

                        # Check multiple CTE references
                        if config.is_antipattern_enabled("multiple_cte_reference"):
                            try:
                                references_cte_multiple_times = (
                                    self.antipatterns.check_multiple_cte_reference(ast)
                                )
                                self.references_cte_multiple_times = max(
                                    references_cte_multiple_times,
                                    self.references_cte_multiple_times,
                                )
                            except Exception as e:
                                print(f"Error in check_multiple_cte_reference: {e!s}")

                        # Check semi join without aggregation
                        if config.is_antipattern_enabled("semi_join_without_aggregation"):
                            try:
                                self.semi_join_without_aggregation = max(
                                    self.antipatterns.check_semi_join_without_aggregation(ast),
                                    self.semi_join_without_aggregation,
                                )
                            except Exception as e:
                                print(f"Error in check_semi_join_without_aggregation: {e!s}")

                        # Check order without limit
                        if config.is_antipattern_enabled("order_without_limit"):
                            try:
                                self.order_without_limit = max(
                                    self.antipatterns.check_order_without_limit(ast),
                                    self.order_without_limit,
                                )
                            except Exception as e:
                                print(f"Error in check_order_without_limit: {e!s}")

                        # Check like before more selective
                        if config.is_antipattern_enabled("like_before_more_selective"):
                            try:
                                self.like_before_more_selective = max(
                                    self.antipatterns.check_like_before_more_selective(ast),
                                    self.like_before_more_selective,
                                )
                            except Exception as e:
                                print(f"Error in check_like_before_more_selective: {e!s}")

                        # Check regexp in where
                        if config.is_antipattern_enabled("regexp_in_where"):
                            try:
                                self.regexp_in_where = max(
                                    self.antipatterns.check_regexp_in_where(ast),
                                    self.regexp_in_where,
                                )
                            except Exception as e:
                                print(f"Error in check_regexp_in_where: {e!s}")

                        # Check unpartitioned tables
                        if config.is_antipattern_enabled("unpartitioned_tables"):
                            try:
                                queries_unpartitioned_table, unpartitioned_tables = (
                                    self.antipatterns.check_unpartitioned_tables(ast, columns_dict)
                                )
                                self.queries_unpartitioned_table = max(
                                    queries_unpartitioned_table, self.queries_unpartitioned_table
                                )
                                self.unpartitioned_tables += (
                                    unpartitioned_tables
                                    if self.queries_unpartitioned_table
                                    else ["-"]
                                )
                            except Exception as e:
                                print(f"Error in check_unpartitioned_tables: {e!s}")

                        # Check distinct on big table
                        if config.is_antipattern_enabled("distinct_on_big_table"):
                            try:
                                self.distinct_on_big_table = max(
                                    self.antipatterns.check_distinct_on_big_table(
                                        ast, columns_dict
                                    ),
                                    self.distinct_on_big_table,
                                )
                            except Exception as e:
                                print(f"Error in check_distinct_on_big_table: {e!s}")

                        # Check count distinct on big table
                        if config.is_antipattern_enabled("count_distinct_on_big_table"):
                            try:
                                self.count_distinct_on_big_table = max(
                                    self.antipatterns.check_count_distinct_on_big_table(
                                        ast, columns_dict
                                    ),
                                    self.count_distinct_on_big_table,
                                )
                            except Exception as e:
                                print(f"Error in check_count_distinct_on_big_table: {e!s}")

            except Exception as e:
                print(f"Error processing statement: {e!s}")
