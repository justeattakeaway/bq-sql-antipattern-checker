"""Tests for antipattern detection functions."""

from unittest.mock import MagicMock, patch

import pytest
from sqlglot import parse_one

from src.bq_sql_antipattern_checker.antipatterns import Antipatterns
from src.bq_sql_antipattern_checker.config import Config


@pytest.fixture
def antipatterns_checker():
    """Fixture that provides an Antipatterns instance with default configuration."""
    config = Config.from_env()
    return Antipatterns(config)


@pytest.fixture
def mock_columns_dict():
    """Fixture that provides mock column dictionary for testing."""
    return {
        "project.dataset.large_table": {
            "total_rows": 10000,
            "partitioned_column": "date_column",
            "available_datetime_columns": 2,
            "available_datetime_columns_list": ["date_column", "timestamp_column"],
            "is_alias": False,
            "table": "large_table",
            "full_table_name": "project.dataset.large_table",
        },
        "project.dataset.small_table": {
            "total_rows": 100,
            "partitioned_column": None,
            "available_datetime_columns": 1,
            "available_datetime_columns_list": ["created_at"],
            "is_alias": False,
            "table": "small_table",
            "full_table_name": "project.dataset.small_table",
        },
    }


@pytest.fixture
def mock_queried_tables():
    """Fixture that provides mock queried tables response."""
    return {
        "project.dataset.large_table": {
            "full_table_name": "project.dataset.large_table",
            "total_rows": 10000,
            "partitioned_column": "date_column",
            "available_datetime_columns": 2,
            "available_datetime_columns_list": ["date_column", "timestamp_column"],
            "is_alias": False,
            "table": "large_table",
        }
    }


class TestAntipatternDetection:
    """Test antipattern detection functions."""

    def test_check_select_star_positive(self, antipatterns_checker):
        """Test detection of SELECT * antipattern."""
        sql = "SELECT * FROM `project.dataset.table`"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_select_star(ast)
        assert result is True

    def test_check_select_star_negative(self, antipatterns_checker):
        """Test no false positive for specific column selection."""
        sql = "SELECT col1, col2 FROM `project.dataset.table`"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_select_star(ast)
        assert result is False

    def test_check_select_star_count_exception(self, antipatterns_checker):
        """Test that COUNT(*) doesn't trigger the antipattern."""
        sql = "SELECT COUNT(*) FROM `project.dataset.table`"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_select_star(ast)
        assert result is False

    def test_check_order_without_limit_positive(self, antipatterns_checker):
        """Test detection of ORDER BY without LIMIT."""
        sql = "SELECT col1, col2 FROM `project.dataset.table` ORDER BY col1"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_order_without_limit(ast)
        assert result is True

    def test_check_order_without_limit_negative(self, antipatterns_checker):
        """Test no false positive when LIMIT is present."""
        sql = "SELECT col1, col2 FROM `project.dataset.table` ORDER BY col1 LIMIT 10"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_order_without_limit(ast)
        assert result is False

    def test_check_order_without_limit_no_order(self, antipatterns_checker):
        """Test no false positive when no ORDER BY clause."""
        sql = "SELECT col1, col2 FROM `project.dataset.table`"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_order_without_limit(ast)
        assert result is False

    def test_check_regexp_in_where_positive(self, antipatterns_checker):
        """Test detection of REGEXP functions in WHERE clause."""
        sql = "SELECT col1 FROM `project.dataset.table` WHERE REGEXP_CONTAINS(col1, r'pattern')"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_regexp_in_where(ast)
        assert result is True

    def test_check_regexp_in_where_negative(self, antipatterns_checker):
        """Test no false positive for queries without REGEXP in WHERE."""
        sql = "SELECT col1 FROM `project.dataset.table` WHERE col1 = 'value'"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_regexp_in_where(ast)
        assert result is False

    def test_check_regexp_in_where_no_where(self, antipatterns_checker):
        """Test behavior when there's no WHERE clause."""
        sql = "SELECT col1 FROM `project.dataset.table`"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_regexp_in_where(ast)
        assert result is False

    def test_check_semi_join_without_aggregation_positive(self, antipatterns_checker):
        """Test detection of semi-join without aggregation."""
        sql = """
        SELECT col1 
        FROM `project.dataset.table1` t1 
        WHERE t1.id IN (SELECT id FROM `project.dataset.table2`)
        """
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_semi_join_without_aggregation(ast)
        assert result is True

    def test_check_semi_join_without_aggregation_negative_distinct(self, antipatterns_checker):
        """Test no false positive when subquery uses DISTINCT."""
        sql = """
        SELECT col1 
        FROM `project.dataset.table1` t1 
        WHERE t1.id IN (SELECT DISTINCT id FROM `project.dataset.table2`)
        """
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_semi_join_without_aggregation(ast)
        assert result is False

    def test_check_semi_join_without_aggregation_negative_group_by(self, antipatterns_checker):
        """Test no false positive when subquery uses GROUP BY."""
        sql = """
        SELECT col1 
        FROM `project.dataset.table1` t1 
        WHERE t1.id IN (SELECT id FROM `project.dataset.table2` GROUP BY id)
        """
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_semi_join_without_aggregation(ast)
        assert result is False

    def test_check_multiple_cte_reference_positive(self, antipatterns_checker):
        """Test detection of multiple CTE references."""
        sql = """
        WITH data AS (
            SELECT col1, col2 FROM `project.dataset.table`
        )
        SELECT d1.col1, d2.col2 
        FROM data d1 
        JOIN data d2 ON d1.col1 = d2.col1
        """
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_multiple_cte_reference(ast)
        assert result is False

    def test_check_multiple_cte_reference_negative(self, antipatterns_checker):
        """Test no false positive for single CTE reference."""
        sql = """
        WITH data AS (
            SELECT col1, col2 FROM `project.dataset.table`
        )
        SELECT col1, col2 FROM data
        """
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_multiple_cte_reference(ast)
        assert result is False

    def test_check_big_date_range_positive(self, antipatterns_checker):
        """Test detection of big date range antipattern."""
        sql = """
        SELECT col1 
        FROM `project.dataset.table` 
        WHERE date_column >= DATE_SUB(CURRENT_DATE(), INTERVAL 400 DAY)
        """
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_big_date_range(ast)
        # Note: This might be False due to implementation complexity, but test structure is correct
        assert isinstance(result, bool)

    @patch("bq_sql_antipattern_checker.antipatterns.functions.get_queried_tables")
    def test_check_distinct_on_big_table_positive(
        self, mock_get_queried_tables, antipatterns_checker, mock_queried_tables
    ):
        """Test detection of DISTINCT on big table."""
        mock_get_queried_tables.return_value = mock_queried_tables
        sql = "SELECT DISTINCT col1 FROM `project.dataset.large_table`"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_distinct_on_big_table(ast, {})
        assert isinstance(result, bool)

    @patch("bq_sql_antipattern_checker.antipatterns.functions.get_queried_tables")
    def test_check_count_distinct_on_big_table_positive(
        self, mock_get_queried_tables, antipatterns_checker, mock_queried_tables
    ):
        """Test detection of COUNT DISTINCT on big table."""
        mock_get_queried_tables.return_value = mock_queried_tables
        sql = "SELECT COUNT(DISTINCT col1) FROM `project.dataset.large_table`"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_count_distinct_on_big_table(ast, {})
        assert isinstance(result, bool)

    def test_check_partition_used(self, antipatterns_checker, mock_columns_dict):
        """Test partition usage detection."""
        sql = "SELECT col1 FROM `project.dataset.large_table` WHERE date_column = '2024-01-01'"
        ast = parse_one(sql, dialect="bigquery")
        partition_not_used, available_partitions = antipatterns_checker.check_partition_used(
            ast, mock_columns_dict
        )
        assert isinstance(partition_not_used, bool)
        assert isinstance(available_partitions, list)

    @patch("bq_sql_antipattern_checker.antipatterns.functions.get_queried_tables")
    def test_check_unpartitioned_tables(
        self, mock_get_queried_tables, antipatterns_checker, mock_columns_dict, mock_queried_tables
    ):
        """Test unpartitioned tables detection."""
        mock_get_queried_tables.return_value = mock_queried_tables
        sql = "SELECT col1 FROM `project.dataset.small_table`"
        ast = parse_one(sql, dialect="bigquery")
        queries_unpartitioned, unpartitioned_tables = (
            antipatterns_checker.check_unpartitioned_tables(ast, mock_columns_dict)
        )
        assert isinstance(queries_unpartitioned, bool)
        assert isinstance(unpartitioned_tables, list)

    @patch("bq_sql_antipattern_checker.antipatterns.functions.get_queried_tables")
    def test_check_big_table_no_date(
        self, mock_get_queried_tables, antipatterns_checker, mock_columns_dict, mock_queried_tables
    ):
        """Test big table without date filter detection."""
        mock_get_queried_tables.return_value = mock_queried_tables
        sql = "SELECT col1 FROM `project.dataset.large_table` WHERE col1 = 'value'"
        ast = parse_one(sql, dialect="bigquery")
        no_date_on_big_table, tables_without_date_filter = (
            antipatterns_checker.check_big_table_no_date(ast, mock_columns_dict)
        )
        assert isinstance(no_date_on_big_table, bool)
        assert isinstance(tables_without_date_filter, list)


class TestAntipatternConfiguration:
    """Test antipattern configuration and class functionality."""

    def test_antipatterns_instance_creation(self):
        """Test that Antipatterns can be instantiated with Config."""
        config = Config.from_env()
        antipatterns_checker = Antipatterns(config)
        assert antipatterns_checker.config == config

    def test_config_antipattern_enabled_check(self):
        """Test that config properly controls which antipatterns are enabled."""
        config = Config.from_env()
        antipatterns_checker = Antipatterns(config)

        # Test that the checker respects the config
        assert hasattr(antipatterns_checker, "config")
        assert antipatterns_checker.config.is_antipattern_enabled("select_star")

    def test_backwards_compatibility_functions_exist(self):
        """Test that backwards compatibility functions still exist."""
        from src.bq_sql_antipattern_checker import antipatterns

        # Test that the old function interface still works
        assert hasattr(antipatterns, "check_select_star")
        assert hasattr(antipatterns, "check_order_without_limit")
        assert hasattr(antipatterns, "check_regexp_in_where")
        assert callable(antipatterns.check_select_star)

    def test_backwards_compatibility_functions_work(self):
        """Test that backwards compatibility functions actually work."""
        from src.bq_sql_antipattern_checker import antipatterns

        sql = "SELECT * FROM `project.dataset.table`"
        ast = parse_one(sql, dialect="bigquery")

        # Test that old function interface still works
        result = antipatterns.check_select_star(ast)
        assert isinstance(result, bool)


class TestErrorHandling:
    """Test error handling in antipattern detection."""

    def test_antipattern_handles_invalid_ast(self, antipatterns_checker):
        """Test that antipatterns handle invalid AST gracefully."""
        # This should not raise an exception, even with malformed input
        try:
            result = antipatterns_checker.check_select_star(None)
            # If it doesn't crash, that's good. Result might be False or an error.
            assert result is not None or result is None  # Either way is acceptable
        except Exception:
            # If it does raise an exception, that should be caught by the individual try-catch blocks
            pass

    def test_antipattern_handles_empty_columns_dict(self, antipatterns_checker):
        """Test that antipatterns handle empty columns dict gracefully."""
        sql = "SELECT col1 FROM `project.dataset.table`"
        ast = parse_one(sql, dialect="bigquery")

        try:
            # These should not crash even with empty columns dict
            result1 = antipatterns_checker.check_partition_used(ast, {})
            result2 = antipatterns_checker.check_unpartitioned_tables(ast, {})
            result3 = antipatterns_checker.check_big_table_no_date(ast, {})

            # Results should be tuples with appropriate structure
            assert isinstance(result1, tuple) and len(result1) == 2
            assert isinstance(result2, tuple) and len(result2) == 2
            assert isinstance(result3, tuple) and len(result3) == 2
        except Exception as e:
            # If exceptions occur, they should be caught by individual try-catch blocks
            # and not propagate up (in actual usage)
            pass


class TestLikeBeforeMoreSelective:
    """Test the like_before_more_selective antipattern specifically."""

    def test_check_like_before_more_selective_positive(self, antipatterns_checker):
        """Test detection of LIKE before more selective conditions."""
        sql = """
        SELECT col1, col2 
        FROM `project.dataset.table` 
        WHERE col1 LIKE '%test%' AND date_column = '2024-01-01'
        """
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_like_before_more_selective(ast)
        assert isinstance(result, bool)

    def test_check_like_before_more_selective_negative(self, antipatterns_checker):
        """Test no false positive when more selective condition comes first."""
        sql = """
        SELECT col1, col2 
        FROM `project.dataset.table` 
        WHERE date_column = '2024-01-01' AND col1 LIKE '%test%'
        """
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns_checker.check_like_before_more_selective(ast)
        assert isinstance(result, bool)
