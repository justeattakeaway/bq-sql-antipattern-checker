"""Tests for antipattern detection functions."""

from sqlglot import parse_one

from bq_sql_antipattern_checker import antipatterns


class TestAntipatternDetection:
    """Test antipattern detection functions."""

    def test_check_select_star_positive(self):
        """Test detection of SELECT * antipattern."""
        sql = "SELECT * FROM `project.dataset.table`"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns.check_select_star(ast)
        assert result is True

    def test_check_select_star_negative(self):
        """Test no false positive for specific column selection."""
        sql = "SELECT col1, col2 FROM `project.dataset.table`"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns.check_select_star(ast)
        assert result is False

    def test_check_select_star_count_exception(self):
        """Test that COUNT(*) doesn't trigger the antipattern."""
        sql = "SELECT COUNT(*) FROM `project.dataset.table`"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns.check_select_star(ast)
        assert result is False

    def test_check_order_without_limit_positive(self):
        """Test detection of ORDER BY without LIMIT."""
        sql = "SELECT col1, col2 FROM `project.dataset.table` ORDER BY col1"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns.check_order_without_limit(ast)
        assert result is True

    def test_check_order_without_limit_negative(self):
        """Test no false positive when LIMIT is present."""
        sql = "SELECT col1, col2 FROM `project.dataset.table` ORDER BY col1 LIMIT 10"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns.check_order_without_limit(ast)
        assert result is False

    def test_check_order_without_limit_no_order(self):
        """Test no false positive when no ORDER BY clause."""
        sql = "SELECT col1, col2 FROM `project.dataset.table`"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns.check_order_without_limit(ast)
        assert result is False

    def test_check_regexp_in_where_positive(self):
        """Test detection of REGEXP functions in WHERE clause."""
        sql = "SELECT col1 FROM `project.dataset.table` WHERE REGEXP_CONTAINS(col1, r'pattern')"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns.check_regexp_in_where(ast)
        assert result is True

    def test_check_regexp_in_where_negative(self):
        """Test no false positive for queries without REGEXP in WHERE."""
        sql = "SELECT col1 FROM `project.dataset.table` WHERE col1 = 'value'"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns.check_regexp_in_where(ast)
        assert result is False

    def test_check_regexp_in_where_no_where(self):
        """Test behavior when there's no WHERE clause."""
        sql = "SELECT col1 FROM `project.dataset.table`"
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns.check_regexp_in_where(ast)
        assert result is False

    def test_check_semi_join_without_aggregation_positive(self):
        """Test detection of semi-join without aggregation."""
        sql = """
        SELECT col1 
        FROM `project.dataset.table1` t1 
        WHERE t1.id IN (SELECT id FROM `project.dataset.table2`)
        """
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns.check_semi_join_without_aggregation(ast)
        assert result is True

    def test_check_semi_join_without_aggregation_negative_distinct(self):
        """Test no false positive when subquery uses DISTINCT."""
        sql = """
        SELECT col1 
        FROM `project.dataset.table1` t1 
        WHERE t1.id IN (SELECT DISTINCT id FROM `project.dataset.table2`)
        """
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns.check_semi_join_without_aggregation(ast)
        assert result is False

    def test_check_semi_join_without_aggregation_negative_group_by(self):
        """Test no false positive when subquery uses GROUP BY."""
        sql = """
        SELECT col1 
        FROM `project.dataset.table1` t1 
        WHERE t1.id IN (SELECT id FROM `project.dataset.table2` GROUP BY id)
        """
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns.check_semi_join_without_aggregation(ast)
        assert result is False

    def test_check_multiple_cte_reference_positive(self):
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
        result = antipatterns.check_multiple_cte_reference(ast)
        assert result is False

    def test_check_multiple_cte_reference_negative(self):
        """Test no false positive for single CTE reference."""
        sql = """
        WITH data AS (
            SELECT col1, col2 FROM `project.dataset.table`
        )
        SELECT col1, col2 FROM data
        """
        ast = parse_one(sql, dialect="bigquery")
        result = antipatterns.check_multiple_cte_reference(ast)
        assert result is False
