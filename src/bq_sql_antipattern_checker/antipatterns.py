"""SQL antipattern detection class for BigQuery optimization.

This module contains the Antipatterns class for detecting various SQL antipatterns
that can impact BigQuery performance and cost. The class is instantiated with a
Config object and provides methods to check individual antipatterns or run all
enabled antipatterns.

Supported antipatterns:
- SELECT * usage
- ORDER BY without LIMIT
- REGEXP functions in WHERE clauses
- Inefficient WHERE clause ordering
- Multiple CTE references
- Missing partition filters
- Large date ranges
- Missing date filters on large tables
- Queries on unpartitioned tables
- DISTINCT on large tables
- COUNT DISTINCT on large tables

Example:
    Create an antipattern checker and run analysis:

    >>> config = Config.from_env()
    >>> checker = Antipatterns(config)
    >>> results = checker.check_all_antipatterns(ast, columns_dict)
    >>> print(f"SELECT * detected: {results.get('select_star', False)}")
"""

import datetime as dt
from collections.abc import Iterator
from typing import Any

from sqlglot import exp

from . import functions
from .config import Config


class Antipatterns:
    """Class for managing and executing antipattern checks.
    
    This class provides a centralized way to manage antipattern detection
    based on configuration settings. It instantiates with a Config object
    and provides methods to check individual antipatterns or run all enabled
    antipatterns.
    
    Args:
        config: Configuration object containing antipattern settings
        
    Example:
        >>> config = Config.from_env()
        >>> checker = Antipatterns(config)
        >>> results = checker.check_all_antipatterns(ast, columns_dict)
    """
    
    def __init__(self, config: Config) -> None:
        """Initialize with configuration settings."""
        self.config = config
        self.columns_dict: dict[str, Any] = {}
        
    def set_columns_dict(self, columns_dict: dict[str, Any]) -> None:
        """Set the columns dictionary for table metadata."""
        self.columns_dict = columns_dict
        
    def check_all_antipatterns(self, ast: exp.Expression, columns_dict: dict[str, Any] | None = None) -> dict[str, Any]:
        """Check all enabled antipatterns for the given AST.
        
        Args:
            ast: SQLGlot AST to analyze
            columns_dict: Optional columns dictionary (uses instance if not provided)
            
        Returns:
            dict: Dictionary of antipattern results
        """
        if columns_dict is None:
            columns_dict = self.columns_dict
            
        results: dict[str, Any] = {}
        
        if self.config.is_antipattern_enabled("select_star"):
            results["select_star"] = self.check_select_star(ast)
            
        if self.config.is_antipattern_enabled("semi_join_without_aggregation"):
            results["semi_join_without_aggregation"] = self.check_semi_join_without_aggregation(ast)
            
        if self.config.is_antipattern_enabled("order_without_limit"):
            results["order_without_limit"] = self.check_order_without_limit(ast)
            
        if self.config.is_antipattern_enabled("regexp_in_where"):
            results["regexp_in_where"] = self.check_regexp_in_where(ast)
            
        if self.config.is_antipattern_enabled("like_before_more_selective"):
            results["like_before_more_selective"] = self.check_like_before_more_selective(ast)
            
        if self.config.is_antipattern_enabled("multiple_cte_reference"):
            results["multiple_cte_reference"] = self.check_multiple_cte_reference(ast)
            
        if self.config.is_antipattern_enabled("partition_used"):
            partition_not_used, available_partitions = self.check_partition_used(ast, columns_dict)
            results["partition_used"] = {
                "partition_not_used": partition_not_used,
                "available_partitions": available_partitions
            }
            
        if self.config.is_antipattern_enabled("big_date_range"):
            results["big_date_range"] = self.check_big_date_range(ast)
            
        if self.config.is_antipattern_enabled("big_table_no_date"):
            no_date_on_big_table, tables_without_date_filter = self.check_big_table_no_date(ast, columns_dict)
            results["big_table_no_date"] = {
                "no_date_on_big_table": no_date_on_big_table,
                "tables_without_date_filter": tables_without_date_filter
            }
            
        if self.config.is_antipattern_enabled("unpartitioned_tables"):
            queries_unpartitioned_table, unpartitioned_tables = self.check_unpartitioned_tables(ast, columns_dict)
            results["unpartitioned_tables"] = {
                "queries_unpartitioned_table": queries_unpartitioned_table,
                "unpartitioned_tables": unpartitioned_tables
            }
            
        if self.config.is_antipattern_enabled("distinct_on_big_table"):
            results["distinct_on_big_table"] = self.check_distinct_on_big_table(ast, columns_dict)
            
        if self.config.is_antipattern_enabled("count_distinct_on_big_table"):
            results["count_distinct_on_big_table"] = self.check_count_distinct_on_big_table(ast, columns_dict)
            
        return results

    def check_select_star(self, ast: exp.Expression) -> bool:
        """Anti Pattern: Selecting all columns
        avoids select * from CTEs or sub queries.
        only looks at a direct from statement where there's dataset value in source table
        ignores cases where no select is found like "update set from"
        """
        from_statements: Iterator[exp.From] = ast.find_all(exp.From)

        for f in from_statements:
            if f.parent_select:
                if f.parent_select.find(exp.Star) and not f.parent_select.find(exp.Count):
                    table_ref = f.args.get("this")
                    if table_ref and table_ref.args.get("db"):
                        return True
        return False

    def check_semi_join_without_aggregation(self, ast: exp.Expression) -> bool:
        """
        # Anti Pattern: SEMI-JOIN without aggregation
        if where clause as has a 'value in (select x from) like subquery statement
        it's best practice to select distinct or group by that value in that subquery
        """
        where_statements: Iterator[exp.Where] = ast.find_all(exp.Where)

        for w in where_statements:
            if "IN" in str(w):
                for s in w.find_all(exp.Select):
                    if len(list(s.find_all(exp.Distinct))) + len(list(s.find_all(exp.Group))) == 0:
                        return True
        return False

    def check_order_without_limit(self, ast: exp.Expression) -> bool:
        """
        Anti Pattern: Using ORDER BY without LIMIT
        queries using order by in the select statement without a limit.
        """
        if ast.args.get("order") and not ast.args.get("limit"):
            return True

        return False

    def check_regexp_in_where(self, ast: exp.Expression) -> bool:
        """
        Anti Pattern: Using REGEXP when LIKE is an option
        like is more performant than regexp functions
        """
        where_statement: exp.Where | None = ast.args.get("where")

        if not where_statement:
            return False

        regex_rules: list[type] = [exp.RegexpLike, exp.RegexpReplace, exp.RegexpExtract]

        return any(where_statement.find(regex) for regex in regex_rules)

    def check_like_before_more_selective(self, ast: exp.Expression) -> bool:
        """
        Anti Pattern: Where order, apply most selective expression first it's checking whether there are cases like
        "Like" or "Regexp Like, Contains" before a more selective statement like IN, EQ,GTE,LTE etc. BQ likes to have more
        selective statements first At the moment until we figure out a way to sort the hierarchy of statements,
        it's searching for string conditions
        TODO: can benefit from tidying up
        """
        where_statement: exp.Where | None = ast.args.get("where")

        if not where_statement:
            return False

        if (
            where_statement.find(exp.Like)
            or where_statement.find(exp.RegexpLike)
            or where_statement.find(exp.RegexpReplace)
            or where_statement.find(exp.RegexpExtract)
        ):
            string_version: str = repr(where_statement.args).replace(" ", "").replace("\n", "")
            one_eq_one_starter: int = string_version.find(
                "EQ(this=Literal(this=1,is_string=False),expression=Literal(this=1"
            )
            bool_starter: int = string_version.find("this=Boolean(this=True)")

            index_starter: int = max(one_eq_one_starter + bool_starter, 0)
            less_selective_clause_index: int = -1
            like_clause: int = string_version[index_starter:].find("Like")
            regexp_clause: int = string_version[index_starter:].find("Regexp")

            if like_clause > -1:
                if regexp_clause > -1:
                    less_selective_clause_index = min(like_clause, less_selective_clause_index)
                else:
                    less_selective_clause_index = like_clause

            more_selective_clauses: list[int] = []
            if string_version[index_starter:].find("GT") >= 0:
                more_selective_clauses.append(string_version[index_starter:].find("GT"))
            if string_version[index_starter:].find("LT") >= 0:
                more_selective_clauses.append(string_version[index_starter:].find("LT"))
            if string_version[index_starter:].find("EQ") >= 0:
                more_selective_clauses.append(string_version[index_starter:].find("EQ"))
            if string_version[index_starter:].find("expression=In") >= 0:
                more_selective_clauses.append(string_version[index_starter:].find("expression=In"))

            if more_selective_clauses:
                if min(more_selective_clauses) > less_selective_clause_index:
                    return True
        return False

    def check_multiple_cte_reference(self, ast: exp.Expression) -> bool:
        """
        Anti Pattern: Referencing Same Table Multiple Times
        """
        status: bool = False
        cte_values: dict[str, int] = {
            cte.alias: 0 for cte in ast.find_all(exp.CTE) if len(list(cte.find_all(exp.From))) > 0
        }
        if len(cte_values) > 0:
            for cte in ast.find_all(exp.CTE):
                for s in cte.find_all(exp.Select):
                    from_statement: exp.From | None = s.args.get("from")
                    if not from_statement:
                        continue

                    from_table: str = str(from_statement.args.get("this"))
                    if from_table in cte_values:
                        cte_values[from_table] += 1
                        if cte_values[from_table] == 2:
                            status = True

        return status

    def check_partition_used(
        self, ast: exp.Expression, columns_dict: dict[str, Any]
    ) -> tuple[bool, list[dict[str, str]]]:
        """
        Anti Pattern: Table not using partitions
        check if the query is using tables with partition
        check if the query is referring to those partitions in join or where clause
        """
        used_tables_with_partition: dict[str, dict[str, str]] = functions.get_partitioned_tables(
            ast, columns_dict
        )
        result: list[dict[str, str]] = []
        passed: list[str] = []
        if len(used_tables_with_partition) > 0:
            tables_with_partitions_used: set[str] = set()
            columns_from_join: list[exp.Column] = []
            columns_from_where: list[exp.Column] = []
            if ast.find_all(exp.Join):
                for j in ast.find_all(exp.Join):
                    columns_from_join += j.find_all(exp.Column)
            if ast.find_all(exp.Where):
                for w in ast.find_all(exp.Where):
                    columns_from_where += w.find_all(exp.Column)
            for c in columns_from_join + columns_from_where:
                column_name: str | None
                table_name: str | None
                column_name, table_name = functions.get_column_and_table_name_from_column(c)
                if column_name:
                    if table_name:
                        if table_name.replace("*", "") in used_tables_with_partition:
                            if (
                                column_name.lower()
                                == used_tables_with_partition[table_name]["partition_column"].lower()
                            ):
                                tables_with_partitions_used.add(
                                    used_tables_with_partition[table_name]["full_table_name"]
                                )
                    if not table_name:
                        for t in used_tables_with_partition:
                            if used_tables_with_partition[t]["partition_column"]:
                                if (
                                    column_name.lower()
                                    == used_tables_with_partition[t]["partition_column"].lower()
                                ):
                                    tables_with_partitions_used.add(
                                        used_tables_with_partition[t]["full_table_name"]
                                    )
                                    break
                    else:
                        # case for columns used without a fully qualified table name or alias. bad practice
                        for k, v in used_tables_with_partition.items():
                            if column_name.lower() == v["partition_column"].lower():
                                tables_with_partitions_used.add(v["full_table_name"])
            for k, v in used_tables_with_partition.items():
                if (
                    v["full_table_name"] not in list(tables_with_partitions_used)
                    and v["full_table_name"] not in passed
                ):
                    result.append(
                        {
                            "table_name": v["full_table_name"],
                            "partitioned_column": v["partition_column"],
                        }
                    )
                    passed.append(v["full_table_name"])
        return len(result) > 0, result

    def check_big_date_range(self, ast: exp.Expression) -> bool:
        """
        Anti Pattern: Long Date Range
        check if the query is using date range that is more than a year
        """
        date_range: list[Any] = []
        status: bool = False
        days: dict[str, float] = {
            "DAY": 1,
            "WEEK": 7,
            "MONTH": 30,
            "YEAR": 365,
            "MINUTE": 0.0007,
            "HOUR": 0.04,
            "QUARTER": 90,
            "SECOND": 0.00001166666667,
        }
        if len(list(ast.find_all(exp.Where)) + list(ast.find_all(exp.Join))) > 0:
            for w in list(ast.find_all(exp.Where)) + list(ast.find_all(exp.Join)):
                for d in (
                    list(w.find_all(exp.Between)) + list(w.find_all(exp.GTE)) + list(w.find_all(exp.GT))
                ):
                    if d.args["this"].find(exp.Identifier):
                        case_check: str = d.args["this"].find(exp.Identifier).args.get("this").lower()
                        cast_expr = d.find(exp.Cast)
                        if cast_expr:
                            cast_to = cast_expr.args.get("to")
                            if cast_to:
                                cast_this = cast_to.args.get("this")
                                if cast_this:
                                    case_check = str(cast_this).lower()
                        if "date" in case_check or "time" in case_check or "partition" in case_check:
                            date_diff: float | None = None
                            if (
                                d.find(exp.DateSub)
                                or d.find(exp.Sub)
                                or (d.find(exp.Neg) and d.find(exp.DateAdd))
                            ):
                                for i in (
                                    list(d.find_all(exp.DateSub))
                                    + list(d.find_all(exp.Sub))
                                    + list(d.find_all(exp.DateAdd))
                                ):
                                    if i.find(exp.Literal):
                                        for j in i.find_all(exp.Literal):
                                            literal_val = j.args.get("this")
                                            if literal_val and str(literal_val).isnumeric():
                                                length: int = int(literal_val)
                                                multiplier: float = 1
                                                unit_expr = i.args.get("unit")
                                                if unit_expr:
                                                    unit_val = unit_expr.args.get("this")
                                                    if unit_val:
                                                        multiplier = days[unit_val]
                                                var_expr = i.find(exp.Var)
                                                if var_expr:
                                                    var_val = var_expr.args.get("this")
                                                    if var_val:
                                                        multiplier = days[var_val]
                                                elif i.find(exp.Mul):
                                                    mul_expr = i.find(exp.Mul)
                                                    if mul_expr:
                                                        expr_val = mul_expr.args.get("expression")
                                                        if expr_val:
                                                            this_val = expr_val.args.get("this")
                                                            if this_val:
                                                                multiplier = int(this_val)
                                                date_diff = length * multiplier
                            elif d.args.get("low"):
                                low_expr = d.args.get("low")
                                if low_expr:
                                    this_val = low_expr.args.get("this")
                                    if this_val:
                                        date_exp: str = str(this_val).replace("'", "")
                                if len(str(date_exp)) > 9 and "-" in str(date_exp):
                                    date_conv: dt.datetime = dt.datetime.strptime(
                                        date_exp[:10], "%Y-%m-%d"
                                    )
                                    date_diff = (dt.datetime.now() - date_conv).days
                            elif d.args["expression"].find(exp.Literal):
                                date_exp = d.args["expression"].find(exp.Literal).args.get("this")
                                if len(str(date_exp)) > 9 and "-" in str(date_exp):
                                    date_conv = dt.datetime.strptime(date_exp[:10], "%Y-%m-%d")
                                    date_diff = (dt.datetime.now() - date_conv).days
                            if date_diff:
                                if date_diff > 365:
                                    status = True
        return status

    def check_big_table_no_date(
        self, ast: exp.Expression, columns_dict: dict[str, Any]
    ) -> tuple[bool, list[str]]:
        """
        Anti Pattern: Big Tables With No Date Filter
        TODO: can benefit from tidying up
        """

        def date_cte_names(table: exp.Table) -> tuple[str | None, str | None]:
            # need a way to find ctes used for date filteration
            full_table_name: str | None = None
            alias: str | None = None
            this_ref = table.args.get("this")
            if this_ref:
                this_val = this_ref.args.get("this")
                if this_val:
                    full_table_name = str(this_val)

            alias_ref = table.args.get("alias")
            if alias_ref:
                alias_this = alias_ref.args.get("this")
                if alias_this:
                    alias_this_val = alias_this.args.get("this")
                    if alias_this_val:
                        alias = str(alias_this_val)
            if table.args.get("db"):
                return None, None

            return full_table_name, alias

        result: list[str] = []
        tables_with_date_filter: set[str] = set()
        tables_without_date_filter: set[str] = set()
        queried_tables: dict[str, dict[str, Any]] = functions.get_queried_tables(ast, columns_dict, self.config)
        cte_list: list[str] = []
        if ast.find_all(exp.CTE):
            cte_list = [cte.alias for cte in ast.find_all(exp.CTE)]
        date_columns_not_clear: set[str] = set()
        rows_to_scan: int = 0
        tables: list[str] = []
        result_table_list: list[str] = []
        table_list: set[str] = set()
        if len(list(ast.find_all(exp.From))) > 0 or len(list(ast.find_all(exp.Join))) > 0:
            for t in list(ast.find_all(exp.From)) + list(ast.find_all(exp.Join)):
                for i in t.find_all(exp.Table):
                    full_table_name, alias = date_cte_names(i)
                    if full_table_name:
                        table_list.add(full_table_name)
                    if alias:
                        table_list.add(alias)
        if len(list(ast.find_all(exp.Where))) > 0 or len(list(ast.find_all(exp.Join))) > 0:
            for w in list(ast.find_all(exp.Where)) + list(ast.find_all(exp.Join)):
                if not w.find(exp.Unnest):
                    if (
                        len(
                            list(w.find_all(exp.Between))
                            + list(w.find_all(exp.GTE))
                            + list(w.find_all(exp.GT))
                            + list(w.find_all(exp.EQ))
                        )
                        > 0
                    ):
                        for d in (
                            list(w.find_all(exp.Between))
                            + list(w.find_all(exp.GTE))
                            + list(w.find_all(exp.GT))
                            + list(w.find_all(exp.EQ))
                        ):
                            if d.find(exp.Identifier) and d.args["this"].find(exp.Identifier):
                                case_check: str = d.args["this"].find(exp.Identifier).args.get("this").lower()
                                cast_expr = d.find(exp.Cast)
                                if cast_expr:
                                    cast_to = cast_expr.args.get("to")
                                    if cast_to:
                                        cast_this = cast_to.args.get("this")
                                        if cast_this:
                                            case_check = str(cast_this).lower()
                                if "date" in case_check or "time" in case_check or "partition" in case_check:
                                    if d.find(exp.Column):
                                        for c in d.find_all(exp.Column):
                                            column_name, table_name = functions.get_column_and_table_name_from_column(c)
                                            if column_name:
                                                if not table_name:
                                                    for t in queried_tables:
                                                        if t in table_list:
                                                            tables_with_date_filter.add(t)
                                                            break
                                                    else:
                                                        date_columns_not_clear.add(column_name)
                                                else:
                                                    if table_name in table_list:
                                                        tables_with_date_filter.add(table_name)
                                                    else:
                                                        for t in queried_tables:
                                                            if t in table_list:
                                                                tables_with_date_filter.add(t)
                                                                break
                                                        else:
                                                            date_columns_not_clear.add(column_name)
                                            else:
                                                date_columns_not_clear.add(column_name)
                            elif d.find(exp.Literal):
                                for l in d.find_all(exp.Literal):
                                    literal_val = l.args.get("this")
                                    if literal_val and isinstance(literal_val, str) and len(literal_val) > 9 and "-" in literal_val:
                                        for t in queried_tables:
                                            if t in table_list:
                                                tables_with_date_filter.add(t)
                                                break
        # Logic to determine tables without date filter
        for table in queried_tables:
            if table in table_list:
                rows_to_scan += queried_tables[table]["total_rows"]
                tables.append(table)
                if table not in tables_with_date_filter:
                    tables_without_date_filter.add(table)
                    result_table_list.append(queried_tables[table]["full_table_name"])

        result = result_table_list
        return len(result) > 0, result

    def check_unpartitioned_tables(
        self, ast: exp.Expression, columns_dict: dict[str, Any]
    ) -> tuple[bool, list[str]]:
        """
        Anti Pattern: Querying Unpartitioned Tables
        """
        result: list[str] = []
        queried_tables: dict[str, dict[str, Any]] = functions.get_queried_tables(ast, columns_dict, self.config)
        for table in queried_tables:
            if not queried_tables[table]["partitioned_column"]:
                result.append(queried_tables[table]["full_table_name"])
        return len(result) > 0, result

    def check_distinct_on_big_table(self, ast: exp.Expression, columns_dict: dict[str, Any]) -> bool:
        """
        Anti Pattern: Using DISTINCT on Big Tables
        """
        queried_tables: dict[str, dict[str, Any]] = functions.get_queried_tables(
            ast, columns_dict, self.config
        )
        if len(list(ast.find_all(exp.Distinct))) > 0:
            for d in ast.find_all(exp.Distinct):
                if d.parent:
                    parent_select = d.parent
                    if parent_select.args.get("from"):
                        from_stmt = parent_select.args.get("from")
                        if from_stmt and from_stmt.args.get("this"):
                            table_ref = from_stmt.args.get("this")
                            if table_ref and table_ref.args.get("this"):
                                table_name = str(table_ref.args.get("this"))
                                if table_name in queried_tables:
                                    if queried_tables[table_name]["total_rows"] >= self.config.distinct_function_row_count:
                                        return True
        return False

    def check_count_distinct_on_big_table(self, ast: exp.Expression, columns_dict: dict[str, Any]) -> bool:
        """
        Anti Pattern: Using COUNT DISTINCT on Big Tables
        """
        queried_tables: dict[str, dict[str, Any]] = functions.get_queried_tables(
            ast, columns_dict, self.config
        )
        if len(list(ast.find_all(exp.Count))) > 0:
            for c in ast.find_all(exp.Count):
                if c.find(exp.Distinct):
                    # Find the table being queried
                    current = c
                    while current and not isinstance(current, exp.Select):
                        current = current.parent
                    
                    if current and current.args.get("from"):
                        from_stmt = current.args.get("from")
                        if from_stmt and from_stmt.args.get("this"):
                            table_ref = from_stmt.args.get("this")
                            if table_ref and table_ref.args.get("this"):
                                table_name = str(table_ref.args.get("this"))
                                if table_name in queried_tables:
                                    if queried_tables[table_name]["total_rows"] >= self.config.distinct_function_row_count:
                                        return True
        return False


# For backwards compatibility, keep the old function-based interface
def check_select_star(ast: exp.Expression) -> bool:
    """Backwards compatibility wrapper for select_star check."""
    from .config import get_default_config
    checker = Antipatterns(get_default_config())
    return checker.check_select_star(ast)


def check_semi_join_without_aggregation(ast: exp.Expression) -> bool:
    """Backwards compatibility wrapper for semi_join_without_aggregation check."""
    from .config import get_default_config
    checker = Antipatterns(get_default_config())
    return checker.check_semi_join_without_aggregation(ast)


def check_order_without_limit(ast: exp.Expression) -> bool:
    """Backwards compatibility wrapper for order_without_limit check."""
    from .config import get_default_config
    checker = Antipatterns(get_default_config())
    return checker.check_order_without_limit(ast)


def check_regexp_in_where(ast: exp.Expression) -> bool:
    """Backwards compatibility wrapper for regexp_in_where check."""
    from .config import get_default_config
    checker = Antipatterns(get_default_config())
    return checker.check_regexp_in_where(ast)


def check_like_before_more_selective(ast: exp.Expression) -> bool:
    """Backwards compatibility wrapper for like_before_more_selective check."""
    from .config import get_default_config
    checker = Antipatterns(get_default_config())
    return checker.check_like_before_more_selective(ast)


def check_multiple_cte_reference(ast: exp.Expression) -> bool:
    """Backwards compatibility wrapper for multiple_cte_reference check."""
    from .config import get_default_config
    checker = Antipatterns(get_default_config())
    return checker.check_multiple_cte_reference(ast)


def check_partition_used(
    ast: exp.Expression, columns_dict: dict[str, Any]
) -> tuple[bool, list[dict[str, str]]]:
    """Backwards compatibility wrapper for partition_used check."""
    from .config import get_default_config
    checker = Antipatterns(get_default_config())
    return checker.check_partition_used(ast, columns_dict)


def check_big_date_range(ast: exp.Expression) -> bool:
    """Backwards compatibility wrapper for big_date_range check."""
    from .config import get_default_config
    checker = Antipatterns(get_default_config())
    return checker.check_big_date_range(ast)


def check_big_table_no_date(
    ast: exp.Expression, columns_dict: dict[str, Any]
) -> tuple[bool, list[str]]:
    """Backwards compatibility wrapper for big_table_no_date check."""
    from .config import get_default_config
    checker = Antipatterns(get_default_config())
    return checker.check_big_table_no_date(ast, columns_dict)


def check_unpartitioned_tables(
    ast: exp.Expression, columns_dict: dict[str, Any]
) -> tuple[bool, list[str]]:
    """Backwards compatibility wrapper for unpartitioned_tables check."""
    from .config import get_default_config
    checker = Antipatterns(get_default_config())
    return checker.check_unpartitioned_tables(ast, columns_dict)


def check_distinct_on_big_table(ast: exp.Expression, columns_dict: dict[str, Any]) -> bool:
    """Backwards compatibility wrapper for distinct_on_big_table check."""
    from .config import get_default_config
    checker = Antipatterns(get_default_config())
    return checker.check_distinct_on_big_table(ast, columns_dict)


def check_count_distinct_on_big_table(ast: exp.Expression, columns_dict: dict[str, Any]) -> bool:
    """Backwards compatibility wrapper for count_distinct_on_big_table check."""
    from .config import get_default_config
    checker = Antipatterns(get_default_config())
    return checker.check_count_distinct_on_big_table(ast, columns_dict)