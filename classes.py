from datetime import datetime
import sqlparse
from sqlglot import parse_one, exp
import antipatterns


class Job:
    def __init__(self, v):
        self.creation_date = v['creation_date']
        self.creation_time = v['creation_time'].strftime("%Y-%m-%d %H:%M:%S")
        self.project_id = v['project_id']
        self.user_email = v['user_email']
        self.reservation_id = v['reservation_id']
        self.total_process_gb = v['total_process_gb']
        self.total_slot_hrs = v['total_slot_hrs']
        self.total_duration_mins = v['total_duration_mins']
        self.query = v['query']
        self.partition_not_used = False
        self.available_partitions = []
        self.big_date_range = False
        self.no_date_on_big_table = False
        self.tables_without_date_filter = []
        self.select_star = False
        self.references_cte_multiple_times = False
        self.semi_join_without_aggregation = False
        self.order_without_limit = False
        self.like_before_more_selective = False
        self.regexp_in_where = False
        self.distinct_and_group = False
        self.queries_unpartitioned_table = False
        self.unpartitioned_tables = []
        self.distinct_on_big_table = False
        self.count_distinct_on_big_table = False

        self.antipattern_run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def get_statements(self):
        statements = sqlparse.split(self.query)
        return statements

    def check_antipatterns(self, columns_dict):
        statements = self.get_statements()
        for i in statements:
            try:
                if 'declare' not in i.lower():
                    ast = parse_one(i, dialect="bigquery")

                    if not ast.find(exp.UserDefinedFunction) and not ast.find(exp.SetItem):
                        partition_not_used, available_partitions = antipatterns.check_partition_used(ast, columns_dict)
                        if partition_not_used:
                            self.partition_not_used = partition_not_used
                            self.available_partitions += available_partitions
                            self.available_partitions = [dict(t) for t in {tuple(d.items()) for d in
                                                                           self.available_partitions}]

                        big_date_range = antipatterns.check_big_date_range(ast)

                        no_date_on_big_table, tables_without_date_filter = antipatterns.check_big_table_no_date(
                            ast, columns_dict)

                        if no_date_on_big_table:
                            self.no_date_on_big_table = no_date_on_big_table
                            self.tables_without_date_filter += tables_without_date_filter

                        #A job can have multiple SELECT statements executed. One case is enough to flag as True, hence max function
                        self.select_star = max(antipatterns.check_select_star(ast), self.select_star)

                        references_cte_multiple_times = antipatterns.check_multiple_cte_reference(
                            ast)
                        self.references_cte_multiple_times = max(references_cte_multiple_times,
                                                                 self.references_cte_multiple_times)

                        self.semi_join_without_aggregation = max(antipatterns.check_semi_join_without_aggregation(ast),
                                                                 self.semi_join_without_aggregation)

                        self.order_without_limit = max(antipatterns.check_order_without_limit(ast),
                                                       self.order_without_limit)

                        self.like_before_more_selective = max(antipatterns.check_like_before_more_selective(ast),
                                                              self.like_before_more_selective)

                        self.regexp_in_where = max(antipatterns.check_regexp_in_where(ast), self.regexp_in_where)

                        self.distinct_and_group = max(antipatterns.check_distinct_and_group(ast),
                                                      self.distinct_and_group)

                        queries_unpartitioned_table, unpartitioned_tables = antipatterns.check_unpartitioned_tables(ast,
                                                                                                                    columns_dict)
                        self.queries_unpartitioned_table = max(queries_unpartitioned_table,
                                                               self.queries_unpartitioned_table)

                        self.unpartitioned_tables += unpartitioned_tables

                        self.distinct_on_big_table = max(
                            antipatterns.check_distinct_on_big_table(ast, columns_dict),
                            self.distinct_on_big_table)

                        self.count_distinct_on_big_table = max(
                            antipatterns.check_count_distinct_on_big_table(ast, columns_dict),
                            self.count_distinct_on_big_table)

            except Exception as e:
                print(str(e))
