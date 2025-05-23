from sqlglot import exp
import datetime as dt
import functions


def check_select_star(ast):
    """Anti Pattern: Selecting all columns
    avoids select * from CTEs or sub queries.
    only looks at a direct from statement where there's dataset value in source table
    ignores cases where no select is found like "update set from"
    """
    from_statements = ast.find_all(exp.From)

    for f in from_statements:
        if f.parent_select:
            if f.parent_select.find(exp.Star) and not f.parent_select.find(exp.Count):
                if f.args.get('this').args.get('db'):
                    return True
    return False


def check_semi_join_without_aggregation(ast):
    """
    # Anti Pattern: SEMI-JOIN without aggregation
    if where clause as has a 'value in (select x from) like subquery statement
    it's best practice to select distinct or group by that value in that subquery
    """
    where_statements = ast.find_all(exp.Where)

    for w in where_statements:
        if "IN" in str(w):
            for s in w.find_all(exp.Select):
                if len(list(s.find_all(exp.Distinct))) + len(list(s.find_all(exp.Group))) == 0:
                    return True
    return False


def check_order_without_limit(ast):
    """
    Anti Pattern: Using ORDER BY without LIMIT
    queries using order by in the select statement without a limit.
    """ 
    if ast.args.get('order') and not ast.args.get('limit'):
        return True

    return False


def check_regexp_in_where(ast):
    """
    Anti Pattern: Using REGEXP when LIKE is an option
    like is more performant than regexp functions
    """
    where_statement = ast.args.get('where')

    if not where_statement:
        return False

    regex_rules = [exp.RegexpLike, exp.RegexpReplace, exp.RegexpExtract]

    return any(where_statement.find(regex) for regex in regex_rules)


def check_like_before_more_selective(ast):
    """
    Anti Pattern: Where order, apply most selective expression first it's checking whether there are cases like
    "Like" or "Regexp Like, Contains" before a more selective statement like IN, EQ,GTE,LTE etc. BQ likes to have more
    selective statements first At the moment until we figure out a way to sort the hierarchy of statements,
    it's searching for string conditions
    TODO: can benefit from tidying up
    """
    where_statement = ast.args.get('where')

    if not where_statement:
        return False

    if where_statement.find(exp.Like) or where_statement.find(exp.RegexpLike) \
            or where_statement.find(exp.RegexpReplace) or where_statement.find(exp.RegexpExtract):

        string_version = repr(where_statement.args).replace(' ', '').replace('\n', '')
        one_eq_one_starter = string_version.find(
            "EQ(this=Literal(this=1,is_string=False),expression=Literal(this=1")
        bool_starter = string_version.find("this=Boolean(this=True)")

        index_starter = max(one_eq_one_starter + bool_starter, 0)
        less_selective_clause_index = -1
        like_clause = string_version[index_starter:].find("Like")
        regexp_clause = string_version[index_starter:].find("Regexp")

        if like_clause > -1:
            if regexp_clause > -1:
                less_selective_clause_index = min(like_clause, less_selective_clause_index)
            else:
                less_selective_clause_index = like_clause

        more_selective_clauses = []
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

def check_multiple_cte_reference(ast):
    """
    Anti Pattern: Referencing Same Table Multiple Times
    """
    status = False
    cte_values = {cte.alias: 0 for cte in ast.find_all(exp.CTE) if len(list(cte.find_all(exp.From))) > 0}
    if len(cte_values) > 0:
        for cte in ast.find_all(exp.CTE):
            for s in cte.find_all(exp.Select):
                from_statement = s.args.get('from')
                if not from_statement:
                    continue

                from_table = str(from_statement.args.get('this'))
                if from_table in cte_values:
                    cte_values[from_table] += 1
                    if cte_values[from_table] == 2:
                        status = True

    return status


def check_partition_used(ast, columns_dict):
    """
    Anti Pattern: Table not using partitions
    check if the query is using tables with partition
    check if the query is referring to those partitions in join or where clause
    """
    used_tables_with_partition = functions.get_partitioned_tables(ast, columns_dict)
    result = []
    passed = []
    if len(used_tables_with_partition) > 0:
        tables_with_partitions_used = set()
        columns_from_join = []
        columns_from_where = []
        if ast.find_all(exp.Join):
            for j in ast.find_all(exp.Join):
                columns_from_join += j.find_all(exp.Column)
        if ast.find_all(exp.Where):
            for w in ast.find_all(exp.Where):
                columns_from_where += w.find_all(exp.Column)
        for c in columns_from_join + columns_from_where:
            column_name, table_name = functions.get_column_and_table_name_from_column(c)
            if column_name:
                if table_name:
                    if table_name.replace('*', '') in used_tables_with_partition:
                        if column_name.lower() == used_tables_with_partition[table_name]['partition_column'].lower():
                            tables_with_partitions_used.add(used_tables_with_partition[table_name]['full_table_name'])
                if not table_name:
                    for t in used_tables_with_partition.keys():

                        if used_tables_with_partition[t]['partition_column']:
                            if column_name.lower() == used_tables_with_partition[t]['partition_column'].lower():
                                tables_with_partitions_used.add(used_tables_with_partition[t]['full_table_name'])
                                break
                else:
                    # case for columns used without a fully qualified table name or alias. bad practice
                    for k, v in used_tables_with_partition.items():
                        if column_name.lower() == used_tables_with_partition[k]['partition_column'].lower():
                            tables_with_partitions_used.add(used_tables_with_partition[k]['full_table_name'])
        for k, v in used_tables_with_partition.items():
            if v['full_table_name'] not in list(tables_with_partitions_used) and v['full_table_name'] not in passed:
                result.append({'table_name': v['full_table_name'], 'partitioned_column': v['partition_column']})
                passed.append(v['full_table_name'])
    return len(result) > 0, result


def check_big_date_range(ast):
    """
    Anti Pattern: Long Date Range
    check if the query is using date range that is more than a year
    """
    date_range = []
    status = False
    days = {'DAY': 1, 'WEEK': 7, 'MONTH': 30, 'YEAR': 365,
            'MINUTE': 0.0007, 'HOUR': 0.04, 'QUARTER': 90,
            'SECOND': 0.00001166666667
            }
    if len(list(ast.find_all(exp.Where)) + list(ast.find_all(exp.Join))) > 0:
        for w in list(ast.find_all(exp.Where)) + list(ast.find_all(exp.Join)):
            for d in list(w.find_all(exp.Between)) + list(w.find_all(exp.GTE)) + list(w.find_all(exp.GT)):
                if d.args['this'].find(exp.Identifier):
                    case_check = d.args['this'].find(exp.Identifier).args.get('this').lower()
                    if d.find(exp.Cast):
                        case_check = str(d.find(exp.Cast).args.get('to').args.get('this')).lower()
                    if 'date' in case_check or 'time' in case_check or 'partition' in case_check:
                        date_diff = None
                        if d.find(exp.DateSub) or d.find(exp.Sub) or (d.find(exp.Neg) and d.find(exp.DateAdd)):
                            for i in list(d.find_all(exp.DateSub)) + list(d.find_all(exp.Sub)) + list(
                                    d.find_all(exp.DateAdd)):
                                if i.find(exp.Literal):
                                    for j in i.find_all(exp.Literal):
                                        if str(j.args.get('this')).isnumeric():
                                            length = int(j.args.get('this'))
                                            multiplier = 1
                                            if i.args.get('unit'):
                                                multiplier = days[i.args.get('unit').args.get('this')]
                                            if i.find(exp.Var):
                                                multiplier = days[i.find(exp.Var).args['this']]
                                            elif i.find(exp.Mul):
                                                multiplier = int(i.find(exp.Mul).args['expression'].args['this'])
                                            date_diff = length * multiplier
                        elif d.args.get('low'):
                            date_exp = str(d.args.get('low').args.get('this')).replace("'", "")
                            if len(str(date_exp)) > 9 and '-' in str(date_exp):
                                date_conv = dt.datetime.strptime(date_exp[:10], '%Y-%m-%d')
                                date_diff = (dt.datetime.now() - date_conv).days
                        elif d.args['expression'].find(exp.Literal):
                            date_exp = d.args['expression'].find(exp.Literal).args.get('this')
                            if len(str(date_exp)) > 9 and '-' in str(date_exp):
                                date_conv = dt.datetime.strptime(date_exp[:10], '%Y-%m-%d')
                                date_diff = (dt.datetime.now() - date_conv).days
                        if date_diff:
                            if date_diff > 365:
                                status = True
    return status
 

def check_big_table_no_date(ast, columns_dict):
    """
    Anti Pattern: Big Tables With No Date Filter
    TODO: can benefit from tidying up
    """

    def date_cte_names(table):
        # need a way to find ctes used for date filteration
        full_table_name = None
        alias = None
        full_table_name = str(table.args.get('this').args.get('this'))

        alias = table.args.get('alias').args.get('this').args.get('this') if table.args.get('alias') else None
        if table.args.get('db'):
            return None, None

        return full_table_name, alias

    result = []
    tables_with_date_filter = set()
    tables_without_date_filter = set()
    queried_tables = functions.get_queried_tables(ast, columns_dict)
    cte_list = []
    if ast.find_all(exp.CTE):
        cte_list = [cte.alias for cte in ast.find_all(exp.CTE)]
    date_columns_not_clear = set()
    rows_to_scan = 0
    tables = []
    result_table_list = []
    table_list = set() 
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
                if len(list(w.find_all(exp.Between)) + list(w.find_all(exp.GTE)) + list(w.find_all(exp.GT)) + list(
                        w.find_all(exp.EQ))) > 0:
                    for d in list(w.find_all(exp.Between)) + list(w.find_all(exp.GTE)) + list(
                            w.find_all(exp.GT)) + list(
                            w.find_all(exp.EQ)):
                        if d.find(exp.Identifier) and d.args['this'].find(exp.Identifier):
                            case_check = d.args['this'].find(exp.Identifier).args.get('this').lower()

                            if 'date' in case_check or 'time' in case_check or 'partition' in case_check:

                                if len(list(d.find_all(exp.Column))) > 1:
                                    # if there are two columns being compared in a date function that's not necessarily a limiting date condition
                                    for c in d.find_all(exp.Column):
                                        column_name, table_name = functions.get_column_and_table_name_from_column(c)
                                        if c.parent_select:
                                            if c.parent_select.find(exp.CTE) or table_name in list(
                                                    table_list) or table_name in cte_list:
                                                for c2 in d.find_all(exp.Column):
                                                    column_name, table_name = functions.get_column_and_table_name_from_column(
                                                        c2)
                                                    if table_name in queried_tables:
                                                        tables_with_date_filter.add(table_name)
                                                        tables_with_date_filter.add(
                                                            queried_tables[table_name]['full_table_name'])
                                                        break
                                            elif len(list(d.parent_select.find_all(exp.Table))) > 0:
                                                for t in d.parent_select.find_all(exp.Table):
                                                    table_name = str(t.args.get('this').args.get('this'))
                                                    if t.args.get('db') and not t.args.get('catalog'):
                                                        table_name = t.args.get('db').args.get('this')
                                                    if t.args.get('catalog'):
                                                        table_name = t.args.get('catalog').args.get(
                                                            'this') + '.' + t.args.get(
                                                            'db').args.get('this') + '.' + table_name
                                                        if table_name in queried_tables:
                                                            if column_name in queried_tables[table_name][
                                                                'available_datetime_columns_list']:
                                                                tables_with_date_filter.add(table_name)
                                                            else:
                                                                tables_without_date_filter.add(table_name)
                                                    if t.args.get('alias'):
                                                        alias = str(t.args.get('alias').args.get('this'))
                                                        if alias in queried_tables:
                                                            if column_name in queried_tables[alias][
                                                                'available_datetime_columns_list']:
                                                                tables_with_date_filter.add(alias)
                                                                tables_with_date_filter.add(
                                                                    queried_tables[alias]['full_table_name'])
                                                            else:
                                                                tables_without_date_filter.add(alias)
                                                                tables_without_date_filter.add(
                                                                    queried_tables[alias]['full_table_name'])
                                            elif not d.find(exp.Column).args.get('table'):
                                                column = d.find(exp.Column)
                                                date_columns_not_clear.add(
                                                    str(column.args.get('this').args.get('this')))
                                            else:
                                                column = d.find(exp.Column)

                                                table_name = column.args.get('table').args.get('this')
                                                if column.args.get('db') and not column.args.get('catalog'):
                                                    table_name = column.args.get('db').args.get('this')
                                                if column.args.get('catalog'):
                                                    table_name = column.args.get('catalog').args.get(
                                                        'this') + '.' + column.args.get('db').args.get(
                                                        'this') + '.' + column.args.get('table').args.get('this')
                                                if table_name in queried_tables:
                                                    tables_without_date_filter.add(table_name)
                                                    tables_without_date_filter.add(
                                                        queried_tables[table_name]['full_table_name'])
                                else:
                                    c = d.find(exp.Column)
                                    column = c.args.get('this').args.get('this')
                                    if c.args.get('table'):
                                        column_name, table_name = functions.get_column_and_table_name_from_column(c)
                                        if table_name in queried_tables:
                                            if column in queried_tables[table_name]['available_datetime_columns_list']:
                                                tables_with_date_filter.add(table_name)
                                                tables_with_date_filter.add(
                                                    queried_tables[table_name]['full_table_name'])
                                    elif d.parent_select:
                                        alias = None
                                        if len(list(d.parent_select.find_all(exp.Table))) > 0:
                                            for t in d.parent_select.find_all(exp.Table):
                                                table_name = str(t.args.get('this').args.get('this'))
                                                if t.args.get('db') and not t.args.get('catalog'):
                                                    table_name = t.args.get('db').args.get('this')
                                                if t.args.get('catalog'):
                                                    table_name = t.args.get('catalog').args.get(
                                                        'this') + '.' + t.args.get(
                                                        'db').args.get('this') + '.' + table_name
                                                    if table_name in queried_tables:
                                                        if column in queried_tables[table_name][
                                                            'available_datetime_columns_list']:
                                                            tables_with_date_filter.add(table_name)
                                                        else:
                                                            tables_without_date_filter.add(table_name)
                                                if t.args.get('alias'):
                                                    alias = str(t.args.get('alias').args.get('this'))
                                                    if alias in queried_tables:
                                                        if column in queried_tables[alias][
                                                            'available_datetime_columns_list']:
                                                            tables_with_date_filter.add(alias)
                                                            tables_with_date_filter.add(
                                                                queried_tables[alias]['full_table_name'])
                                                        else:
                                                            tables_without_date_filter.add(alias)
                                                            tables_without_date_filter.add(
                                                                queried_tables[alias]['full_table_name'])
    result_table_list = set(list(tables_without_date_filter - tables_with_date_filter) + list(
        set(list(queried_tables)) - tables_with_date_filter))
    if len(result_table_list) > 0:
        for t in result_table_list:
            if not (queried_tables[t]['is_alias']):
                if not date_columns_not_clear & set(queried_tables[t]['available_datetime_columns_list']):
                    if 'dim_' not in queried_tables[t]['table']:
                        tables.append(t)
    return len(tables) > 0, tables


def check_unpartitioned_tables(ast, columns_dict):
    """
    Anti Pattern: Big Tables With No Partitioned Columns 
    Ignores dim tables with a prefix as an example. Can be changed based on the environment. 
    """
    queried_tables = functions.get_queried_tables(ast, columns_dict)
    result = []
    if len(queried_tables) > 0:
        for k, v in queried_tables.items():
            if not v['partitioned_column'] and not v['is_alias'] and 'dim_' not in v['table']:
                result.append(k)
    return len(result) > 0, result


def check_distinct_on_big_table(ast, columns_dict):
    """
    Anti Pattern: Using Distinct on Big Tables
    """
    queried_tables = functions.get_queried_tables(ast, columns_dict, config.distinct_function_row_count)
    if len(queried_tables) > 0:
        if ast.find(exp.Select).find(exp.Distinct):
            return True
            
    return False

def check_count_distinct_on_big_table(ast, columns_dict):
    """
    Anti Pattern: Count Distinct on Large Tables
    """
    queried_tables = functions.get_queried_tables(ast, columns_dict, config.distinct_function_row_count)
    if len(queried_tables) > 0: 
        if ast.find(exp.Count).find(exp.Distinct):
            return True
    return False
