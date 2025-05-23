import pandas as pd
import os
from pathlib import Path

from config import date_values, bigquery_job_project, large_table_row_count
from sqlglot import exp
from google.cloud import bigquery
from jinja2 import Template

template_root_folder = Path(os.path.dirname(os.path.realpath(__file__)),
                            "../antipattern-checker/templates")


def get_client():
    return bigquery.Client(project=bigquery_job_project)


def get_jobs_dict(date, query_project, bigquery_region):
    # TODO: Add required BigQuery job labels

    with open(f'{template_root_folder}/jobs_query.sql.j2') as file_:
        template = Template(file_.read())
    query = template.render()
    jobs_query = query.format(
        region=bigquery_region,
        date=date,
        query_project=query_project,
        bigquery_region=bigquery_region
    )

    query_job = get_client().query(jobs_query)
    if query_job.result():
        jobs_df = query_job.to_dataframe()

    jobs_dict = jobs_df.to_dict('index')
    return jobs_dict


def get_columns_dict(bigquery_dataset_project, bigquery_region, large_table_row_count):
    # TODO: Add required BigQuery job labels
    with open(f'{template_root_folder}/columns_query.sql.j2') as file_:
        template = Template(file_.read())
    query = template.render()
    columns_query = query.format(bigquery_dataset_project=bigquery_dataset_project,
                                 bigquery_region=bigquery_region,
                                 large_table_row_count=large_table_row_count
                                )

    query_job = get_client().query(columns_query)
    if query_job.result():
        columns_df = query_job.to_dataframe()

    columns_dict = columns_df.set_index('full_table_name').to_dict('index')

    return columns_dict


def get_queried_tables(ast, columns_dict, row_count=large_table_row_count):
    queried_tables = {}
    if len(list(ast.find_all(exp.Join))) > 0 or len(list(ast.find_all(exp.From))) > 0:
        for c in list(ast.find_all(exp.Join)) + list(ast.find_all(exp.From)):
            for t in c.find_all(exp.Table):
                if t.args.get('db'):
                    full_table_name, alias = get_alias_and_table_name_from_table(t)
                    if '*' in full_table_name:
                        table_list = [k for k in columns_dict.keys() if full_table_name.replace('*', '') in k]
                    else:
                        table_list = [k for k in columns_dict.keys() if full_table_name == k]
                    if len(table_list) > 0:
                        table_list.sort()
                        total_rows = 0
                        for k in table_list:
                            total_rows += columns_dict[k]['total_rows']
                            partitioned_column = columns_dict[k].get('partitioned_column')
                            available_datetime_columns = len(columns_dict[k].get('datetime_columns'))
                            available_datetime_columns_list = columns_dict[k].get('datetime_columns')
                            table = columns_dict[k].get('table')
                        if full_table_name not in queried_tables and total_rows >= row_count:
                            queried_tables[full_table_name] = {
                                'full_table_name': full_table_name,
                                'total_rows': total_rows,
                                'partitioned_column': partitioned_column,
                                'available_datetime_columns': available_datetime_columns,
                                'available_datetime_columns_list': available_datetime_columns_list,
                                'is_alias': False,
                                'table': table
                            }
                        if alias:
                            if alias not in queried_tables and total_rows >= row_count:
                                queried_tables[alias] = {
                                    'full_table_name': full_table_name,
                                    'total_rows': total_rows,
                                    'partitioned_column': partitioned_column,
                                    'available_datetime_columns': available_datetime_columns,
                                    'available_datetime_columns_list': available_datetime_columns_list,
                                    'is_alias': True,
                                    'table': table
                                }
    return queried_tables


def get_alias_and_table_name_from_table(table):
    full_table_name = None
    alias = None
    if table.args.get('db'):
        table_name = table.args.get('this').args.get('this')
        full_table_name = str(table_name)
        if table.args.get('db'):
            dataset = table.args.get('db').args.get('this')
            full_table_name = dataset + '.' + full_table_name
        if table.args.get('catalog'):
            project = table.args.get('catalog').args.get('this')
            full_table_name = project + '.' + full_table_name
        alias = table.args.get('alias').args.get('this').args.get('this') if table.args.get('alias') else None

    return full_table_name, alias

def get_column_and_table_name_from_column(column):
    column_name = column.args.get('this').args.get('this')
    table_name = None
    if column.args.get('table'):
        table_name = column.args.get('table').args.get('this')
        if column.args.get('db') and not column.args.get('catalog'):
            table_name = column.args.get('db').args.get('this')
        if column.args.get('catalog'):
            dataset = column.args.get('db').args.get('this')
            table_name = column.args.get('catalog').args.get('this') + '.' + dataset + '.' + column.args.get(
                'table').args.get('this')

    return column_name, table_name
def get_partitioned_tables(ast, columns_dict):
    used_tables_with_partition = {}
    partitioned_tables = [k for k in columns_dict.keys() if columns_dict[k]['partitioned_column']]
    if len(list(ast.find_all(exp.From)) + list(ast.find_all(exp.Join))) > 0:
        for i in list(ast.find_all(exp.From)) + list(ast.find_all(exp.Join)):
            for t in i.find_all(exp.Table):
                if t.args.get('db'):
                    full_table_name, alias = get_alias_and_table_name_from_table(t)
                    for k in partitioned_tables:
                        if '*' in full_table_name:
                            if full_table_name.replace('*', '') in k:
                                if full_table_name not in used_tables_with_partition:
                                    used_tables_with_partition[full_table_name] = {
                                        'full_table_name': full_table_name,
                                        'qualified': True,
                                        'alias': alias,
                                        'partition_column': columns_dict[k]['partitioned_column']
                                    }
                                if alias:
                                    if alias not in used_tables_with_partition:
                                        used_tables_with_partition[alias] = {
                                            'full_table_name': full_table_name,
                                            'qualified': False,
                                            'alias': alias,
                                            'partition_column': columns_dict[k]['partitioned_column']
                                        }
                                break
                        elif full_table_name == k:
                            if full_table_name not in used_tables_with_partition:
                                used_tables_with_partition[full_table_name] = {
                                    'full_table_name': full_table_name,
                                    'qualified': True,
                                    'alias': alias,
                                    'partition_column': columns_dict[k]['partitioned_column']
                                }
                            if alias:
                                if alias not in used_tables_with_partition:
                                    used_tables_with_partition[alias] = {
                                        'full_table_name': full_table_name,
                                        'qualified': False,
                                        'alias': alias,
                                        'partition_column': columns_dict[k]['partitioned_column']
                                    }
    return used_tables_with_partition


def get_output_df(output, index_value):
    output_df = pd.DataFrame.from_dict(output, orient='index')
    output_df = output_df.reset_index().rename(columns={"index": index_value})
    return output_df


def push_df_to_bq(df, table, project, dataset):
    table_id = "{dataset_project}.{dataset_name}.{table_name}${date}".format(dataset_project=project,
                                                                             dataset_name=dataset,
                                                                             table_name=table,
                                                                             date=date_values['partition_date']
                                                                             )
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="creation_date",  # field to use for partitioning
        )
    )
    job = get_client().load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.

    job.result()  # Wait

    table = get_client().get_table(table_id)  # Make an API request.
    print(
        "Loaded {row_count} rows and {column_count} columns to {table_name}".format(
            row_count=table.num_rows, column_count=len(table.schema), table_name=table_id
        )
    )
