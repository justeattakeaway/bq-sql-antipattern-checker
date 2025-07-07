"""Helper functions for BigQuery SQL Antipattern Checker.

This module contains utility functions for:
- Connecting to BigQuery and executing queries
- Retrieving job and table metadata from INFORMATION_SCHEMA
- Processing SQL ASTs to extract table and column information
- Formatting and uploading results to BigQuery

The functions support the core antipattern detection workflow by providing
data access and manipulation capabilities.

Example:
    Get column information for large tables:

    >>> config = Config.from_env()
    >>> columns_dict = get_columns_dict(config)
    >>> print(f"Found {len(columns_dict)} large tables")

    Upload results to BigQuery:

    >>> push_df_to_bq(results_df, config)
"""

from pathlib import Path
from typing import Any

import pandas as pd
from google.cloud import bigquery
from jinja2 import Template
from sqlglot import exp

from bq_sql_antipattern_checker.config import Config


def get_client(config: Config) -> bigquery.Client:
    """Get a BigQuery client instance.

    Args:
        config: Configuration object containing BigQuery settings

    Returns:
        bigquery.Client: Authenticated BigQuery client for the configured project.
    """
    return bigquery.Client(project=config.bigquery_job_project)


def get_jobs_dict(
    config: Config, limit_row: int | None, cumul_perc: float | None
) -> dict[str, Any]:
    """Retrieve BigQuery jobs for a specific date and project.

    Queries INFORMATION_SCHEMA.JOBS to get job metadata including SQL statements
    for analysis. Uses a Jinja2 template to construct the query.

    Args:
        config: Configuration object containing BigQuery settings
        limit_row: limit on the number of rows to return (None for no limit)
        cumul_perc: limit number of jobs to top costing ones by their cumulative percentage of cost

    Returns:
        dict: Dictionary of jobs indexed by job ID, containing job metadata
    """
    jobs_query_raw_template_path = Path(__file__).parent / "templates" / "jobs_query_raw.sql.j2"
    jobs_query_template_path = Path(__file__).parent / "templates" / "jobs_query.sql.j2"
    with open(jobs_query_raw_template_path) as file_:
        template = Template(file_.read())
    jobs_query_raw_ = template.render()
    with open(jobs_query_template_path) as file_:
        template = Template(file_.read())
    jobs_query = template.render(limit_row=limit_row)

    # Generate queries for each query project and join with UNION ALL
    jobs_raw_queries = []
    for query_project in config.query_project:
        jobs_query_raw = jobs_query_raw_.format(
            region=config.bigquery_region,
            date=config.date_values["query_run_date_str"],
            query_project=query_project,
            bigquery_region=config.bigquery_region,
            cumul_perc=cumul_perc,
        )
        jobs_raw_queries.append(jobs_query_raw)

    final_jobs_query_raw = "\n UNION ALL \n".join(jobs_raw_queries)
    jobs_query = jobs_query.format(
        jobs_query_raw=final_jobs_query_raw,
        cumul_perc=cumul_perc
        )
    query_job = get_client(config).query(jobs_query)
    if query_job.result():
        jobs_df = query_job.to_dataframe()
        jobs_dict: dict = jobs_df.to_dict("index")
        return jobs_dict
    else:
        return {}


def get_columns_dict(config: Config) -> dict[str, Any]:
    """Retrieve column and table metadata for large tables.

    Queries INFORMATION_SCHEMA to get table metadata including row counts,
    partition information, and datetime columns for tables exceeding the
    specified size threshold.

    Args:
        config: Configuration object containing BigQuery settings

    Returns:
        dict: Dictionary of table metadata indexed by full table name

    TODO: Add required BigQuery job labels
    """
    column_template_path = Path(__file__).parent / "templates" / "metadata_column_info.sql.j2"
    row_count_template_path = Path(__file__).parent / "templates" / "metadata_row_count.sql.j2"
    information_schema_template_path = Path(__file__).parent / "templates" / "information_schema_query.sql.j2"
    with open(column_template_path) as file_:
        template = Template(file_.read())
    column_query_= template.render()
    with open(row_count_template_path) as file_:
        template = Template(file_.read())
    row_count_query_ = template.render()
    with open(information_schema_template_path) as file_:
        template = Template(file_.read())
    information_schema_query = template.render()

    # Generate queries for each information_schema_project and join with UNION ALL
    column_queries = []
    row_count_queries = []
    for information_schema_project in config.information_schema_project:
        column_query = column_query_.format(
            information_schema_project=information_schema_project,
            bigquery_region=config.bigquery_region,
            large_table_row_count=config.large_table_row_count,
        )
        column_queries.append(column_query)

        row_count_query = row_count_query_.format(
            information_schema_project=information_schema_project,
            bigquery_region=config.bigquery_region,
            large_table_row_count=config.large_table_row_count,
        )
        row_count_queries.append(row_count_query)

    metadata_column_query = "\n UNION ALL \n".join(column_queries)
    metadata_row_count_query = "\n UNION ALL \n".join(row_count_queries)

    information_schema_query = information_schema_query.format(
        metadata_row_count_query=metadata_row_count_query,
        metadata_column_query=metadata_column_query
    )

    query_job = get_client(config).query(information_schema_query)
    if query_job.result():
        columns_df = query_job.to_dataframe()
        columns_dict: dict[str, Any] = columns_df.set_index("full_table_name").to_dict("index")
        return columns_dict
    else:
        return {}


def get_queried_tables(
    ast: exp.Expression, columns_dict: dict[str, Any], row_count: int
) -> dict[str, dict[str, Any]]:
    """Extract queried table information from SQL AST.

    Analyzes the SQL AST to find all tables referenced in FROM and JOIN clauses,
    matching them against the provided columns dictionary to get metadata.

    Args:
        ast: SQLGlot AST representing the parsed SQL query
        columns_dict: Dictionary of table metadata from get_columns_dict()
        row_count: Minimum row count for a table to be considered

    Returns:
        dict: Dictionary of queried table metadata with aliases resolved
    """
    queried_tables = {}
    if len(list(ast.find_all(exp.Join))) > 0 or len(list(ast.find_all(exp.From))) > 0:
        for c in list(ast.find_all(exp.Join)) + list(ast.find_all(exp.From)):
            for t in c.find_all(exp.Table):
                if t.args.get("db"):
                    full_table_name, alias = get_alias_and_table_name_from_table(t)
                    if "*" in full_table_name:
                        table_list = [
                            k for k in columns_dict.keys() if full_table_name.replace("*", "") in k
                        ]
                    else:
                        table_list = [k for k in columns_dict.keys() if full_table_name == k]
                    if len(table_list) > 0:
                        table_list.sort()
                        total_rows = 0
                        for k in table_list:
                            total_rows += columns_dict[k]["total_rows"]
                            partitioned_column = columns_dict[k].get("partitioned_column")
                            available_datetime_columns = len(
                                columns_dict[k].get("datetime_columns")
                            )
                            available_datetime_columns_list = columns_dict[k].get(
                                "datetime_columns"
                            )
                            table = columns_dict[k].get("table")
                        if full_table_name not in queried_tables and total_rows >= row_count:
                            queried_tables[full_table_name] = {
                                "full_table_name": full_table_name,
                                "total_rows": total_rows,
                                "partitioned_column": partitioned_column,
                                "available_datetime_columns": available_datetime_columns,
                                "available_datetime_columns_list": available_datetime_columns_list,
                                "is_alias": False,
                                "table": table,
                            }
                        if alias:
                            if alias not in queried_tables and total_rows >= row_count:
                                queried_tables[alias] = {
                                    "full_table_name": full_table_name,
                                    "total_rows": total_rows,
                                    "partitioned_column": partitioned_column,
                                    "available_datetime_columns": available_datetime_columns,
                                    "available_datetime_columns_list": available_datetime_columns_list,
                                    "is_alias": True,
                                    "table": table,
                                }
    return queried_tables


def get_alias_and_table_name_from_table(table: exp.Table) -> tuple[str | None, str | None]:
    """Extract table name and alias from SQLGlot Table node.

    Args:
        table: SQLGlot Table expression node

    Returns:
        tuple: (full_table_name, alias) where alias may be None
    """
    full_table_name = None
    alias = None
    if table.args.get("db"):
        table_name = table.args.get("this").args.get("this")
        full_table_name = str(table_name)
        if table.args.get("db"):
            dataset = table.args.get("db").args.get("this")
            full_table_name = dataset + "." + full_table_name
        if table.args.get("catalog"):
            project = table.args.get("catalog").args.get("this")
            full_table_name = project + "." + full_table_name
        alias = (
            table.args.get("alias").args.get("this").args.get("this")
            if table.args.get("alias")
            else None
        )

    return full_table_name, alias


def get_column_and_table_name_from_column(column: exp.Column) -> tuple[str | None, str | None]:
    """Extract column name and table name from SQLGlot Column node.

    Args:
        column: SQLGlot Column expression node

    Returns:
        tuple: (column_name, table_name) where table_name may be None
    """
    this_ref = column.args.get("this")
    column_name = this_ref.args.get("this") if this_ref else None
    table_name = None

    table_ref = column.args.get("table")
    if table_ref:
        table_name_val = table_ref.args.get("this")
        if table_name_val:
            table_name = str(table_name_val)

        db_ref = column.args.get("db")
        catalog_ref = column.args.get("catalog")

        if db_ref and not catalog_ref:
            db_val = db_ref.args.get("this")
            if db_val:
                table_name = str(db_val)

        if catalog_ref:
            catalog_val = catalog_ref.args.get("this")
            db_val = db_ref.args.get("this") if db_ref else None
            table_val = table_ref.args.get("this")

            if catalog_val and db_val and table_val:
                table_name = f"{catalog_val}.{db_val}.{table_val}"

    return column_name, table_name


def get_partitioned_tables(
    ast: exp.Expression, columns_dict: dict[str, Any]
) -> dict[str, dict[str, Any]]:
    """Find partitioned tables referenced in the SQL query.

    Identifies tables with partition columns that are referenced in the query,
    tracking both full table names and aliases.

    Args:
        ast: SQLGlot AST representing the parsed SQL query
        columns_dict: Dictionary of table metadata including partition info

    Returns:
        dict: Dictionary of partitioned tables with metadata
    """
    used_tables_with_partition = {}
    partitioned_tables = [k for k in columns_dict.keys() if columns_dict[k]["partitioned_column"]]
    if len(list(ast.find_all(exp.From)) + list(ast.find_all(exp.Join))) > 0:
        for i in list(ast.find_all(exp.From)) + list(ast.find_all(exp.Join)):
            for t in i.find_all(exp.Table):
                if t.args.get("db"):
                    full_table_name, alias = get_alias_and_table_name_from_table(t)
                    for k in partitioned_tables:
                        if full_table_name and "*" in full_table_name:
                            if full_table_name.replace("*", "") in k:
                                if full_table_name not in used_tables_with_partition:
                                    used_tables_with_partition[full_table_name] = {
                                        "full_table_name": full_table_name,
                                        "qualified": True,
                                        "alias": alias,
                                        "partition_column": columns_dict[k]["partitioned_column"],
                                    }
                                if alias:
                                    if alias not in used_tables_with_partition:
                                        used_tables_with_partition[alias] = {
                                            "full_table_name": full_table_name,
                                            "qualified": False,
                                            "alias": alias,
                                            "partition_column": columns_dict[k][
                                                "partitioned_column"
                                            ],
                                        }
                                break
                        elif full_table_name and full_table_name == k:
                            if full_table_name not in used_tables_with_partition:
                                used_tables_with_partition[full_table_name] = {
                                    "full_table_name": full_table_name,
                                    "qualified": True,
                                    "alias": alias,
                                    "partition_column": columns_dict[k]["partitioned_column"],
                                }
                            if alias:
                                if alias not in used_tables_with_partition:
                                    used_tables_with_partition[alias] = {
                                        "full_table_name": full_table_name,
                                        "qualified": False,
                                        "alias": alias,
                                        "partition_column": columns_dict[k]["partitioned_column"],
                                    }
    return used_tables_with_partition


def get_output_df(output: dict[str, Any], index_value: str) -> pd.DataFrame:
    """Convert job output dictionary to pandas DataFrame.

    Args:
        output: Dictionary of job results keyed by job ID
        index_value: Column name for the index (typically 'job_id')

    Returns:
        pd.DataFrame: DataFrame with job results and proper index
    """

    output_df = pd.DataFrame.from_dict(output, orient="index")
    output_df = output_df.reset_index().rename(columns={"index": index_value})
    return output_df


def push_df_to_bq(df: pd.DataFrame, config: Config) -> None:
    """Upload DataFrame to BigQuery table.

    Uploads the results DataFrame to a partitioned BigQuery table,
    using date-based partitioning for efficient querying.

    Args:
        df: pandas DataFrame containing the results
        config: Configuration object containing BigQuery settings
    """
    table_id = "{dataset_project}.{dataset_name}.{table_name}${date}".format(
        dataset_project=config.bigquery_dataset_project,
        dataset_name=config.bigquery_dataset,
        table_name=config.results_table_name,
        date=config.date_values["partition_date"],
    )
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="creation_date",  # field to use for partitioning
        ),
    )
    job = get_client(config).load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.

    job.result()  # Wait

    bq_table = get_client(config).get_table(table_id)  # Make an API request.
    print(f"Loaded {bq_table.num_rows} rows and {len(bq_table.schema)} columns to {table_id}")
