# BigQuery Antipattern Checker

## Introduction
SQL Antipattern is an in house made application to provide information about BQ jobs that contain antipattern cases that can be significant for optimisation both in terms of cost and runtimes.
It is used in entire JET including all production jobs, adhoc queries, dashboard extracts etc

## What is the purpose? How can it be used?
Can you justify the cost of their query or task running? It's a difficult question to answer. However if someone asks "Can this job run cheaper and faster?", that's a question you can answer with a lot more confidence. This application aims to give you a very clear recipe of actions to take control of your costs on BigQuery and also create reports of cost, optimisation gaps and progress. The application checks the most impactful query antipatterns and shows the results and locations of them per query. 

* You can use this application to check all the BigQuery jobs being executed in a specific project or your entire company. 
* It doesn't differentiate type of jobs, they can be adhoc queries, Connected Sheet tasks, DAGs//Tasks, Dashboard extracts etc anything to has a query statement.
* The application aims to provide optimisation gaps of those jobs with clear information of what is missing, what can be done on the query. If it's about specific fields missing or wrongly used or unoptimised tables it also gives clear information about them. 
* It also provides additional performance metrics and indicators to help with further optimising your query.
* You can use the information to tackle recurring costs on your DAGs due to lack of optimisation or show users the cost of their adhoc queries and what they can do to improve them next time.
* The application also produces an output table on job ID level along with the query project and all the necessary information that can be directly plugged into a dashboard tool of your choice for team / company level aggregations.

* At JET we usually assign a separate Query / Computation project to a team. If you have a similar approach you can use the project_id dimension of the output table to break things down by teams. You can add labels you have on your JOBS information_schema to have different levels of reports. 

## What is a SQL antipattern?
Antipatterns are specific SQL syntaxes that in some cases might cause performance impact and they are generally accepted as bad coding practice.

There are a variety of conditions that can be used in query which can create huge computational complexity unnecessarily and also impact the code readability and quality. 
 
## How Does It Work?
The application is a Python project that queries INFORMATION_SCHEMA get job metadata including the query executed.
After that the queries go through a series of functions using SQLGlot Parser to check antipatterns. 

Final output is pushed to a BQ table.

## How accurate is this?
Some of the antipatterns are a lot more straightforward to detect (like Select *) but some of them vary heavily depending on the style of the user writing that query and the functions used. We have tested extensively using JET queries and we are confident that it has significant coverage over the impactful antipatterns. Please also note that some edge cases of SQL syntaxes are not detectable by SQLGlot so do not expect 100% coverage. Our aim is not to achieve such coverage but to provide a platform to detect major issues with optimisation and provide actionable insights on them. 

## Development Information
Every function for antipattern check is developed from scratch by testing real queries used at JET.

We got inspiration from some of the antipatterns that can be found in this opensource repo 
[GoogleCloudPlatform/bigquery-antipattern-recognition](https://github.com/GoogleCloudPlatform/bigquery-antipattern-recognition)   
But we wrote those ones from scratch as well, we didn’t refer to their codes because it’s in Java, and we are using Python to make it more convenient and a different library to solve our problems.
We added more cases that are not present in that repo. 

## Owner
The application is owned by Platform Engineering under Platform Engineering at JET

## Code of Conduct
Please see [Code of Conduct Guide](https://github.com/justeattakeaway/bq-sql-antipattern-checker/blob/main/CODE_OF_CONDUCT.md)
If you don't play nice you'll be banned. 

## Installation

### Install via pip (recommended)

```bash
pip install bq-sql-antipattern-checker
```

### Install from source

```bash
# Clone the repository
git clone https://github.com/justeattakeaway/bq-sql-antipattern-checker.git
cd bq-sql-antipattern-checker

# Install with pip
pip install .

# Or install in development mode
pip install -e .
```

### Requirements

* Python >= 3.10
* Google Cloud CLI for authentication
* BigQuery project access with appropriate permissions

## Quick Start

1. **Install the package:**
   ```bash
   pip install bq-sql-antipattern-checker
   ```

2. **Set up authentication:**
   ```bash
   gcloud auth application-default login
   ```

3. **Create a configuration file:**
   ```bash
   bq-antipattern-checker create-config
   ```

4. **Edit the configuration file** (`antipattern-config.yaml`) with your project details.

5. **Run the antipattern checker:**
   ```bash
   # Test with a small sample first (recommended)
   bq-antipattern-checker run --config antipattern-config.yaml --dry-run --limit-row 10

   # Run with dry-run to save results locally
   bq-antipattern-checker run --config antipattern-config.yaml --dry-run

   # Run and push results to BigQuery
   bq-antipattern-checker run --config antipattern-config.yaml

   # Run with verbose output and limited rows
   bq-antipattern-checker run --config antipattern-config.yaml --verbose --limit-row 100
   ```

## Usage

The tool provides several commands to help you analyze your BigQuery jobs:

### Available Commands

* `run` - Execute antipattern analysis
* `list-antipatterns` - Display available antipatterns and their status
* `show-config` - Display current configuration settings
* `create-config` - Generate a default configuration file

### Command Examples

```bash
# Run antipattern analysis with default config
bq-antipattern-checker run

# Run with custom config and verbose output
bq-antipattern-checker run --config my-config.yaml --verbose

# Save results locally instead of pushing to BigQuery
bq-antipattern-checker run --dry-run --output-format json

# Limit number of jobs processed (useful for testing)
bq-antipattern-checker run --limit-row 50

# Combine dry-run with limit for local testing
bq-antipattern-checker run --dry-run --limit-row 10 --output-format csv

# List all available antipatterns
bq-antipattern-checker list-antipatterns

# Show current configuration
bq-antipattern-checker show-config

# Create a new configuration file
bq-antipattern-checker create-config --output my-config.yaml
```

### Command Options

#### `run` Command Options

* `--config, -c` - Path to YAML configuration file (default: `antipattern-config.yaml`)
* `--verbose, -v` - Enable verbose output for detailed progress information
* `--dry-run` - Save results locally instead of pushing to BigQuery
* `--limit-row` - Limit number of jobs to process (useful for testing or sampling)
* `--output-format` - Output format for dry-run: `console`, `json`, `csv`, `parquet`
* `--output-file` - Specify output file path (auto-generated if not provided)

#### Other Commands

* `list-antipatterns --config [file]` - List antipatterns with their enabled/disabled status
* `show-config --config [file]` - Display detailed configuration including all settings
* `create-config --output [file] --force` - Create configuration file with optional force overwrite

### Output Formats (for dry-run)

When using `--dry-run`, you can specify different output formats:

* `console` - Display formatted results in terminal with summary statistics (default)
* `json` - Save results as JSON file with complete job metadata
* `csv` - Save results as CSV file for spreadsheet analysis
* `parquet` - Save results as Parquet file for efficient data processing

```bash
# Save results as CSV with specific filename
bq-antipattern-checker run --dry-run --output-format csv --output-file results.csv

# Display results in console with verbose output
bq-antipattern-checker run --dry-run --output-format console --verbose

# Process limited jobs and save as JSON for testing
bq-antipattern-checker run --dry-run --limit-row 100 --output-format json

# Save as Parquet for data analysis
bq-antipattern-checker run --dry-run --output-format parquet --output-file analysis.parquet
```

### Performance and Testing

The `--limit-row` option is particularly useful for:

* **Testing configurations** - Process a small sample before full runs
* **Development** - Quick feedback during development
* **Sampling** - Analyze a subset of jobs for pattern identification
* **Resource management** - Control processing load on large job datasets

**Example testing workflow:**
```bash
# 1. Test with small sample first
bq-antipattern-checker run --dry-run --limit-row 10 --verbose

# 2. If successful, run larger sample
bq-antipattern-checker run --dry-run --limit-row 100 --output-format csv

# 3. Finally, run full analysis
bq-antipattern-checker run --verbose
```

## Configuration

The tool uses a YAML configuration file to specify BigQuery projects, datasets, and antipattern settings. Generate a default configuration with:

```bash
bq-antipattern-checker create-config
```

Key configuration sections:
* **BigQuery settings**: Projects, datasets, regions
* **Thresholds**: Row counts for large table detection
* **Date settings**: Number of days back to analyze
* **Antipatterns**: Enable/disable specific checks

### Database Setup

Before running the tool, you need to create a results table in BigQuery. The tool includes a DDL template:

```sql
-- Create the results table (replace with your project/dataset)
CREATE TABLE `your-project.your-dataset.antipattern_results`
(
  job_id STRING,
  creation_date DATE,
  creation_time STRING,
  project_id STRING,
  user_email STRING,
  reservation_id STRING,
  total_process_gb FLOAT64,
  total_slot_hrs FLOAT64,
  total_duration_mins FLOAT64,
  query STRING,
  partition_not_used BOOL,
  available_partitions ARRAY<STRUCT<partitioned_column STRING, table_name STRING>>,
  big_date_range BOOL,
  no_date_on_big_table BOOL,
  tables_without_date_filter ARRAY<STRING>,
  select_star BOOL,
  references_cte_multiple_times BOOL,
  semi_join_without_aggregation BOOL,
  order_without_limit BOOL,
  like_before_more_selective BOOL,
  regexp_in_where BOOL, 
  queries_unpartitioned_table BOOL,
  unpartitioned_tables ARRAY<STRING>,
  distinct_on_big_table BOOL,
  count_distinct_on_big_table BOOL,
  antipattern_run_time STRING
)
PARTITION BY creation_date;
```

The table is partitioned by `creation_date` for efficient querying and data management. When the tool runs with the same date, it replaces the partition data, allowing for safe re-runs.

## How can you contribute?
There are numerous ways you can contribute in this project.
* Improve code quality. This project is primarily aimed at getting the information required and enabling teams to take actionable advice through it. Code can be made leaner & more efficient. If you can show that the changes done don't change the results we are happy to evaluate it and make the changes.
* Currently it's tailored to BigQuery dialect but it can be used on other dialects as well. Check relevant section below. 
* New antipatterns. If you think there are critical things to check on BigQuery for optimisation and they can be added. 
* Improving existing antipatterns, if you spotted scenarios being missed out in the existing antipatterns.
* For clarification of any questions and clarifications feel free to start a discussion.
  
## Requirements & How to Run On Your Environment (Legacy)
* Python >= 3.10
* Clone the repo to your environment. We will convert the whole thing to a pip package later.
* When you clone the repo, your IDE should install the dependencies on the requirements.txt otherwise you can use `pip install -r requirements.txt` command to install everything or independently install items from requirements.txt file. 
* You need a BigQuery project to execute SQL statements and to populate results table. That is used in bigquery_job_project in config section below
* A table created for the results. You can use the DDL SQL statement under templates. Full qualification of table name will be used for bigquery_dataset_project, bigquery_dataset and table_names variables in the config section below.
* You need Google CLI to authorise, so install if your environment doesn't have it from [here](https://cloud.google.com/sdk/docs/install) 
* You need to authorise your BigQuery job project if you haven't yet on your environment. Follow the information on this [link](https://cloud.google.com/bigquery/docs/authentication) to configure your authorisation.

Whether you run locally or as a cloud run, used account needs these permissions
* INFORMATION_SCHEMA views (JOBS, TABLE_STORAGE_BY_PROJECT, COLUMNS) for the query projects and dataset projects you want to check. Metadata Viewer role should suffice. 
* Editor role (write permission) on the dataset you will push the results.
* Check [here](https://cloud.google.com/bigquery/docs/access-control) for more information on roles and permissions.

## Configuration File Structure

The tool uses a YAML configuration file (`antipattern-config.yaml`) to manage all settings. You can generate a default configuration file using:

```bash
bq-antipattern-checker create-config
```

Here's an example configuration file with explanations:

```yaml
# BigQuery SQL Antipattern Checker Configuration

# BigQuery Configuration
bigquery_job_project: "dev-project"  # Project where SQL commands are executed for this application
bigquery_dataset_project: "dev-dataset-project"  # Project where your results table resides
bigquery_dataset: "dev_dataset"  # Dataset where your results table resides
bigquery_region: "region-EU"  # BigQuery region (e.g., "US", "EU", "asia-northeast1")
information_schema_project: "dev-dataset-project"  # Project where TABLE_STORAGE and COLUMNS views are stored
query_project: "dev-project"  # Project where INFORMATION_SCHEMA.JOBS view resides

# Table Configuration
results_table_name: "antipattern_results"  # Name of the results table

# Thresholds
large_table_row_count: 1000  # Minimum row count to consider a table "large"
distinct_function_row_count: 10000  # Threshold for distinct function checks

# Date Configuration
days_back: 1  # Number of days back to check jobs (default: yesterday's jobs)

# Antipattern Configuration
# Each antipattern can be enabled/disabled and optionally have a description
antipatterns:
  select_star:
    enabled: true
    description: "Check for SELECT * statements that can impact performance"
  
  semi_join_without_aggregation:
    enabled: true
    description: "Check for semi-joins without proper aggregation"
  
  order_without_limit:
    enabled: true
    description: "Check for ORDER BY clauses without LIMIT"
  
  regexp_in_where:
    enabled: true
    description: "Check for expensive REGEXP functions in WHERE clauses"
  
  like_before_more_selective:
    enabled: true
    description: "Check for LIKE conditions placed before more selective conditions"
  
  multiple_cte_reference:
    enabled: true
    description: "Check for CTEs that are referenced multiple times (may cause re-evaluation)"
  
  partition_used:
    enabled: true
    description: "Check if partitioned tables are properly filtered by partition key"
  
  big_date_range:
    enabled: true
    description: "Check for date ranges larger than 365 days"
  
  big_table_no_date:
    enabled: true
    description: "Check for queries on large tables without date filters"
  
  unpartitioned_tables:
    enabled: true
    description: "Check for queries on large unpartitioned tables"
  
  distinct_on_big_table:
    enabled: true
    description: "Check for DISTINCT operations on large tables"
  
  count_distinct_on_big_table:
    enabled: true
    description: "Check for COUNT DISTINCT operations on large tables"
```

### Configuration Variable Details

#### BigQuery Settings
* **`bigquery_job_project`** - The project that the application uses to execute its queries. This is used to authorize the BigQuery client.
* **`bigquery_dataset_project`** - The project where the dataset and the results table resides.
* **`bigquery_dataset`** - The dataset where your results table resides.
* **`bigquery_region`** - Your BigQuery region (e.g., "US", "EU", "asia-northeast1").
* **`information_schema_project`** - Where your information schema views reside, mainly TABLE_STORAGE and COLUMNS views.
* **`query_project`** - This can be used for JOBS view. If you have decoupled storage from computation, this field can be used for checking the jobs for antipatterns.
* **`results_table_name`** - The name of the table where analysis results will be stored.

#### Threshold Settings
* **`large_table_row_count`** - Minimum row count to consider a table "large" for antipattern detection (default: 1000).
* **`distinct_function_row_count`** - Threshold for distinct function checks (default: 10000).

#### Date Settings
* **`days_back`** - Number of days back to check jobs. Default is 1 (yesterday's jobs).

#### Antipattern Configuration
Each antipattern can be individually enabled or disabled. The configuration supports:
* **`enabled`** - Boolean flag to enable/disable the antipattern check
* **`description`** - Optional description explaining what the antipattern detects

### Environment Variable Overrides

You can override any configuration setting using environment variables:
* `BIGQUERY_JOB_PROJECT`
* `DESTINATION_DATASET_PROJECT`
* `DESTINATION_DATASET`
* `BIGQUERY_REGION`
* `INFORMATION_SCHEMA_PROJECT`
* `QUERY_PROJECT`
* `RESULTS_TABLE_NAME`
* `LARGE_TABLE_ROW_COUNT`
* `DISTINCT_FUNCTION_ROW_COUNT`
* `DAYS_BACK`

### Project Architecture Considerations

Some companies/users keep all their tables and run all their jobs on the same project, while others prefer decoupling storage from compute. The configuration options above are designed to accommodate both approaches:

- **Single Project Setup**: Set all project fields to the same project ID
- **Decoupled Setup**: Use different projects for job execution, data storage, and metadata access

### Viewing Your Configuration

You can display your current configuration settings at any time using:

```bash
bq-antipattern-checker show-config --config your-config.yaml
```

## Architecture Overview

The application is built with a modular, class-based architecture for better maintainability and configuration management:

### Core Components

#### `antipatterns.py` - Antipattern Detection Class
* Contains the `Antipatterns` class that encapsulates all antipattern detection logic
* Instantiated with a `Config` object to respect enabled/disabled antipattern settings
* Each antipattern is a class method that analyzes SQLGlot AST (Abstract Syntax Tree)
* **Enhanced Error Handling**: Each antipattern check has individual try-catch blocks for precise error identification
* Provides both class-based interface and backwards-compatible function wrappers

**Key Features:**
* Configuration-driven execution (only runs enabled antipatterns)
* Individual error handling per antipattern for easier debugging
* Clean separation of concerns
* Extensible design for adding new antipatterns

#### `functions.py` - Helper Functions
* Contains utility functions for BigQuery operations and data processing
* All functions now accept `Config` object as parameter for centralized configuration
* Functions include:
  - BigQuery client management and query execution
  - Column, partition & storage information retrieval
  - Job metadata extraction from INFORMATION_SCHEMA
  - Table alias and naming utilities
  - Results formatting and BigQuery upload

#### `config.py` - Configuration Management
* `Config` dataclass for centralized configuration management
* Support for YAML file configuration and environment variable overrides
* Individual antipattern enable/disable controls
* Backwards compatibility with legacy configuration methods

#### `classes.py` - Job Processing
* `Job` class represents individual BigQuery jobs for analysis
* Integrates with `Antipatterns` class for configuration-aware processing
* **Enhanced Error Handling**: Individual try-catch blocks for each antipattern check
* Detailed error messages that identify which specific antipattern failed

### Error Handling Improvements

The application now provides granular error reporting:

```
Error in check_partition_used: division by zero
Error in check_big_date_range: invalid date format
Error in check_select_star: unexpected AST structure
```

This makes debugging much easier by pinpointing exactly which antipattern detection failed and why, rather than generic error messages.

## Troubleshooting

### Common Issues and Solutions

#### Authentication Issues
```bash
# Error: Could not automatically determine credentials
gcloud auth application-default login

# Error: Permission denied
# Ensure your account has the required BigQuery permissions:
# - Metadata Viewer role for INFORMATION_SCHEMA access
# - Editor role for the results dataset
```

#### Configuration Issues
```bash
# Verify your configuration
bq-antipattern-checker show-config --config your-config.yaml

# Test with a small sample first
bq-antipattern-checker run --dry-run --limit-row 5 --verbose
```

#### Memory/Performance Issues
```bash
# Use limit-row for large datasets
bq-antipattern-checker run --limit-row 1000

# Process in smaller batches during testing
bq-antipattern-checker run --dry-run --limit-row 100
```

#### Debugging Antipattern Errors
When specific antipattern checks fail, the enhanced error handling will show exactly which check failed:
```
Error in check_big_table_no_date: KeyError: 'column_name'
Error in check_partition_used: AttributeError: 'NoneType' object has no attribute 'args'
```

This allows you to:
1. Identify problematic queries by job ID
2. Focus debugging on specific antipattern logic
3. Temporarily disable problematic antipatterns in configuration
4. Report specific errors for investigation

## How it Operates
* Gets all the jobs from the given project
* Parses queries of each job, converts to AST using SQLGlot
* Calls each antipattern function and keeps a track of them as TRUE/FALSE using SQLGlot for predefined cases and optimisation logic we are using
* Also adds helper values like, unused partitions, tables without date filter, unpartition tables etc
* Creates a Pandas dataframe of the results to push into BQ
* Sends the results to designated antipattern_results table
* If you follow the table creation guideline on the DDL template. Application would scan T-1 day's jobs and it can be rerun as it will replace the partition.

## Will this only run on BigQuery?
Currently the SQL dialect for SQLGlot is BigQuery for BQ specific syntaxes and functions and some antipatterns are tuned for BQ specific cases.
However SQLGlot supports many dialects and most of the antipatterns are valid for any SQL dialect. 
We hope to increase coverage by working with fellow developers on different dialects and you can contribute into this as well. 
You are welcome to reach out and discuss about collaboration.

## Antipatterns

The tool currently detects 12 different SQL antipatterns. You can view the status of all antipatterns using:

```bash
bq-antipattern-checker list-antipatterns --config your-config.yaml
```

Each antipattern can be individually enabled or disabled in your configuration file.

### Complete Antipattern List

| Antipattern | Default | Description |
|-------------|---------|-------------|
| `select_star` | ✓ | SELECT * statements that can impact performance |
| `semi_join_without_aggregation` | ✓ | Semi-joins without proper aggregation |
| `order_without_limit` | ✓ | ORDER BY clauses without LIMIT |
| `regexp_in_where` | ✓ | Expensive REGEXP functions in WHERE clauses |
| `like_before_more_selective` | ✓ | LIKE conditions before more selective conditions |
| `multiple_cte_reference` | ✓ | CTEs referenced multiple times (causes re-evaluation) |
| `partition_used` | ✓ | Partitioned tables not filtered by partition key |
| `big_date_range` | ✓ | Date ranges larger than 365 days |
| `big_table_no_date` | ✓ | Queries on large tables without date filters |
| `unpartitioned_tables` | ✓ | Queries on large unpartitioned tables |
| `distinct_on_big_table` | ✓ | DISTINCT operations on large tables |
| `count_distinct_on_big_table` | ✓ | COUNT DISTINCT operations on large tables |

### Detailed Antipattern Descriptions

### partition_used (formerly partition_not_used)

If a table in JOIN or WHERE clause references a table with a partitioned column but the query is not using that column in JOIN or WHERE, then this value is True.

This effectively means that the query is not benefiting from the partition available in a table. Partition drastically improves the query performance / reduce the cost and is the most important antipattern to prioritise for when fixing antipatterns and focusing on cost reduction.

**Helper column:** available_partitions
* This column provides you the tables and their partitioned columns that you can use in a query.

You can define the minimum size of the tables you want to check. You can change the condition on `columns_query.sql`

### big_data_range

This antipattern function checks JOIN and WHERE clauses for date functions and tries to identify if a date range bigger than 365 days is being used in the query.

**Helper column:** big_date_range_columns

#### Why is this an antipattern?

While we understand that some use cases may require scanning a big date range, we should also acknowledge that scanning a date range bigger than required timeframe increase the size of the data to be processed therefore leading to additional resource usage, very high in some cases.

So this antipattern is there to show that there is an opportunity to reduce the date range to less than at the very least. 

If the date range is greater than 1 year then this case is TRUE. 

You can define the minimum size of the tables you want to check. You can change the condition on `columns_query.sql`

### no_date_on_big_table

In addition to scanning big date ranges on tables, we have seen many cases of queries where there is no date filter used at all when selecting data from a big table. This is highly costly and should be avoided at all times when you are querying an event / fact table that is very high volume (see helper columns)

Below is how the function determines if there is no date filter on a big table:
* Checks the tables in FROM and JOIN statements
* Checks if there are any date columns & date functions.
* If there are no date columns mentioned belonging to those tables, then returns TRUE
* If there is a date column but it is used as a join condition to another table but not for limiting data, it returns TRUE

You can define the minimum size of the tables you want to check. You can change the condition on `columns_query.sql`

### distinct_on_big_table

If you are selecting from a big table and using a DISTINCT statement, this value is TRUE

You can define the minimum size of the tables you want to check

While there are justifiable cases to use DISTINCT in order to reduce duplications, we strongly recommend you to ensure that:
* The row count is actually duplicated when distinct is not used
* The difference coming from not using distinct and having some duplicate rows is not negligible.
* Duplications can be removed by improving joins and where conditions

### count_distinct_on_big_table

If you are using a COUNT DISTINCT statement on a large table, this value is TRUE

You can define the minimum size of the tables you want to check

While there are justifiable cases to use COUNT DISTINCT in order to reduce duplications. Check;
* The row count is actually duplicated when distinct is not used
* The difference coming from not using distinct and having some duplicate rows is not negligible.
* Duplications can be removed by improving joins and where conditions
* approx_count_distinct is sufficient. 

### queries_unpartitioned_table
This antipattern is different from the partition_not_used antipattern. That one checks if the source table you query from has a partition column and if you are not using it. This one checks if the source table you are querying from has no partitioned column at all.
Helper column: unpartitioned_tables (list of tables that don’t have a partition) You can use this data to check with owners to see adding partition can help. 
You can define the minimum size of the tables you want to check. You can change the condition on `columns_query.sql`

#### Why is this listed as an antipattern?
Big tables without a partition is a potentially high additional cost to anyone querying from them. 

### select_star 

BigQuery stores table data in columnar format, meaning that it performs better when only required columns are scanned instead of the whole table.
This antipattern function checks if the query contains “select *” statement. 

Ignores:

* If it’s used on a CTE or Subquery within that query. 
* count(*)

### references_cte_multiple_times

If you created CTE, and then referred to that CTE multiple times, BQ may reevaluate them each time causing additional computation and defeating its purpose. Try calling a CTE once.

#### Reevaluation?

The purpose of CTE is to keep a computed result in memory and refer to it any time it’s being called in the script, however BigQuery may recalculate the content of that CTE each time it’s called which is the reevaluation. 

### semi_join_without_aggregation

If you are using a subquery as a WHERE or JOIN condition, not selecting distinct values from the subquery may cause huge overhead because of initial duplication and then deduplication done at the back. 

If you look at the example below there is a WHERE condition for `t1.col2` which is filtered for the values from `table2.col2`. 
If col2 values appear multiple times in table2 then this condition would need to do the extra effort to de-duplicate.

```sql
SELECT 
   t1.col1 
FROM 
   `project.dataset.table1` t1 
WHERE 
    t1.col2 not in (select col2 from `project.dataset.table2`);
```
 
### order_without_limit

ORDER BY statement in a query requires another series of computation to sort the results by given conditions.

While it’s fine to use it for small size of output or analysing a case to get TOP X number of results, using ORDER BY in a scheduled transformation job without limiting has arguably no added value and increases the computation cost. 

### like_before_more_selective

In your WHERE clause, the ordering of conditions can have an impact on the query performance. In order to have the fastest possible computation, try to order your conditions from the most selective to least. 

#### Selective?

It shows the definitive level of a condition. 
Such as giving a date condition like `where date = current_date()` is a much more selective statement than a fuzzy statement such as `LIKE` or `REGEXP_CONTAINS`, `REGEXP_LIKE`. 

There this antipattern is named like_before_more_selective. 

```sql
SELECT 
  column_a,
  column_b
FROM 
  table_1 
WHERE
  column_a like '%test%'
  and date >= '2024-04-01';
```

Here if you change the sequence of WHERE conditions you will get a better performance. 
Which is as simple as below:

```sql
SELECT 
  column_a,
  column_b
FROM 
  table_1 
WHERE
  date >= '2024-04-01'
  and column_a like '%test%'
  ;
```

### regexp_in_where

While it can be powerful in text search based on conditions, it should also be noted that REGEXP functions are also costly text search functions. If possible, refer to LIKE instead of REGEXP in your WHERE statements. 
 
