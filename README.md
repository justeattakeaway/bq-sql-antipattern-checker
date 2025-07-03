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
   # Run and push results to BigQuery
   bq-antipattern-checker run --config antipattern-config.yaml

   # Run with dry-run to save results locally
   bq-antipattern-checker run --config antipattern-config.yaml --dry-run

   # Run with verbose output
   bq-antipattern-checker run --config antipattern-config.yaml --verbose
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

# List all available antipatterns
bq-antipattern-checker list-antipatterns

# Show current configuration
bq-antipattern-checker show-config

# Create a new configuration file
bq-antipattern-checker create-config --output my-config.yaml
```

### Output Formats (for dry-run)

When using `--dry-run`, you can specify different output formats:

* `console` - Display results in terminal (default)
* `json` - Save as JSON file
* `csv` - Save as CSV file
* `parquet` - Save as Parquet file

```bash
# Save results as CSV
bq-antipattern-checker run --dry-run --output-format csv --output-file results.csv

# Display results in console
bq-antipattern-checker run --dry-run --output-format console
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
  
  partition_used:
    enabled: true
    description: "Check if partitioned tables are properly filtered by partition key"
  
  big_date_range:
    enabled: true
    description: "Check for date ranges larger than 365 days"
  
  # ... additional antipatterns
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

## Information About antipatterns.py
* This file contains all the functions for each antipattern. 
* They take in AST (Asymmetric Syntax Tree) conversion done by SQLGlot and uses the same library to parse and check for relevant conditions

## Information About functions.py
* Contains helper functions like preparing the output and pushing into BQ
* Also contains functions like getting column, partition & storage information, getting the jobs to check for antipatterns
* Helper functions like getting table alias from the SQL syntax and getting project, dataset, table information from queries

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

### partition_not_used

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
 
