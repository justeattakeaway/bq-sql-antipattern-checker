# BigQuery Antipattern Checker
test

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

## What is an antipattern?
Antipatterns are specific SQL syntaxes that in some cases might cause performance impact and they are generally accepted as bad coding practice.

There are a variety of conditions that can be used in query which can create huge computational complexity unnecessarily and also impact the code readability and quality. 
 
## How Does It Work?
The application is a Python project that queries INFORMATION_SCHEMA get job metadata including the query executed.
After that the queries go through a series of functions using SQLGlot Parser to check antipatterns. 

Final output is pushed to a BQ table.

## Development Information
Every function for antipattern check is developed from scratch by testing real queries used at JET.

We got inspiration from some of the antipatterns that can be found in this opensource repo 
[GoogleCloudPlatform/bigquery-antipattern-recognition](https://github.com/GoogleCloudPlatform/bigquery-antipattern-recognition)   
But we wrote those ones from scratch as well, we didn’t refer to their codes because it’s in Java, and we are using Python and a different library to solve our problems.
We added more cases that are not present in that repo. 

## Owner
The application is owned by Platform Engineering under Platform Engineering at JET

## Code of Conduct
Please see [Code of Conduct Guide](https://github.com/justeattakeaway/bq-sql-antipattern-checker/blob/main/CODE_OF_CONDUCT.md)
If you don't play nice you'll be banned. 

## How can you contribute?
There are numerous ways you can contribute in this project.
* Improve code quality. This project is primarily aimed at getting the information required and enabling teams to take actionable advice through it. Code can be made leaner & more efficient. If you can show that the changes done don't change the results we are happy to evaluate it and make the changes.
* Currently it's tailored to BigQuery dialect but it can be used on other dialects as well. Check relevant section below. 
* New antipatterns. If you think there are critical things to check on BigQuery for optimisation and they can be added. 
* Improving existing antipatterns, if you spotted scenarios being missed out in the existing antipatterns.
* For clarification of any questions and clarifications feel free to start a discussion.
  
## Requirements & How to Run On Your Environment
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

## Information About Config Variables
* You will notice some variables starting with os.getenv('DESTINATION_XYZ'). That is our common practice to read the environemnet variables from a separate infrastructure configuration. You don't need to stick to that practice.
* bigquery_job_project = The project that the application would run to execute its queries. This is used to authorize BigQuery client
* bigquery_dataset_project = The project where the dataset and the results table resides. 
* bigquery_dataset = The dataset which the queried table/view resides
* bigquery_region = Your BigQuery region like 'region-EU'
* information_schema_project = Where your information schema views reside mainly TABLE_STORAGE and COLUMNS views
* query_project = This can be used for JOBS view. If you have decoupled storage from computation this field can be used for checking the jobs for antipatterns.
* FYI some companies/users keep all their tables and run all their jobs on the same project and some prefer decoupling storage from compute. All the configurations above are here to accomodate those different methods.

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
Helper column: available_partitions
* This column provides you the tables and their partitioned columns that you can use in a query.
You can define the minimum size of the tables you want to check. You can change the condition on `columns_query.sql`

### big_data_range
This antipattern function checks JOIN and WHERE clauses for date functions and tries to identify if a date range bigger than 365 days is being used in the query.
Helper column: big_date_range_columns
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
This antipattern function checks if the query contains “select *” statement. Ignores:
* If it’s used on a CTE or Subquery within that query. 
* count(*)

### references_cte_multiple_times
If you created CTE, and then referred to that CTE multiple times, BQ may reevaluate them each time causing additional computation and defeating its purpose. Try calling a CTE once.

#### Reevaluation?
The purpose of CTE is to keep a computed result in memory and refer to it any time it’s being called in the script, however BigQuery may recalculate the content of that CTE each time it’s called which is the reevaluation. 

### semi_join_without_aggregation
If you are using a subquery as a WHERE or JOIN condition, not selecting distinct values from the subquery may cause huge overhead because of initial duplication and then deduplication done at the back. If you look at the example below there is a WHERE condition for “t1.col2” which is filtered for the values from “table2.col2”. If col2 values appear multiple times in table2 then this condition would need to do the extra effort to de-duplicate.
```
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
It shows the definitive level of a condition. Such as giving a date condition like where date = current_date() is a much more selective statement than a fuzzy statement such as LIKE or REGEXP_CONTAINS, REGEXP_LIKE. There this antipattern is named like_before_more_selective. 
```
SELECT 
  column_a,
  column_b
FROM 
  table_1 
WHERE
  column_a like '%test%'
  and date >= '2024-04-01';
```
Here if you change the sequence of WHERE conditions you will get a better performance. Which is as simple as below
```
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
 
