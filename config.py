from datetime import date, timedelta
import os

#os.getenv() is called here to get the environment variables of the cloud run deployment. local run would refer to second value
bigquery_job_project = os.getenv('BIGQUERY_JOB_PROJECT', "dev-project") #project where the SQL commands are executed
bigquery_dataset_project = os.getenv('DESTINATION_DATASET_PROJECT', 'dev-dataset-project') #project where your results table resides
bigquery_dataset = os.getenv('DESTINATION_DATASET', 'dev_dataset') #dataset where your results table resides
bigquery_region = 'region-EU' #sample region. you can use different region based on your location
information_schema_project = bigquery_dataset_project #specify if your your information schema views are stored on a different project than your results table
query_project = bigquery_job_project #you can use different project if environment is different

table_names = {
    'results': 'antipattern_results'
}

large_table_row_count = 1000 # Used for getting list of tables with partitions and also for finding large tables without partition. This number is random, change for your own company data size
distinct_function_row_count = 10000 # Used for distinct and count distinct antipatterns. This number is random, change for your own company data size

job_run_date = date.today() - timedelta(days=1) # by default it checks yesterday's jobs
date_values = {
    'partition_date': str(job_run_date.strftime("%Y%m%d")),
    'query_run_date': job_run_date,
    'query_run_date_str': "'" + str(job_run_date) + "'"
}
