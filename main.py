import functions
import config
from classes import Job
import time


def main():
    start = time.perf_counter()
    columns_dict = functions.get_columns_dict(config.bigquery_dataset_project,
                                              config.bigquery_region, 
                                              config.large_table_row_count
                                              )
    job_output = {}
    print("Checking Jobs Ran On: ", config.date_values['query_run_date_str'])

    jobs_dict = functions.get_jobs_dict(config.date_values['query_run_date_str'],
                                        config.query_project,
                                        config.bigquery_region
                                        )
    print("Jobs Found: ", len(jobs_dict))

    for k, v in jobs_dict.items():
        job_id = v['job_id']
        job = Job(v)
        job.check_antipatterns(columns_dict)

        job_output[job_id] = job.__dict__

    job_output_df = functions.get_output_df(job_output, 'job_id')

    functions.push_df_to_bq(job_output_df,
                            config.table_names['results'],
                            config.bigquery_dataset_project,
                            config.bigquery_dataset)

    end = time.perf_counter()
    elapsed = end - start
    print(f'Time taken: {elapsed:.6f} seconds')


if __name__ == "__main__":
    main()
