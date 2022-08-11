import argparse
from concurrent.futures import ThreadPoolExecutor
import logging
import os

import pandas as pd
import numpy as np

import apache_beam as beam
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema, TableFieldSchema

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def read_from_task_file(task_file='task.xls'):
    task_file = os.path.abspath(task_file)
    xl_file = pd.ExcelFile(task_file)
    task = None
    try:
        task_list = list(xl_file.parse().replace(
            {np.nan: None}).T.to_dict().values())
    except Exception as e:
        raise e
    for task in task_list:
        if not task['target_table']:
            task['target_table'] = task['source_table']
    return task_list


def get_table_schema(table):
    from google.cloud import bigquery
    client = bigquery.Client()
    dataset_ref = client.dataset(
        table['source_dataset'], project=table['source_project'])
    table_ref = dataset_ref.table(table['source_table'])

    table = client.get_table(table_ref)
    schema_field_list = table.schema

    # Convert SchemaField type to TableSchema, which beam.io.WriteToBigQuery used to start jobs.
    table_schema = TableSchema()
    table_schema.fields = [schemafield_to_tableschema(
        schema_field) for schema_field in schema_field_list]
    return table_schema


def schemafield_to_tableschema(schema_field):
    """
        beam.io.WriteToBigQuery takes TableSchema object as parameter, while bigquery client returns SchemaField object, so need to convert here.
    """
    table_field_schema = TableFieldSchema()
    table_field_schema.name = schema_field.name
    table_field_schema.type = schema_field.field_type
    table_field_schema.mode = schema_field.mode
    table_field_schema.policyTags = schema_field.policy_tags
    table_field_schema.description = schema_field.description
    table_field_schema.fields = list(schema_field.fields)
    return table_field_schema


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--task', dest='task', default='task.xls')
    known_args, pipeline_args_command_line = parser.parse_known_args(argv)
    pipeline_args = [
        '--runner=DataflowRunner',
        '--project=zheng-356906',
        '--region=asia-east2',
        '--staging_location=gs://jiawei_bucket/staging',
        '--temp_location=gs://jiawei_bucket/tmp',
        #'--job_name=beam_zj_migrate_bigquery_tablea_job',
        # '--sdk_location=',
        '--service_account_email=airflow-operator-423@zheng-356906.iam.gserviceaccount.com'
    ]
    pipeline_args += pipeline_args_command_line
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    task_list = read_from_task_file(known_args.task)

    with ThreadPoolExecutor(max_workers=8) as executor:
        for task in task_list:
            print(task, "started!!!!!!")
            executor.submit(migrate_job, task, pipeline_options)
            #print(future.result())
    print("all job finished!!!!!!!!!!")        


def migrate_job(task, pipeline_options):
    """Run the workflow."""
    table_schema = get_table_schema(task)

    input = task['source_project'] + ':' + \
        task['source_dataset'] + '.' + task['source_table']
    output = task['target_project'] + ':' + \
        task['target_dataset'] + '.' + task['target_table']

    with beam.Pipeline(options=pipeline_options) as p:

        table_records = p | 'ReadTable' >> beam.io.ReadFromBigQuery(
            table=input)

        table_records | 'WriteTable' >> beam.io.WriteToBigQuery(
            output,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )
        p.run()
    return f"done with task{task}"


if __name__ == '__main__':
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\\zj\\coding\\GCP\service_account\\airflow-operator\\zheng-356906-1c4ce543fca0.json"
    logging.getLogger().setLevel(logging.INFO)
    run()
