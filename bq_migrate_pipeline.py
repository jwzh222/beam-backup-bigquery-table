import argparse
import logging

import apache_beam as beam


def run(argv=None):
    """Run the workflow."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help=(
            'Input(source) BigQuery table specified as: '
            'PROJECT:DATASET.TABLE or DATASET.TABLE.'))

    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help=(
            'Output(destination) BigQuery table for results specified as: '
            'PROJECT:DATASET.TABLE or DATASET.TABLE.'))
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--runner=DataflowRunner',
        '--project=zheng-356906',

        #'--region=asia-east2',
        
        #'--staging_location=gs://YOUR_BUCKET_NAME/AND_STAGING_DIRECTORY',
        #'--temp_location=gs://YOUR_BUCKET_NAME/AND_TEMP_DIRECTORY',
        #'--job_name=your-wordcount-job',
    ])

    with beam.Pipeline(argv=pipeline_args) as p:

        table_records = p | 'ReadTable' >> beam.io.ReadFromBigQuery(
            table=known_args.input)

        from apache_beam.io.gcp.internal.clients import bigquery
        table_schema = bigquery.TableSchema()

        # Fields that use standard types.
        schema1 = bigquery.TableFieldSchema()
        schema1.name = 'date'
        schema1.type = 'DATE'
        schema1.mode = 'nullable'
        table_schema.fields.append(schema1)

        schema2 = bigquery.TableFieldSchema()
        schema2.name = 'country_name'
        schema2.type = 'string'
        schema2.mode = 'nullable'
        table_schema.fields.append(schema2)

        schema3 = bigquery.TableFieldSchema()
        schema3.name = 'new_confirmed'
        schema3.type = 'integer'
        schema3.mode = 'nullable'
        table_schema.fields.append(schema3)

        schema4 = bigquery.TableFieldSchema()
        schema4.name = 'new_deceased'
        schema4.type = 'integer'
        schema4.mode = 'nullable'
        table_schema.fields.append(schema4)

        schema5 = bigquery.TableFieldSchema()
        schema5.name = 'cumulative_persons_fully_vaccinated'
        schema5.type = 'integer'
        schema5.mode = 'nullable'
        table_schema.fields.append(schema5)

        schema6 = bigquery.TableFieldSchema()
        schema6.name = 'population'
        schema6.type = 'integer'
        schema6.mode = 'nullable'
        table_schema.fields.append(schema6)

        table_records | 'WriteTable' >> beam.io.WriteToBigQuery(
            known_args.output,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
