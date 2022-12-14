from google.cloud import bigquery

bq_client = bigquery.Client()

def load_data(uri, table_id, logger):
# Loads the JSON data to created or updated BigQuery table.
    job_config = bigquery.LoadJobConfig(
                    write_disposition = bigquery.WriteDisposition.WRITE_APPEND,
                    schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
                    source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    load_job = bq_client.load_table_from_uri(
                        uri
                        , table_id
                        , job_config=job_config
    )

    load_job.result()

    logger.info('Loaded {} rows'.format(load_job.output_rows))