from logger import log_config
from google.cloud import bigquery
from read_data import extract_data
from create_schema_for_bq import create_bq_schema
from create_or_update_table import create_or_update
from load_data import load_data

bq_client = bigquery.Client()

bucket_name = 'your-bucket'
file_name = 'your-file.json'
uri = 'your-bucket-that-consists-json-file'

project_id = 'your-project-id'
dataset_id = 'your-dataset-id'
table_id = 'your-table-id'
full_table_name = '{}.{}.{}'.format(project_id, dataset_id, table_id)

if __name__ == '__main__':

    logger = log_config()
    
    json_list = extract_data(bucket_name, file_name)

    bq_schema, source_field_paths = create_bq_schema(json_list)

    create_or_update(full_table_name, bq_schema, source_field_paths, project_id, dataset_id, table_id, logger)

    load_data(uri, full_table_name, logger)

