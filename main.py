from logger import log_config
from google.cloud import bigquery
from read_data import extract_data
from create_schema_for_bq import create_bq_schema

bq_client = bigquery.Client()

bucket_name = 'bckt_challenge'
file_name = 'challenge_data.json'

project_id = 'your-project-id'
dataset_id = 'your-dataset-id'
table_id = 'your-table-id'

if __name__ == '__main__':

    logger = log_config()
    
    json_list = extract_data(bucket_name, file_name)

    bq_schema, source_field_paths = create_bq_schema(json_list)