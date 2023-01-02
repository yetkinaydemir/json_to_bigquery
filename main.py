from logger import log_config
from google.cloud import bigquery
from read_data import extract_data
from create_schema_for_bq import create_bq_schema

bq_client = bigquery.Client()

bucket_name = 'bckt_challenge'
file_name = 'challenge_data.json'

if __name__ == '__main__':

    logger = log_config()
    
    json_list = extract_data(bucket_name, file_name)

    create_bq_schema(json_list)