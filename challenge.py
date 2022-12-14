import ndjson
from google.cloud import bigquery
import os
from google.cloud import storage

# Service account key. A new service account key needs to be provided  for every new project.
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/ytkna/Downloads/devoteam-techchallenge-9cbd03c5683e.json'

client = bigquery.Client()
# Gets the JSON formatted data from Google Cloud Storage and loads the data to a list
storage_client = storage.Client()
bucket = storage_client.get_bucket('bckt_challenge')
blob = bucket.get_blob('challenge_data.json')
json_list = ndjson.loads(blob.download_as_string())

project_id = 'devoteam-techchallenge'
dataset_id = 'test'
table_name = 'load_from_uri_4'
table_id = '{}.{}.{}'.format(project_id, dataset_id, table_name)

print('Data List: \n', json_list)

def json_extract(obj):
# Extracts fields and values from JSON data.
    values = []
    keys = []
    records = []

    def extract(obj, values):
    # Seperates the data into keys, values and records. This part is for checking the data and it does not affect the rest of the code.
        for item in obj:
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if isinstance(v, (dict, list)):
                        extract(v, values)
                        records.append({k: v})
                    else:
                        values.append(v)
                        keys.append(k)
            elif isinstance(obj, list):
                for item in obj:
                    extract(item, values)    
            return values, keys, records
    
    values, keys, records = extract(obj, values)
    return values, keys, records
    #print(values)

values, keys, records = json_extract(json_list)
#print('Values :', values, 'Non-records :', keys, 'Records :', records)

def convert_list_to_dict(json_list):
# Converts the list formatted JSON data to dictionary format. Dictionary is being used to create the schema for BigQuery table.
    data_dict = {}
    
    for item in json_list:
        data_dict.update(item)

    print('Data Dict: \n', data_dict)
    return data_dict

def create_bq_schema(data_dict):
    bq_schema = []

    def data_type(data_dict):
    # Detects integer and string data types of dictionary values.
        for k, v in data_dict.items():
            if type(v) is int:
                value_type = 'integer'
            elif type(v) is str:
                value_type = 'string'
        return value_type
    
    def extract_keys(data_dict, value_type):
    # Creates the schema inline with BigQuery schema specification notation. Additionaly, creating a schema
        source_field_paths = []
        fields = []
        fields_for_schemacheck = []
    
        for k, v in data_dict.items():
            if not isinstance(v, dict):
                bq_schema.append(bigquery.SchemaField(k, value_type))
                source_field_paths.append(k)
            
            elif isinstance(v, dict):
                source_field_paths.append(k)
                for x, y in v.items():
                    fields.append(bigquery.SchemaField(x, data_type(v)))
                    fields_for_schemacheck.append('{}.{}'.format(k, x))
                bq_schema.append(bigquery.SchemaField(k, 'record', mode='repeated', fields=fields))
                source_field_paths.append(fields_for_schemacheck)
                fields=[]
                fields_for_schemacheck = []
            
        return bq_schema, source_field_paths

    value_type = data_type(data_dict)
    
    bq_schema, source_field_paths = extract_keys(data_dict, value_type)
    #print('Fields: \n', fields)
    #print('Source Field Paths: ', source_field_paths)
    return bq_schema, source_field_paths

def create_table_bq(table_id, bq_schema):
# If the table exists in the dataset, updates the schema. 
# Before updating the schema, it checks both the source and the target schema if there is a new field to be added. This part is for verifying the schemas.
# If the table does not exist in the dataset, creates the table.
    try:
        table_info = client.get_table(table_id)
        print('Table {}.{}.{} already exists!'.format(table_info.project, table_info.dataset_id, table_info.table_id))
        print('BQ Table Schema : \n', table_info.schema)
        schema_check(source_field_paths)
        table_info.schema = bq_schema
        table_info = client.update_table(table_info, ['schema'])
        print('Updated BQ Schema: \n', table_info.schema)
        return table_info.schema
    except:
        table = bigquery.Table(table_id, schema=bq_schema)
        table = client.create_table(table)
        print('Created {}.{}.{}'.format(table.project, table.dataset_id, table.table_id))

def schema_check(source_field_paths):
# Compares the source and the target schema to check if there is a field to be added or not.
    target_fields = []
    source_fields = []

    query = """
            select 
                field_path
            from {}.{}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
            where 1=1
            and table_name = '{}'""".format(project_id, dataset_id, table_name)
    
    query_job = client.query(query)
    results = query_job.result()

    for row in results:
        target_fields.append(row.field_path)
    print('Target Field Paths: \n', target_fields)

    for item in source_field_paths:
        if isinstance(item, list):
            for i in item:
                source_fields.append(i)
        else:
            source_fields.append(item)
    #print('Source Field Paths: \n', source_field_paths)
    print('Source Field Paths Unlisted: \n', source_fields)
    
    for source_item in source_fields:
        if source_item in target_fields:
            print('Fields match!')
        else:
            print('There is a new field to be added and the field is {}'.format(source_item))

def load_data(uri, table_id):
# Loads the JSON data to created or updated BigQuery table.
    job_config = bigquery.LoadJobConfig(
                    write_disposition = bigquery.WriteDisposition.WRITE_APPEND,
                    schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
                    source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    load_job = client.load_table_from_uri(
                        uri
                        , table_id
                        , job_config=job_config
    )

    load_job.result()

    print('Loaded {} rows'.format(load_job.output_rows))

data_dict = convert_list_to_dict(json_list)

bq_schema, source_field_paths = create_bq_schema(data_dict)

print('BigQuery Schema: \n', bq_schema)

schema_exists = create_table_bq(table_id, bq_schema)

uri = 'gs://bckt_challenge/challenge_data.json'

load_data(uri, table_id)


