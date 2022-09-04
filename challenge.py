import json
from google.cloud import bigquery


client = bigquery.Client()

json_file = 'C:/Users/ytkna/Desktop/challenge_data.json' 

table_id = 'devoteam-techchallenge.test.load_from_uri_test_4'

json_list=[]

f = open(json_file)

for line in f:
    json_list.append(json.loads(line))

print('Data List: \n', json_list)

def json_extract(obj):

    values = []
    keys = []
    records = []

    def extract(obj, values):
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
print('Values :', values, 'Non-records :', keys, 'Records :', records)

data_dict = {k: v for k, v in zip(keys, values)}
print('Data Dict: \n', data_dict)

def data_dict_schema(json_list):

    data_dict_schema = {}
    
    for item in json_list:
        data_dict_schema.update(item)

    print('Data Dict Schema: \n', data_dict_schema)
    return data_dict_schema

def data_types(data_dict):
    bq_schema = []

    def data_type(data_dict):
        for k, v in data_dict.items():
            if type(v) is int:
                value_type = 'integer'
            elif type(v) is str:
                value_type = 'string'
        return value_type
    
    def extract_keys(data_dict, value_type):
        fields = []
    
        for k, v in data_dict.items():
            if not isinstance(v, dict):
                bq_schema.append(bigquery.SchemaField(k, value_type))

            elif isinstance(v, dict):
                for x, y in v.items():
                    fields.append(bigquery.SchemaField(x, data_type(v)))
                    bq_schema.append(bigquery.SchemaField(k, 'record', mode='repeated', fields=fields))
                    fields=[]
            
        return bq_schema

    value_type = data_type(data_dict)
    
    bq_schema = extract_keys(data_dict, value_type)
    #print('Fields: \n', fields)
    return bq_schema

def create_table_bq(table_id, schema):
    
    table = bigquery.Table(table_id, schema=bq_schema)
    table = client.create_table(table)

    print('Created {}.{}.{}'.format(table.project, table.dataset_id, table.table_id))

dict_schema = data_dict_schema(json_list)

bq_schema = data_types(dict_schema)

print('BigQuery Schema: \n', bq_schema)

create_table_bq(table_id, bq_schema)


