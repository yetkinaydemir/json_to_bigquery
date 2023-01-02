from google.cloud import bigquery

def create_bq_schema(json_list):
    bq_schema = []
    data_dict = {}
    
    for item in json_list:
        data_dict.update(item)

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
    print('BQ Schema: \n', bq_schema)
    #print('Source Field Paths: ', source_field_paths)
    return bq_schema, source_field_paths