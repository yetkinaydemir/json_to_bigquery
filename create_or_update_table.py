from google.cloud import bigquery

bq_client = bigquery.bq_client()

def create_table_bq(table_id, bq_schema):
# If the table exists in the dataset, updates the schema. 
# Before updating the schema, it checks both the source and the target schema if there is a new field to be added. This part is for verifying the schemas.
# If the table does not exist in the dataset, creates the table.
    try:
        table_info = bq_client.get_table(table_id)
        print('Table {}.{}.{} already exists!'.format(table_info.project, table_info.dataset_id, table_info.table_id))
        print('BQ Table Schema : \n', table_info.schema)
        schema_check(source_field_paths)
        table_info.schema = bq_schema
        table_info = bq_client.update_table(table_info, ['schema'])
        print('Updated BQ Schema: \n', table_info.schema)
        return table_info.schema
    except:
        table = bigquery.Table(table_id, schema=bq_schema)
        table = bq_client.create_table(table)
        print('Created {}.{}.{}'.format(table.project, table.dataset_id, table.table_id))