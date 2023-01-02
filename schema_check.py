

def schema_check(source_field_paths, project_id, dataset_id, table_name, bq_client, logger):
# Compares the source and the target schema to check if there is a field to be added or not.
    target_fields = []
    source_fields = []
    unmatch_list = []

    query = """
            select 
                field_path
            from {}.{}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
            where 1=1
            and table_name = '{}'""".format(project_id, dataset_id, table_name)
    
    query_job = bq_client.query(query)
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
    
    for source_item in source_fields:
        if source_item not in target_fields:
            logger.info('There is a new field to be added and the field is {}'.format(source_item))
            unmatch_list.append(source_item)
    
    if len(unmatch_list) < 1:
        logger.info('Table schema is not updated.')
    else:
        logger.info('Table schema is updated.')