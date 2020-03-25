from google.cloud import bigquery
import logging


def bqCreateDataset(bq_client, dataset_name):
    """
    Creates a dataset on Google Cloud Platform.
    Args:
        dataset_name: str - Name of the dataset
    Returns:
        dataset_id: str - Reference id for the dataset just created
    """
    dataset_ref = bq_client.dataset(dataset_name)

    try:
        dataset_id = bq_client.get_dataset(dataset_ref).dataset_id
        logging.warning('This dataset name: {} is already used.'.format(dataset_id))
        return dataset_id
    except:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bq_client.create_dataset(dataset)
        logging.info('Dataset {} created.'.format(dataset.dataset_id))
        return dataset.dataset_id


def bqCreateTable(bq_client, dataset_id, table_name):
    """
    Create main table with all cases and the medical text.
    Args:
        dataset_id: str - Reference id for the dataset to use
        table_name: str - Name of the table to create

    Returns:
        table_id: str - Reference id for the table just created
    """
    dataset_ref = bq_client.dataset(dataset_id)

    # Prepares a reference to the table
    table_ref = dataset_ref.table(table_name)

    try:
        return bq_client.get_table(table_ref).table_id
    except:
        schema = [
            bigquery.SchemaField('case', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('it_raw_txt', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('eng_raw_txt', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('eng_txt', 'STRING', mode='REQUIRED',
                                 description='Output of preprocessing pipeline.')]
        table = bigquery.Table(table_ref, schema=schema)
        table = bq_client.create_table(table)
        logging.info('table {} has been created.'.format(table.table_id))
        return table.table_id


def exportItems2BQ(bq_client, dataset_id, table_id, case, it_raw_blob, eng_raw_blob, curated_eng_blob):
    """
    Export text data to BigQuery.
    Args:
        dataset_id:
        table_id:
        case:
        it_raw_blob:
        eng_raw_blob:
        curated_eng_blob:

    Returns:

    """
    # Prepares a reference to the dataset
    dataset_ref = bq_client.dataset(dataset_id)

    table_ref = dataset_ref.table(table_id)
    table = bq_client.get_table(table_ref)  # API call

    # Download text from GCS
    it_raw_txt_string = it_raw_blob.download_as_string().decode('utf-8')
    eng_raw_txt_string = eng_raw_blob.download_as_string().decode('utf-8')
    curated_eng_string = curated_eng_blob.download_as_string().decode('utf-8')

    rows_to_insert = [{'case': case,
                       'it_raw_txt': it_raw_txt_string,
                       'eng_raw_txt': eng_raw_txt_string,
                       'eng_txt': curated_eng_string
                       }]
    errors = bq_client.insert_rows(table, rows_to_insert)  # API request
    assert errors == []
    logging.info('{} was added to {} dataset, specifically in {} table.'.format(case,
                                                                                dataset_id,
                                                                                table_id))


def returnQueryResults(bq_client, project_id, dataset_id, table_id, case_id):
    """
    Get results from a BigQuery query.
    Args:
        bq_client:
        project_id:
        dataset_id:
        table_id:
        case_id:

    Returns:

    """

    query = ('SELECT * FROM `{}.{}.{}` WHERE `case`="{}" LIMIT 1'.format(project_id, dataset_id, table_id, case_id))

    try:
        query_job = bq_client.query(query)
        is_exist = len(list(query_job.result())) >= 1
        logging.info('Query case id: {}'.format(case_id) if is_exist \
                         else "Case id: {} does NOT exist".format(case_id))
        logging.info(list(query_job.result()))
    except Exception as e:
        logging.error("Error", e)
