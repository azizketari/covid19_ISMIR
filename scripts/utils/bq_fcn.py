from google.cloud import bigquery
import os
import logging


def bqCreateDataset(bq_client, dataset_name):
    """
    Creates a dataset on Google Cloud Platform.
    Args:
        bq_client - BigQuery client instantiation -
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
        bq_client: BigQuery client instantiation -
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
        bq_client: BigQuery client instance -
        dataset_id: str -
        table_id: str -
        case: str -
        it_raw_blob:gcs blob object -
        eng_raw_blob: gcs blob object -
        curated_eng_blob: gcs blob object -

    Returns:
        Logging completion
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
    return logging.info('{} was added to {} dataset, specifically in {} table.'.format(case,
                                                                                       dataset_id,
                                                                                       table_id))


def constructQuery(column_lst, case_id):
    """
    Construct the query to public dataset: aketari-covid19-public.covid19.ISMIR
    Args:
        column_lst: list - ["*"] or ["column_name1", "column_name2" ...]
        case_id: str - Optional e.g "case1"

    Returns:
        query object
    """
    # Public dataset
    # project_id = 'aketari-covid19-public'
    # dataset_id = 'covid19'
    # table_id = 'ISMIR'

    if (len(column_lst) == 1) and column_lst[0] == "*":
        query = ('SELECT * FROM `aketari-covid19-public.covid19.ISMIR` '
                 'WHERE `case`="{}" '.format(case_id))
        return query
    else:
        columns_str = ", ".join(column_lst)
        query = ('SELECT {} FROM `aketari-covid19-public.covid19.ISMIR` '
                 'WHERE `case`="{}" '.format(columns_str, case_id))
        return query


def returnQueryResults(bq_client, query):
    """
    Get results from a BigQuery query.
    Args:
        bq_client: BigQuery client instantiation -
        query: query object

    Returns:
        list of all rows of the query
    """

    try:
        query_job = bq_client.query(query)
        return list(query_job.result())
    except Exception as e:
        return logging.error("Error", e)


def populateBQ(bq_client, storage_client, bucket_name, dataset_name, table_name):
    """
    Populate BigQuery dataset.
    Args:
        bq_client: BigQuery client instantiation -
        storage_client:
        bucket_name:
        dataset_name:
        table_name:

    Returns:
        Populated BigQuery data warehouse
    """
    try:
        dataset_id = bqCreateDataset(bq_client, dataset_name)
        logging.info("The following dataset {} was successfully created/retrieved.".format(dataset_name))
    except Exception as e:
        logging.error("An error occurred.", e)

    try:
        table_id = bqCreateTable(bq_client, dataset_id, table_name)
        logging.info("The following table {} was successfully created/retrieved.".format(table_name))
    except Exception as e:
        logging.error("An error occurred.", e)

    src_bucket = os.environ['SRC_BUCKET']
    dest_bucket = os.environ['DEST_BUCKET']
    gcs_source_prefix = 'pdf'
    lst_blobs = storage_client.list_blobs(bucket_or_name=src_bucket,
                                          prefix=gcs_source_prefix)

    for blob in lst_blobs:
        doc_title = blob.name.split('/')[-1].split('.txt')[0]

        # download as string
        it_raw_blob = storage_client.get_bucket(dest_bucket).get_blob('raw_txt/{}.txt'.format(doc_title))

        # set the GCS path
        try:
            # Path in case using batch translation
            path_blob_eng_raw = 'eng_txt/{}/{}_raw_txt_{}_en_translations.txt'.format(doc_title, dest_bucket, doc_title)
            eng_raw_blob = storage_client.get_bucket(dest_bucket).get_blob(path_blob_eng_raw)
            # If the file is not present, decoding a None Type will result in an error
            eng_raw_txt = eng_raw_blob.download_as_string().decode('utf-8')
        except:
            # New path used for pdf update
            path_blob_eng_raw = 'eng_txt/{}.txt'.format(doc_title)
            eng_raw_blob = storage_client.get_bucket(dest_bucket).get_blob(path_blob_eng_raw)

        # Upload blob of interest
        curated_eng_blob = storage_client.get_bucket(bucket_name) \
            .get_blob('curated_eng_txt/{}.txt'.format(doc_title))

        # populate to BQ dataset
        exportItems2BQ(bq_client, dataset_id, table_id, doc_title, it_raw_blob, eng_raw_blob, curated_eng_blob)
