from google.cloud import storage, bigquery, datastore
from google.oauth2 import service_account
from utils.bq_fcn import bqCreateDataset, bqCreateTable, exportItems2BQ
from utils.ner_fcn import loadModel, addTask, extractMedEntities

import logging
logging.getLogger().setLevel(logging.INFO)

try:
    import en_core_sci_sm
except:
    logging.warning("404: en_core_sci_sm NOT FOUND. Make sure the model was downloaded and installed.")

try:
    import en_core_sci_lg
except:
    logging.warning("404: en_core_sci_lg NOT FOUND. Make sure the model was downloaded and installed.")
try:
    import en_ner_bionlp13cg_md
except:
    logging.warning("404: en_ner_bionlp13cg_md NOT FOUND. Make sure the model was downloaded and installed.")


import time
import os
import pandas as pd

project_id = os.getenv('PROJECT_ID')
bucket_name = os.getenv('BUCKET_NAME')
location = os.getenv('LOCATION')
key_path = os.getenv('SA_KEY_PATH')
dataset_name = os.getenv('BQ_DATASET_NAME')
table_name = os.getenv('BQ_TABLE_NAME')

credentials = service_account.Credentials.from_service_account_file(key_path)

storage_client = storage.Client(credentials=credentials)

datastore_client = datastore.Client(credentials=credentials)

bq_client = bigquery.Client(credentials=credentials)

gcs_source_prefix = 'raw_txt'
lst_blobs = storage_client.list_blobs(bucket_or_name=bucket_name,
                                      prefix=gcs_source_prefix)

start_time = time.time()

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

for blob in lst_blobs:
    doc_title = blob.name.split('/')[-1].split('.txt')[0]

    # download as string
    it_raw_blob = storage_client.get_bucket(bucket_name).get_blob('raw_txt/{}.txt'.format(doc_title))

    # set the GCS path
    path_blob_eng_raw = 'eng_txt/{}/{}_raw_txt_{}_en_translations.txt'.format(doc_title, bucket_name, doc_title)
    eng_raw_blob = storage_client.get_bucket(bucket_name).get_blob(path_blob_eng_raw)

    # Upload blob of interest
    curated_eng_blob = storage_client.get_bucket(bucket_name) \
        .get_blob('curated_eng_txt/{}.txt'.format(doc_title))

    # populate to BQ dataset
    exportItems2BQ(bq_client, dataset_id, table_id, doc_title, it_raw_blob, eng_raw_blob, curated_eng_blob)

total_time = time.time() - start_time
logging.info('The export to BigQuery was completed successfully and took {} minutes.'.format(round(total_time / 60, 1)))

curated_gcs_source_prefix = 'curated_eng_txt'
lst_curated_blobs = storage_client.list_blobs(bucket_or_name=bucket_name,
                                              prefix=curated_gcs_source_prefix)

nlp = loadModel(model=en_core_sci_sm)

start_time = time.time()
for blob in lst_curated_blobs:
    doc_title = blob.name.split('/')[-1].split('.txt')[0]

    # download as string
    eng_string = blob.download_as_string().decode('utf-8')

    # convert to vector
    doc = nlp(eng_string)

    # Extract medical entities
    UMLS_tuis_entity = extractMedEntities(doc)

    # Generate dataframes
    entities = list(UMLS_tuis_entity.keys())
    TUIs = list(UMLS_tuis_entity.values())
    df_entities = pd.DataFrame(data={'entity': entities, 'TUIs': TUIs})
    df_reference_TUIs = pd.read_csv('~/data/UMLS_tuis.csv')
    df_annotated_text_entities = pd.merge(df_entities, df_reference_TUIs, how='inner', on=['TUIs'])

    # Upload entities to datastore
    entities_dict = {}
    for idx in range(df_annotated_text_entities.shape[0]):
        category = df_annotated_text_entities.iloc[idx].values[2]
        med_entity = df_annotated_text_entities.iloc[idx].values[0]

        # Append to list of entities if the key,value pair already exist
        try:
            entities_dict[category].append(med_entity)
        except:
            entities_dict[category] = []
            entities_dict[category].append(med_entity)

        # API call
    key = addTask(datastore_client, doc_title, entities_dict)
    logging.info('The upload of {} entities is done.'.format(doc_title))

total_time = time.time() - start_time
logging.info(
    "The export to Datastore was completed successfully and took {} minutes.".format(round(total_time / 60, 1)))



