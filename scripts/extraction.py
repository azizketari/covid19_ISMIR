from google.cloud import storage, vision
from google.oauth2 import service_account
from utils.preprocessing_fcn import async_detect_document, read_json_result, upload_blob

import logging
import time
import os

project_id = os.getenv('PROJECT_ID')
bucket_name = os.getenv('BUCKET_NAME')
location = os.getenv('LOCATION')
key_path = os.getenv('SA_KEY_PATH')

credentials = service_account.Credentials.from_service_account_file(key_path)

storage_client = storage.Client(credentials=credentials)

vision_client = vision.ImageAnnotatorClient(credentials=credentials)

lst_pdf_blobs = storage_client.list_blobs(bucket_or_name=bucket_name,
                                          prefix='pdf')

lst_json_blobs = storage_client.list_blobs(bucket_or_name=bucket_name,
                                           prefix='json')

start_time = time.time()
nbr_documents = len(lst_pdf_blobs)
for blob in lst_pdf_blobs:
    doc_title = blob.name.split('/')[-1].split('.pdf')[0]

    # Generate all paths
    gcs_source_path = 'gs://' + bucket_name + '/' + blob.name
    json_gcs_dest_path = 'gs://' + bucket_name + '/json/' + blob.name

    # OCR pdf documents
    async_detect_document(vision_client,
                          gcs_source_path,
                          json_gcs_dest_path)

total_time = time.time() - start_time
logging.info("Vision API successfully completed OCR of all {} documents on {} minutes".format(nbr_documents,
                                                                                              round(total_time / 60,
                                                                                                    1)))

start_time = time.time()
for blob in lst_json_blobs:
    doc_title = blob.name.split('/')[-1].split('-')[0]

    # Define GCS paths
    json_gcs_dest_path = 'gs://' + bucket_name + '/{}'.format(blob.name)
    txt_gcs_dest_path = 'gs://' + bucket_name + '/raw_txt/' + doc_title + '.txt'

    # Parse json
    all_text = read_json_result(json_gcs_dest_path, doc_title)

    # Upload raw text to GCS
    upload_blob(all_text, txt_gcs_dest_path)

total_time = time.time() - start_time
logging.info(
    'Successful parsing of all {} documents resulting from Vision API on {} minutes'.format(round(total_time / 60, 1)))
