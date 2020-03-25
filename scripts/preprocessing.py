from google.cloud import storage, translate
from google.oauth2 import service_account
from utils.preprocessing_fcn import batch_translate_text, upload_blob
import logging

import re
import time
import os

project_id = os.getenv('PROJECT_ID')
bucket_name = os.getenv('BUCKET_NAME')
location = os.getenv('LOCATION')
key_path = os.getenv('SA_KEY_PATH')

credentials = service_account.Credentials.from_service_account_file(key_path)

storage_client = storage.Client(credentials=credentials)

translate_client = translate.TranslationServiceClient(credentials=credentials)

lst_json_blobs = storage_client.list_blobs(bucket_or_name=bucket_name,
                                           prefix='json')

customize_stop_words = [
    'uoc', 'diagnostic', 'interventional', 'radiology', 'madonna', 'delle', 'grazie', 'hospital',
    'Borgheresi', 'Agostini', 'Ottaviani', 'Floridi', 'Giovagnoni', 'di', 'specialization',
    'Polytechnic', 'University', 'marche', 'ANCONA', 'Italy', 'Azienda', 'Ospedali',
    'Riuniti', 'Yorrette', 'Matera', 'Michele', 'Nardella', 'Gerardo', 'Costanzo',
    'Claudia', 'Lopez', 'st', 'a.', 'a', 'of', 's', 'cien', 'ze', 'diolog', 'ic', 'he',
    'â', '€', 's', 'b', 'case', 'Cuoladi', 'l', 'c', 'ra', 'bergamo', 'patelli', 'est', 'asst',
    'dr', 'Dianluigi', 'Svizzero', 'i', 'riccardo', 'Alessandro', 'Spinazzola', 'angelo',
    'maggiore', 'p', 'r', 't', 'm', 'en', 't', 'o', 'd', 'e', 'n', 'd', 'o', 'g', 'h', 'u'
]

start_time = time.time()
for blob in lst_json_blobs:
    doc_title = blob.name.split('/')[-1].split('-')[0]

    txt_gcs_dest_path = 'gs://' + bucket_name + '/raw_txt/' + doc_title + '.txt'
    eng_txt_gcs_dest_path = 'gs://' + bucket_name + '/eng_txt/{}/'.format(doc_title)
    processed_eng_gcs_dest_path = 'gs://' + bucket_name + '/curated_eng_txt/' + doc_title + '.txt'

    # Translate raw text to english
    try:
        batch_translate_text(project_id=project_id,
                             location=location,
                             input_uri=txt_gcs_dest_path,
                             output_uri=eng_txt_gcs_dest_path)
        logging.info("Translation of {} document was successful.".format(doc_title))
    except Exception, e:
        logging.error("Error", e)

    # Process eng raw text
    blob_prefix = 'eng_txt/{}/{}_raw_txt_{}_en_translations.txt'.format(doc_title,
                                                                        bucket_name,
                                                                        doc_title)

    eng_blob = storage_client.get_bucket(bucket_name).get_blob(blob_prefix)
    eng_raw_string = eng_blob.download_as_string().decode('utf-8')

    # Remove dates
    # 1 or 2 digit number followed by back slash followed by 1 or 2 digit number ...
    pattern_dates = '(\d{1,2})/(\d{1,2})/(\d{4})'
    pattern_fig = 'Figure (\d{1,2})'
    pattern_image = '^Image .$'
    replace = ''

    eng_raw_string = re.sub(pattern_dates, replace, eng_raw_string)
    eng_raw_string = re.sub(pattern_fig, replace, eng_raw_string)
    eng_raw_string = re.sub(pattern_image, replace, eng_raw_string)

    # remove punctuation and special characters
    eng_raw_string = re.sub('[^A-Za-z0-9]+', ' ', eng_raw_string)

    # Remove custom stop words
    tokens = [token for token in eng_raw_string.split() if token not in customize_stop_words]

    refined_doc = ''
    for word in tokens:
        refined_doc += ' {}'.format(word)

    # Upload raw text to GCS
    upload_blob(refined_doc, processed_eng_gcs_dest_path)
    logging.info("The curation of {} text completed successfully.".format(doc_title))

total_time = time.time() - start_time
logging.info('The translation and curation of all documents was successfully completed in {} minutes.'.format(
    round(total_time / 60, 1)))

