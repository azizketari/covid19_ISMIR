from google.cloud import storage, bigquery, datastore
from google.oauth2 import service_account
from utils.bq_fcn import returnQueryResults
from utils.ner_fcn import getCases

import logging
import os

project_id = os.getenv('PROJECT_ID')
bucket_name = os.getenv('BUCKET_NAME')
location = os.getenv('LOCATION')
key_path = os.getenv('SA_KEY_PATH')
dataset_name = os.getenv('BQ_DATASET_NAME')
table_name = os.getenv('BQ_TABLE_NAME')
case_id = os.getenv('TEST_CASE')

credentials = service_account.Credentials.from_service_account_file(key_path)

bq_client = bigquery.Client(credentials=credentials,
                            project_id=project_id)

datastore_client = datastore.Client(credentials=credentials,
                                    project_id=project_id)

# Returns a list of results
try:
    results_lst = returnQueryResults(bq_client, project_id, dataset_name, table_name, case_id)
    logging.info("Here is the result of the test query: \n {}".format(results_lst))
except Exception, e:
    logging.error("Error", e)

try:
    filter_dict = {'Sign or Symptom':['onset symptoms', "chills"]}
    results = getCases(datastore_client, filter_dict, limit=10)
    logging.info("Here is the result of the test query: \n {}".format(results))
except Exception, e:
    logging.error("Error", e)



