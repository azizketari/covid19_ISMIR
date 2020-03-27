from google.cloud import storage, bigquery, datastore
from google.oauth2 import service_account
from utils.bq_fcn import returnQueryResults, constructQuery
from utils.ner_fcn import getCases
import os
import logging
logging.getLogger().setLevel(logging.INFO)


project_id = os.getenv('PROJECT_ID')
bucket_name = os.getenv('BUCKET_NAME')
location = os.getenv('LOCATION')
key_path = os.getenv('SA_KEY_PATH')
dataset_name = os.getenv('BQ_DATASET_NAME')
table_name = os.getenv('BQ_TABLE_NAME')
case_id = os.getenv('TEST_CASE')

credentials = service_account.Credentials.from_service_account_file(key_path)

bq_client = bigquery.Client(credentials=credentials)

datastore_client = datastore.Client(credentials=credentials)

# Returns a list of results
try:
    query = constructQuery(column_lst=['*'], case_id='case23')
    results_lst = returnQueryResults(bq_client, query)
    logging.info("Here is the result of the test query: \n {}".format(results_lst))
except Exception as e:
    logging.error("Error", e)

try:
    filter_dict = {'Sign or Symptom': ['onset symptoms', "chills"]}
    results = getCases(datastore_client, filter_dict, limit=10)
    logging.info("Here is the result of the test query: \n {}".format(results))
except Exception as e:
    logging.error("Error", e)



