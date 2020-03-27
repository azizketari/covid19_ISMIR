from google.cloud import storage, bigquery, datastore
from google.oauth2 import service_account
from utils.bq_fcn import populateBQ
from utils.ner_fcn import populateDatastore
import logging
import argparse
import os
import time

# Importing the models
logging.getLogger().setLevel(logging.INFO)

# Create the parser
parser = argparse.ArgumentParser(description='Select the model of interest.')

# Add the arguments
parser.add_argument('store_bigquery',
                    metavar='bool',
                    choices=['True', 'False'],
                    help='Store data in BigQuery. Options: True or False')

parser.add_argument('store_datastore',
                    metavar='bool',
                    choices=['True', 'False'],
                    help='Store data in Datastore. Options: True or False')

model_choices = ['en_core_sci_sm', 'en_core_sci_lg', 'en_ner_bc5cdr_md']
parser.add_argument('model_name',
                    metavar='name',
                    type=str,
                    help='Model options: en_core_sci_sm, en_core_sci_lg, en_ner_bc5cdr_md')

# Execute the parse_args() method
args = parser.parse_args()
if args.store_datastore == 'True' and not args.model_name:
    parser.error('--storing in datastore can only be done when --model_name is set to a specific model.')
elif args.store_datastore == 'True' and args.model_name not in model_choices:
    parser.error('--storing in datastore can only be done when --model_name is among the supported models: {}.'.format(model_choices))


model_name = args['model_name']
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

if args.store_bigquery == 'True':
    start_time = time.time()
    populateBQ(bq_client=bq_client,storage_client=storage_client,
               bucket_name=bucket_name, dataset_name=dataset_name,
               table_name=table_name)
    total_time = time.time() - start_time
    logging.info(
        'The export to BigQuery was completed successfully and took {} seconds.'.format(round(total_time, 1)))
else:
    logging.info('The export to BigQuery was disable.')

if args.store_datastore == 'True':
    start_time = time.time()
    populateDatastore(datastore_client=datastore_client, storage_client=storage_client,
                      bucket_name=bucket_name, model_name=model_name)
    total_time = time.time() - start_time
    logging.info(
        "The export to Datastore was completed successfully and took {} seconds.".format(round(total_time, 1)))

else:
    logging.info('The export to Datastore was disable.')
