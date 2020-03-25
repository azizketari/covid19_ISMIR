from google.cloud import storage, translate, vision
from google.oauth2 import service_account
import logging
import os

from google.protobuf import json_format

# DEVELOPER: change path to key
project_id = os.getenv('PROJECT_ID')
bucket_name = os.getenv('BUCKET_NAME')
location = os.getenv('LOCATION')
key_path = os.getenv('SA_KEY_PATH')

# DEVELOPER: change path to key
credentials = service_account.Credentials.from_service_account_file(key_path)

storage_client = storage.Client(credentials=credentials,
                                project_id=credentials.project_id)

translate_client = translate.Client(credentials=credentials,
                                    project_id=credentials.project_id)

vision_client = vision.Client(credentials=credentials,
                              project_id=credentials.project_id)


def async_detect_document(vision_client, gcs_source_uri, gcs_destination_uri, batch_size=20):
    """
    OCR with PDF/TIFF as source files on GCS
    Args:
        vision_client:
        gcs_source_uri:
        gcs_destination_uri:
        batch_size: How many pages should be grouped into each json output file.

    Returns:

    """
    doc_title = gcs_source_uri.split('/')[-1].split('.pdf')[0]

    # Supported mime_types are: 'application/pdf' and 'image/tiff'
    mime_type = 'application/pdf'

    # Feature in vision API
    feature = vision.types.Feature(
        type=vision.enums.Feature.Type.DOCUMENT_TEXT_DETECTION)

    gcs_source = vision.types.GcsSource(uri=gcs_source_uri)
    input_config = vision.types.InputConfig(
        gcs_source=gcs_source, mime_type=mime_type)

    gcs_destination = vision.types.GcsDestination(uri=gcs_destination_uri)
    output_config = vision.types.OutputConfig(
        gcs_destination=gcs_destination, batch_size=batch_size)

    async_request = vision.types.AsyncAnnotateFileRequest(
        features=[feature], input_config=input_config,
        output_config=output_config)

    operation = vision_client.async_batch_annotate_files(
        requests=[async_request])

    # print('Waiting for the operation to finish.')
    operation.result(timeout=180)
    logging.info('Text extraction from document {} is completed.'.format(doc_title))


def read_json_result(bucket_name, doc_title):
    """
    Parsing the json files and extract text.
    Args:
        bucket_name:
        doc_title:

    Returns:
        all_text: str - Containing all text of the document
    """
    gcs_destination_prefix = 'json/' + '{}-'.format(doc_title)

    # List objects with the given prefix.
    blob_list = list(storage_client.list_blobs(bucket_or_name=bucket_name,
                                               prefix=gcs_destination_prefix))
    all_text = ''
    for blob in blob_list:

        json_string = blob.download_as_string()
        response = json_format.Parse(
            json_string, vision.types.AnnotateFileResponse())

        # The actual response for the first page of the input file.
        for response in response.responses:
            # first_page_response = response.responses[0]
            text_response = response.full_text_annotation.text
            all_text += text_response
            all_text += ' '

    logging.info("Parsing of {} json doc was successful.".format(doc_title))
    return all_text


def upload_blob(bucket_name, txt_content, destination_blob_name):
    """
    Uploads a file to the bucket.
    Args:
        bucket_name:
        txt_content:
        destination_blob_name:

    Returns:

    """
    destination_blob_name = destination_blob_name.split('gs://{}/'.format(bucket_name))[-1]
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(txt_content)

    print("Text uploaded to {}".format(destination_blob_name))


def batch_translate_text(project_id, location,
                         input_uri="gs://YOUR_BUCKET_ID/path/to/your/file.txt",
                         output_uri="gs://YOUR_BUCKET_ID/path/to/save/results/"):
    """
    Translates a batch of texts on GCS and stores the result in a GCS location.
    Args:
        project_id:
        location:
        input_uri:
        output_uri:

    Returns:

    """

    # Supported file types: https://cloud.google.com/translate/docs/supported-formats
    gcs_source = {"input_uri": input_uri}

    input_configs_element = {
        "gcs_source": gcs_source,
        "mime_type": "text/plain"  # Can be "text/plain" or "text/html".
    }
    gcs_destination = {"output_uri_prefix": output_uri}
    output_config = {"gcs_destination": gcs_destination}
    parent = translate_client.location_path(project_id, location)

    # Supported language codes: https://cloud.google.com/translate/docs/language
    operation = translate_client.batch_translate_text(
        parent=parent,
        source_language_code="it",
        target_language_codes=["en"],  # Up to 10 language codes here.
        input_configs=[input_configs_element],
        output_config=output_config)

    response = operation.result(180)
