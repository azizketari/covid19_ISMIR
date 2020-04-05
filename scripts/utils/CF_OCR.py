import logging
import json
import os
import time

from google.cloud import pubsub_v1
from google.cloud import vision, storage
from google.protobuf import json_format

publisher_client = pubsub_v1.PublisherClient()
vision_client = vision.ImageAnnotatorClient()
storage_client = storage.Client()

project_id = os.environ['GCP_PROJECT']
RESULT_TOPIC = os.environ["RESULT_TOPIC"] #e.g pdf2text


def documentOCR(vision_client, gcs_source_uri, gcs_destination_uri, batch_size=20):
    """

    Args:
        vision_client:
        gcs_source_uri:
        gcs_destination_uri:
        batch_size:

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


def readJsonResult(storage_client, bucket_name, doc_title):
    """
    Parsing the json files and extract text.
    Args:
        storage_client:
        bucket_name:
        doc_title:

    Returns:
        all_text: str - Containing all text of the document
    """
    gcs_src_prefix = 'json/' + '{}-'.format(doc_title)

    # List objects with the given prefix.
    bucket_client = storage_client.get_bucket(bucket_name)
    blob_list = list(bucket_client.list_blobs(prefix=gcs_src_prefix))
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


def uploadBlob(storage_client, bucket_name, txt_content, destination_blob_name):
    """
    Uploads a file to the bucket.
    Args:
        storage_client:
        bucket_name:
        txt_content: str - text
        destination_blob_name: str - prefix

    Returns:

    """
    destination_blob_name = destination_blob_name.split('gs://{}/'.format(bucket_name))[-1]
    bucket_client = storage_client.bucket(bucket_name)
    blob = bucket_client.blob(destination_blob_name)

    blob.upload_from_string(txt_content)

    logging.info("Text uploaded to {}".format(destination_blob_name))


def publishMsg(text, doc_title, topic_name):
    """
    Publish message with text and filename.
    Args:
        text: str - Text contained in the document
        doc_title: str -
        topic_name: str -
    Returns:

    """

    # Compose the message to be sent to pubsub
    message = {
        'text': text,
        'doc_title': doc_title,
    }

    # Publish message to PubSub
    # Note: the message_data needs to be in bytestring
    # Refer to the documentation:
    # https://googleapis.dev/python/pubsub/latest/publisher/api/client.html
    message_data = json.dumps(message).encode('utf-8')
    topic_path = publisher_client.topic_path(project_id, topic_name)

    # Publish method returns a future instance
    future = publisher_client.publish(topic_path, data=message_data)

    # We need to call result method to extract the message ID
    # Refer to the documentation:
    # https://googleapis.dev/python/pubsub/latest/publisher/api/futures.html#google.cloud.pubsub_v1.publisher.futures.Future
    message_id = future.result()

    logging.info("Message id: {} was published in topic: {}".format(message_id, topic_name))


def processPDFFile(file, context):
    """
    This function will be triggered when a pdf file is uploaded to the GCS bucket of interest.
    Args:
        file (dict): Metadata of the changed file, provided by the triggering
                                 Cloud Storage event.
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to stdout and Stackdriver Logging
    """
    start_time = time.time()
    src_bucket = file.get('bucket')
    dest_bucket = 'covid19-repo-test'

    prefix_and_doc_title = file.get('name')
    doc_title = prefix_and_doc_title.split('/')[-1].split('.')[0]
    print('name is: {}'.format(prefix_and_doc_title))

    # Step 1: Call OCR helper function
    gcs_source_path = 'gs://' + src_bucket + '/' + prefix_and_doc_title
    print('source gcs path: {}'.format(gcs_source_path))
    print('=============================')
    json_gcs_dest_path = 'gs://' + dest_bucket + '/json/' + doc_title + '-'
    print('destination json path: {}'.format(json_gcs_dest_path))
    print('=============================')
    documentOCR(vision_client, gcs_source_path, json_gcs_dest_path)
    print("completed OCR!")
    print('=============================')
    # Step 2: Parse json file
    text = readJsonResult(storage_client, dest_bucket, doc_title)
    print("Completed json parsing!")
    print('=============================')
    # Step 3: Publish on pubsub
    topic_name = RESULT_TOPIC
    publishMsg(text, doc_title, topic_name)
    print("Completed pubsub messaging!")
    print('=============================')
    # Step 4: Save on GCS
    upload_dest_prefix = 'raw_txt/{}.txt'.format(doc_title)
    uploadBlob(storage_client, dest_bucket, text, upload_dest_prefix)
    print("Completed upload!")
    print('=============================')
    print('File {} processed.'.format(doc_title))
    end_time = time.time() - start_time
    logging.info("Completion of text_extract took: {} seconds".format(round(end_time,1)))
