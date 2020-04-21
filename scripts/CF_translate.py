import base64
import json
import os
import re
import time
import logging

from google.cloud import pubsub_v1, translate, storage
import google.cloud.dlp

def doTranslation(translate_client, project_id, text, src_lang="it", target_lang="en-US"):
    """

    Args:
        text: str -
        src_lang: str - default it
        target_lang: str - default en

    Returns:
        translated_txt: txt - response from translate API
    """
    logging.info('Translating text into {}.'.format(target_lang))

    parent = translate_client.location_path(project_id, location="global")

    # Detail on supported types can be found here:
    # https://cloud.google.com/translate/docs/supported-formats
    translated_dict = translate_client.translate_text(parent=parent,
                                                      contents=[text],
                                                      mime_type="text/plain",
                                                      source_language_code=src_lang,
                                                      target_language_code=target_lang)

    for translation in translated_dict.translations:
        translated_txt = translation.translated_text
    return translated_txt


def publishMsg(publisher_client, project_id, text, doc_title, topic_name):
    """
    Publish message with text and doc_title.
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


def cleanEngText(eng_raw_string, customize_stop_words=[]):
    """

    Args:
        eng_raw_string: str -
        customize_stop_words: list - all stopwords to remove

    Returns:
        refined_doc: str - curated string of eng text
    """

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
    eng_raw_string = re.sub("[^A-Za-z0-9]+", ' ', eng_raw_string)

    # Remove custom stop words
    tokens = [token for token in eng_raw_string.split() if token not in customize_stop_words]

    refined_doc = ''
    for word in tokens:
        refined_doc += ' {}'.format(word)

    return refined_doc


def deterministicDeidentifyWithFpe(dlp_client, parent, text, info_types, surrogate_type, wrapped_key=None):
    """Uses the Data Loss Prevention API to deidentify sensitive data in a
    string using Format Preserving Encryption (FPE).
    Args:
        dlp_client: DLP Client instantiation
        parent: str - The parent resource name, for example projects/my-project-id.
        text: str - text to deidentify
        info_types: list type of sensitive data, such as a name, email address, telephone number, identification number,
        or credit card number.  https://cloud.google.com/dlp/docs/infotypes-reference
        surrogate_type: The name of the surrogate custom info type to use. Only
            necessary if you want to reverse the deidentification process. Can
            be essentially any arbitrary string, as long as it doesn't appear
            in your dataset otherwise.
        wrapped_key: The encrypted ('wrapped') AES-256 key to use. This key
            should be encrypted using the Cloud KMS key specified by key_name.
    Returns:
        None; the response from the API is printed to the terminal.
    """
    # The wrapped key is base64-encoded, but the library expects a binary
    # string, so decode it here.
    wrapped_key = base64.b64decode(wrapped_key)

    # Construct inspect configuration dictionary
    inspect_config = {
        "info_types": [{"name": info_type} for info_type in info_types]
    }

    # Construct deidentify configuration dictionary
    deidentify_config = {
        "info_type_transformations": {
            "transformations": [
                {
                    "primitive_transformation": {
                        "crypto_deterministic_config": {
                            "crypto_key": {
                                "unwrapped": {
                                    "key": wrapped_key
                                }
                            },
                            'surrogate_info_type': {"name": surrogate_type}
                        },

                    }
                }
            ]
        }
    }

    # Convert string to item
    item = {"value": text}

    # Call the API
    response = dlp_client.deidentify_content(
        parent=parent,
        inspect_config=inspect_config,
        deidentify_config=deidentify_config,
        item=item,
    )

    # Print results
    logging.info('Successful Redaction.')
    return response.item.value


def translateAndRefine(event, context):
    """
    This Cloud Function will be triggered when a message is published on the
    PubSub topic of interest. It will call Translate API.
    args:
        event (dict): Metadata of the event, received from Pub/Sub.
        context (google.cloud.functions.Context): Metadata of triggering event.
    returns:
        None; the output is written to stdout and Stackdriver Logging
    """
    # INSTANTIATION
    translate_client = translate.TranslationServiceClient()
    storage_client = storage.Client()
    dlp_client = google.cloud.dlp_v2.DlpServiceClient()

    # SET VARIABLES
    project_id = os.environ['GCP_PROJECT']
    location = 'global' # or you can set it to os.environ['LOCATION']

    start_time = time.time()
    if event.get('data'):
        message_data = base64.b64decode(event['data']).decode('utf-8')
        message = json.loads(message_data)
    else:
        raise ValueError('Data sector is missing in the Pub/Sub message.')

    it_text = message.get('text')
    doc_title = message.get('doc_title')
    dest_bucket = 'aketari-covid19-data'

    # Step 1: Call Translate API
    raw_eng_text = doTranslation(translate_client,project_id, it_text)
    print("Completed translation step!")
    print('=============================')

    # Step 2: Clean eng text
    curated_eng_text = cleanEngText(raw_eng_text)
    print("Completed english curation step!")
    print('=============================')

    # Step 3: Redact text
    parent = "{}/{}".format(project_id, location)
    # TODO: replace gcs_prefix_secret with the correct location
    gcs_prefix_secret = 'path/to/your/secret_file.txt'
    INFO_TYPES = ["FIRST_NAME", "LAST_NAME", "FEMALE_NAME", "MALE_NAME",
                  "PERSON_NAME", "STREET_ADDRESS", "ITALY_FISCAL_CODE"]
    bucket_client = storage_client.get_bucket(dest_bucket)
    AES_bytes = bucket_client.blob(gcs_prefix_secret).download_as_string().encode('utf-8')
    base64_AES_bytes = base64.b64encode(AES_bytes)
    redacted_text = deterministicDeidentifyWithFpe(dlp_client=dlp_client, parent=parent,
                                                   text=text, info_types=INFO_TYPES,
                                                   surrogate_type="REDACTED",
                                                   b64encoded_bytes=base64_AES_bytes)

    print("Completed redaction step!")
    print('=============================')

    # Step 4: Upload translated text
    prefix_raw_eng_txt = 'eng_txt/{}.txt'.format(doc_title)
    uploadBlob(storage_client, dest_bucket, raw_eng_text, prefix_raw_eng_txt)

    prefix_curated_eng_txt = 'curated_eng_txt/{}.txt'.format(doc_title)
    uploadBlob(storage_client, dest_bucket, curated_eng_text, prefix_curated_eng_txt)

    prefix_redacted_eng_txt = 'redacted_raw_eng_txt/{}.txt'.format(doc_title)
    uploadBlob(storage_client, dest_bucket, redacted_text, prefix_redacted_eng_txt)
    print("Completed upload step!")
    print('=============================')

    end_time = time.time() - start_time
    logging.info("Completion of text_extract took: {} seconds".format(round(end_time, 1)))
