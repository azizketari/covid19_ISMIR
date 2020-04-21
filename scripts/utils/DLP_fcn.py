import base64
import logging

def getKeyNamePath(kms_client, project_id, location, key_ring, key_name):
    """

    Args:
        kms_client: Client instantiation
        project_id: str -
        location: str -
        key_ring: str -
        key_name: str -

    Returns: key_name: str - 'projects/YOUR_GCLOUD_PROJECT/locations/YOUR_LOCATION/keyRings/YOUR_KEY_RING/cryptoKeys
    /YOUR_CRYPTO_KEY
    """
    key_name_path = kms_client.crypto_key_path_path(project_id=project_id,
                                                    location=location,
                                                    key_ring=key_ring,
                                                    crypto_key_path=key_name)
    return key_name_path

def encryptData(kms_client, data, key_path_name):
    """

    Args:
        kms_client:
        data:
        key_path_name: str - 'projects/YOUR_GCLOUD_PROJECT/locations/YOUR_LOCATION/keyRings/YOUR_KEY_RING/cryptoKeys
    /YOUR_CRYPTO_KEY'

    Returns:
            response: str -

    """
    response = kms_client.encrypt(key_path_name, data.encode('utf-8'))
    return response

def decryptData(kms_client, data, key_path_name):
    """

        Args:
            kms_client: Instantiated client
            data: encrypted string
            key_path_name: str - 'projects/YOUR_GCLOUD_PROJECT/locations/YOUR_LOCATION/keyRings/YOUR_KEY_RING/cryptoKeys
    /YOUR_CRYPTO_KEY'

        Returns:
            response: str -
        """
    response = kms_client.decrypt(key_path_name, data.encode('utf-8'))
    return response

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


