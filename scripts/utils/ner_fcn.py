from google.cloud import datastore
from scispacy.umls_linking import UmlsEntityLinker
import logging
import pandas as pd
import re

def importModel(model_name):
    """
    Selective import of the required model from scispacy. These models are quite heavy, hence this function.
    Args:
        model_name: str -

    Returns:

    """
    if model_name == 'en_core_sci_sm':
        import en_core_sci_sm
    elif model_name == 'en_core_sci_lg':
        import en_core_sci_lg
    elif model_name == 'en_ner_bc5cdr_md':
        import en_ner_bc5cdr_md

def loadModel(model):
    """
    Loading Named Entity Recognition model.
    Args:
        model: options: en_core_sci_sm, en_core_sci_lg, en_ner_bc5cdr_md

    Returns:
        nlp: loaded model
        linker: loaded add-on
    """
    # Load the model
    nlp = model.load()

    # Add pipe features to pipeline
    linker = UmlsEntityLinker(resolve_abbreviations=True)
    nlp.add_pipe(linker)

    logging.info("Model and add-ons successfully loaded.")
    return nlp, linker


def extractMedEntities(vectorized_doc, linker):
    """
    Returns UMLS entities contained in a text.
    Args:
        vectorized_doc:
        linker:
    Returns:
        UMLS_tuis_entity: dict - key: entity and value: TUI code
    """
    # Pattern for TUI code
    pattern = 'T(\d{3})'

    UMLS_tuis_entity = {}
    entity_dict = {}

    for idx in range(len(vectorized_doc.ents)):
        entity = vectorized_doc.ents[idx]
        entity_dict[entity] = ''
        for umls_ent in entity._.umls_ents:
            entity_dict[entity] = linker.umls.cui_to_entity[umls_ent[0]]

        # RegEx expression if contains TUI code
        tui = re.search(pattern, str(entity_dict[entity]))
        if tui:
            UMLS_tuis_entity[str(entity)] = tui.group()
        else:
            UMLS_tuis_entity[str(entity)] = None

    return UMLS_tuis_entity


def addTask(datastore_client, doc_title, entities_dict):
    """
    Upload entities to Datastore.
    Args:
        datastore_client:
        doc_title:
        entities_dict:

    Returns:
        Datastore key object.
    """
    key = datastore_client.key('case', doc_title)
    task = datastore.Entity(key=key)
    task.update(
        entities_dict
    )
    datastore_client.put(task)
    # Then get by key for this entity
    logging.info("Uploaded {} to Datastore.".format(doc_title))
    return datastore_client.get(key)


def getCases(datastore_client, filter_dict, limit=10):
    """
    Get results of query with custom filters
    Args:
        datastore_client: Client object
        filter_dict: dict - e.g {parameter_A: [entity_name_A, entity_name_B],
                                parameter_B: [entitiy_name_C]
                                }
        limit: int - result limits per default 10
    Returns:
        results: list - query results
    """
    query = datastore_client.query(kind='case')

    for key, values in filter_dict.items():
        for value in values:
            query.add_filter(key, '=', value)
    results = list(query.fetch(limit=limit))
    return results


def populateDatastore(datastore_client, storage_client, model_name, src_bucket='aketari-covid19-data-update'):
    """
    Extract UMLS entities and store them in a No-SQL db: Datastore.
    Args:
        datastore_client: Storage client instantiation -
        storage_client: Storage client instantiation -
        model_name: str -
        src_bucket: str - contains pdf of the newest files
    Returns:
        Queriable database
    """

    lst_curated_blobs = storage_client.list_blobs(bucket_or_name=src_bucket)

    importModel(model_name)

    if model_name == 'en_core_sci_sm':
        nlp, linker = loadModel(model=en_core_sci_sm)
    elif model_name == 'en_core_sci_lg':
        nlp, linker = loadModel(model=en_core_sci_lg)
    elif model_name == 'en_ner_bc5cdr_md':
        nlp, linker = loadModel(model=en_ner_bc5cdr_md)
    else:
        return False

    for blob in lst_curated_blobs:
        doc_title = blob.name.split('/')[-1].split('.pdf')[0]

        # download as string
        eng_string = blob.download_as_string().decode('utf-8')

        # convert to vector
        doc = nlp(eng_string)

        # Extract medical entities
        UMLS_tuis_entity = extractMedEntities(doc, linker)

        # Mapping of UMLS entities with reference csv
        entities = list(UMLS_tuis_entity.keys())
        TUIs = list(UMLS_tuis_entity.values())
        df_entities = pd.DataFrame(data={'entity': entities, 'TUIs': TUIs})
        df_reference_TUIs = pd.read_csv('./scripts/utils/UMLS_tuis.csv')
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
