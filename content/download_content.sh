gsutil -m cp -r gs://covid19-public-dataset-aketari/pdf/* gs://$BUCKET_NAME/pdf/
gsutil -m cp -r gs://covid19-public-dataset-aketari/scispacy_models/en_core_sci_lg-0.2.4.tar.gz ./content/scispacy_models/en_core_sci_lg-0.2.4.tar.gz
gsutil -m cp -r gs://covid19-public-dataset-aketari/scispacy_models/en_core_sci_sm-0.2.4.tar.gz ./content/scispacy_models/en_core_sci_sm-0.2.4.tar.gz
gsutil -m cp -r gs://covid19-public-dataset-aketari/scispacy_models/en_ner_bc5cdr_md-0.2.4.tar.gz ./content/scispacy_models/en_ner_bc5cdr_md-0.2.4.tar.gz

# Installing all supported NER models
pip install -U ./content/scispacy_models/en_core_sci_sm-0.2.4.tar.gz
pip install -U ./content/scispacy_models/en_core_sci_lg-0.2.4.tar.gz
pip install -U ./content/scispacy_models/en_ner_bc5cdr_md-0.2.4.tar.gz