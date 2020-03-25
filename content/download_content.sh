gsutil -m cp -r gs://covid19-public-dataset-aketari/pdf/* gs://$BUCKET_NAME/pdf/
gsutil -m cp -r gs://covid19-public-dataset-aketari/scispacy_models/en_core_sci_lg-0.2.4.tar.gz ./scispacy_models/en_core_sci_lg-0.2.4.tar.gz
gsutil -m cp -r gs://covid19-public-dataset-aketari/scispacy_models/en_core_sci_sm-0.2.4.tar.gz ./scispacy_models/en_core_sci_sm-0.2.4.tar.gz
