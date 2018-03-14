#!/usr/bin/env bash
#python -m main \
#  --project open-targets \
#  --job_name open-targets-medline-process-test\
#  --runner DataflowRunner \
#  --temp_location gs://opentargets-library-tmp/temp-new \
#  --setup_file ./setup.py \
#  --worker_machine_type n1-highmem-16 \
#  --input_baseline gs://pubmed-medline/baseline/pubmed18n082*.xml.gz \
#  --output_enriched gs://medline-json/test/analyzed/pubmed18 \
#  --max_num_workers 3 \
#  --zone europe-west1-d

#  --requirements_file requirements.txt \


python2 -m main \
    --project siren-pubmed \
    --job_name siren-medline-nlp-test \
    --runner DataflowRunner \
    --temp_location gs://siren-pubmed-temp/output \
    --setup_file ./setup.py \
    --worker_machine_type n1-highmem-16 \
    --input_baseline gs://pubmed-medline/baseline/pubmed18n082*.xml.gz \
    --output_enriched gs://siren-pubmed/test/analyzed/pubmed18 \
    --max_num_workers 3 \
    --zone europe-west2-b
