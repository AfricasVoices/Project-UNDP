#!/usr/bin/env bash

set -e

if [[ $# -ne 9 ]]; then
    echo "Usage: ./run_pipeline.sh"
    echo "  <user> <pipeline-configuration-json> <location>"
    echo "  <coda-pull-credentials-path> <coda-push-credentials-path> <avf-bucket-credentials-path>"
    echo "  <coda-tools-root> <data-root> <data-backup-dir>"
    echo "Runs the pipeline end-to-end (data fetch, coda fetch, output generation, Drive upload, Coda upload, data backup)"
    exit
fi

USER=$1
PIPELINE_CONFIGURATION=$2
LOCATION=$3
CODA_PULL_CREDENTIALS_PATH=$4
CODA_PUSH_CREDENTIALS_PATH=$5
AVF_BUCKET_CREDENTIALS_PATH=$6
CODA_TOOLS_ROOT=$7
DATA_ROOT=$8
DATA_BACKUPS_DIR=$9

./1_coda_get.sh "$CODA_PULL_CREDENTIALS_PATH" "$CODA_TOOLS_ROOT" "$DATA_ROOT" "$LOCATION"

./2_fetch_raw_data.sh "$USER" "$AVF_BUCKET_CREDENTIALS_PATH" "$PIPELINE_CONFIGURATION" "$DATA_ROOT"

./3_generate_outputs.sh "$USER" "$AVF_BUCKET_CREDENTIALS_PATH" "$PIPELINE_CONFIGURATION" "$DATA_ROOT"

./3_generate_outputs.sh "$USER" "$AVF_BUCKET_CREDENTIALS_PATH" "$PIPELINE_CONFIGURATION" "$DATA_ROOT"

./4_coda_add.sh "$CODA_PUSH_CREDENTIALS_PATH" "$CODA_TOOLS_ROOT" "$DATA_ROOT" "$LOCATION"

./5_backup_data_root.sh "$DATA_ROOT" "$DATA_BACKUPS_DIR"
