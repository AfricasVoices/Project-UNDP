#!/usr/bin/env bash

set -e

if [[ $# -ne 4 ]]; then
    echo "Usage: ./4_coda_add.sh <coda-auth-file> <coda-v2-root> <data-root> <location>"
    echo "Uploads coded messages datasets from '<data-root>/Outputs/Coda Files' to Coda"
    exit
fi

AUTH=$1
CODA_V2_ROOT=$2
DATA_ROOT=$3
LOCATION=$4

./checkout_coda_v2.sh "$CODA_V2_ROOT"

PROJECT_NAME="UNDP_RCO"
DATASETS=(
    "s03e01_${LOCATION}"
    "s03e02_${LOCATION}"
    "s03e03_${LOCATION}"
    "s03e04_${LOCATION}"

    "location"
    "gender"
    "age"
    "recently_displaced"
    "in_idp_camp"

    "have_voice"
    "suggestions"
)

cd "$CODA_V2_ROOT/data_tools"

for DATASET in ${DATASETS[@]}
do
    echo "Pushing messages data to ${PROJECT_NAME}_${DATASET}..."

    pipenv run python add.py "$AUTH" "${PROJECT_NAME}_${DATASET}" messages "$DATA_ROOT/Outputs/Coda Files/$DATASET.json"
done
