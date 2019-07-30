#!/usr/bin/env bash

set -e

if [[ $# -ne 4 ]]; then
    echo "Usage: ./1_coda_get.sh <coda-auth-file> <coda-v2-root> <data-root> <location>"
    echo "Downloads coded messages datasets from Coda to '<data-root>/Coded Coda Files'"
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
)

cd "$CODA_V2_ROOT/data_tools"
mkdir -p "$DATA_ROOT/Coded Coda Files"

for DATASET in ${DATASETS[@]}
do
    echo "Getting messages data from ${PROJECT_NAME}_${DATASET}..."

    pipenv run python get.py "$AUTH" "${PROJECT_NAME}_${DATASET}" messages >"$DATA_ROOT/Coded Coda Files/$DATASET.json"
done
