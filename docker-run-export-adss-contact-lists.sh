#!/bin/bash

set -e

IMAGE_NAME=undp-rco

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile-cpu)
            PROFILE_CPU=true
            CPU_PROFILE_OUTPUT_PATH="$2"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done


# Check that the correct number of arguments were provided.
if [[ $# -ne 4 ]]; then
    echo "Usage: ./docker-run.sh
    [--profile-cpu <profile-output-path>]
    <traced-data-input-path> <phone-number-uuid-table-path> <bossaso-output-path> <baidoa-output-path>"
    exit
fi

# Assign the program arguments to bash variables.
INPUT_TRACED_DATA=$1
INPUT_PHONE_NUMBER_UUID_TABLE=$2
OUTPUT_BOSSASO=$3
OUTPUT_BAIDOA=$4

# Build an image for this pipeline stage.
docker build --build-arg INSTALL_CPU_PROFILER="$PROFILE_CPU" -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
if [[ "$PROFILE_CPU" = true ]]; then
    PROFILE_CPU_CMD="pyflame -o /data/cpu.prof -t"
    SYS_PTRACE_CAPABILITY="--cap-add SYS_PTRACE"
fi
CMD="pipenv run $PROFILE_CPU_CMD python -u export_adss_contact_lists.py \
    /data/traced-data.json /data/phone-number-uuid-table.json \
    /data/bossaso-phone-numbers.csv /data/baidoa-phone-numbers.csv
"
container="$(docker container create ${SYS_PTRACE_CAPABILITY} -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"

# Copy input data into the container
docker cp "$INPUT_TRACED_DATA" "$container:/data/traced-data.json"
docker cp "$INPUT_PHONE_NUMBER_UUID_TABLE" "$container:/data/phone-number-uuid-table.json"

# Run the container
docker start -a -i "$container"

# Copy the output data back out of the container
docker cp "$container:/data/bossaso-phone-numbers.csv" "$OUTPUT_BOSSASO"
docker cp "$container:/data/baidoa-phone-numbers.csv" "$OUTPUT_BAIDOA"

if [[ "$PROFILE_CPU" = true ]]; then
    mkdir -p "$(dirname "$CPU_PROFILE_OUTPUT_PATH")"
    docker cp "$container:/data/cpu.prof" "$CPU_PROFILE_OUTPUT_PATH"
fi

# Tear down the container, now that all expected output files have been copied out successfully
docker container rm "$container" >/dev/null
