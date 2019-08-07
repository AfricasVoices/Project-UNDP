import argparse
import csv
import json

from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from storage.google_cloud import google_cloud_utils

from src.lib import PipelineConfiguration
from src.lib.code_schemes import CodeSchemes

Logger.set_project_name("UNDP-RCO")
log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates lists of phone numbers of UNDP-RCO respondents who "
                                                 "reported living in baidoa or bossaso")

    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file")
    parser.add_argument("traced_data_path", metavar="traced-data-path",
                        help="Path to the UNDP-RCO traced data *messages* file to extract phone "
                             "numbers from")
    parser.add_argument("bossaso_output_path", metavar="bossaso-output-path",
                        help="CSV file to write the ADSS contacts from Bossaso to")
    parser.add_argument("baidoa_output_path", metavar="baidoa-output-path",
                        help="CSV file to write the ADSS contacts from Baidoa to")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    traced_data_path = args.traced_data_path
    bossaso_output_path = args.bossaso_output_path
    baidoa_output_path = args.baidoa_output_path

    # Read the settings from the configuration file
    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)

    log.info("Downloading Firestore UUID Table credentials...")
    firestore_uuid_table_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        pipeline_configuration.phone_number_uuid_table.firebase_credentials_file_url
    ))

    phone_number_uuid_table = FirestoreUuidTable(
        pipeline_configuration.phone_number_uuid_table.table_name,
        firestore_uuid_table_credentials,
        "avf-phone-uuid-"
    )
    log.info("Initialised the Firestore UUID table")

    log.info(f"Loading UNDP-RCO traced data from file '{traced_data_path}'...")
    with open(traced_data_path, "r") as f:
        data = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
    log.info(f"Loaded {len(data)} traced data objects")
        
    # Search the TracedData for the bossaso/baidoa contacts
    bossaso_uuids = set()
    baidoa_uuids = set()
    log.info("Searching for participants from Bossaso or Baidoa")
    for td in data:
        if td["district_coded"] == "STOP":
            continue

        if td["district_coded"]["CodeID"] == CodeSchemes.SOMALIA_DISTRICT.get_code_with_match_value("bossaso").code_id:
            bossaso_uuids.add(td["uid"])
        elif td["district_coded"]["CodeID"] == CodeSchemes.SOMALIA_DISTRICT.get_code_with_match_value("baidoa").code_id:
            baidoa_uuids.add(td["uid"])
    log.info(f"Found {len(bossaso_uuids)} contacts from Bossaso, and {len(baidoa_uuids)} contacts from Baidoa")

    # Convert the uuids to phone numbers
    log.info("Converting the uuids to phone numbers...")
    uuids_to_phone_numbers = phone_number_uuid_table.uuid_to_data_batch(list(bossaso_uuids) + list(baidoa_uuids))
    bossaso_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in bossaso_uuids]
    baidoa_phone_numbers = [f"+{uuids_to_phone_numbers[uuid]}" for uuid in baidoa_uuids]

    # Export CSVs
    def export_numbers_to_csv(numbers, csv_path):
        log.warning(f"Exporting {len(numbers)} phone numbers to {csv_path}...")
        with open(csv_path, "w") as f:
            writer = csv.DictWriter(f, fieldnames=["URN:Tel", "Name"], lineterminator="\n")
            writer.writeheader()

            for n in numbers:
                writer.writerow({
                    "URN:Tel": n
                })
            log.info(f"Wrote {len(numbers)} contacts to {csv_path}")

    log.info(f"Exporting Bossaso contacts to {bossaso_output_path}...")
    export_numbers_to_csv(bossaso_phone_numbers, bossaso_output_path)

    log.info(f"Exporting Baidoa contacts to {baidoa_output_path}...")
    export_numbers_to_csv(baidoa_phone_numbers, baidoa_output_path)
