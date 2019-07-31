import argparse
import csv
import json

from core_data_modules.logging import Logger
from core_data_modules.traced_data import TracedData
from core_data_modules.util import PhoneNumberUuidTable

from src.lib.code_schemes import CodeSchemes

Logger.set_project_name("UNDP-RCO")
log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generates lists of phone numbers of ADSS respondents who reported "
                                                 "living in baidoa or bossaso")

    parser.add_argument("traced_data_path", metavar="traced-data-path",
                        help="Path to the ADSS traced data file (either messages or individuals) to extract phone "
                             "numbers from")
    parser.add_argument("phone_number_uuid_table_path", metavar="phone-number-uuid-table-path",
                        help="JSON file containing the phone number <-> UUID lookup table for the messages/surveys "
                             "datasets")
    parser.add_argument("bossaso_output_path", metavar="bossaso-output-path",
                        help="CSV file to write the ADSS contacts from Bossaso to")
    parser.add_argument("baidoa_output_path", metavar="baidoa-output-path",
                        help="CSV file to write the ADSS contacts from Baidoa to")

    args = parser.parse_args()

    traced_data_path = args.traced_data_path
    phone_number_uuid_table_path = args.phone_number_uuid_table_path
    bossaso_output_path = args.bossaso_output_path
    baidoa_output_path = args.baidoa_output_path

    # Load the phone number <-> uuid table
    log.info(f"Loading the phone number <-> uuid table from file '{phone_number_uuid_table_path}'...")
    with open(phone_number_uuid_table_path, "r") as f:
        phone_number_uuid_table = PhoneNumberUuidTable.load(f)
    log.info(f"Loaded {len(phone_number_uuid_table.numbers())} contacts")
    
    # Load the ADSS traced data
    log.info(f"Loading ADSS traced data from file '{traced_data_path}'...")
    with open(traced_data_path, "r") as f:
        # Manually deserialise the traced data because ADSS used an older serialiser
        data = [TracedData.deserialize(d) for d in json.load(f)]
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
    bossaso_phone_numbers = [f"+{phone_number_uuid_table.get_phone(uuid)}" for uuid in bossaso_uuids]
    baidoa_phone_numbers = [f"+{phone_number_uuid_table.get_phone(uuid)}" for uuid in baidoa_uuids]

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
