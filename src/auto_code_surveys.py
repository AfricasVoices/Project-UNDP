import time
from os import path

from core_data_modules.cleaners import Codes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.logging import Logger
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCodaV2IO
from core_data_modules.util import IOUtils
from dateutil.parser import isoparse

from src.lib.code_schemes import CodeSchemes
from src.lib.pipeline_configuration import PipelineConfiguration

log = Logger(__name__)


class AutoCodeSurveys(object):
    SENT_ON_KEY = "sent_on"

    @classmethod
    def auto_code_surveys(cls, user, data, pipeline_configuration, coda_output_dir):
        # Auto-code surveys
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.cleaner is not None:
                    CleaningUtils.apply_cleaner_to_traced_data_iterable(user, data, plan.raw_field, cc.coded_field,
                                                                        cc.cleaner, cc.code_scheme)

        # Remove survey data sent after the project finished
        log.info("Hiding survey messages sent after the end of the project. These will not be exported in "
                 "production/analysis files")
        out_of_range_count = 0
        for td in data:
            for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
                if plan.time_field in td and isoparse(td[plan.time_field]) > pipeline_configuration.project_end_date:
                    out_of_range_count += 1
                    td.hide_keys({plan.raw_field, plan.time_field},
                                 Metadata(user, Metadata.get_call_location(), time.time()))
        log.info(f"Hid {out_of_range_count} survey messages sent after the end of the project")

        # For any locations where the cleaners assigned a code to a sub district, set the district code to NC
        # (this is because only one column should have a value set in Coda)
        for td in data:
            if "mogadishu_sub_district_coded" in td:
                mogadishu_code_id = td["mogadishu_sub_district_coded"]["CodeID"]
                if CodeSchemes.MOGADISHU_SUB_DISTRICT.get_code_with_id(mogadishu_code_id).code_type == "Normal":
                    nc_label = CleaningUtils.make_label_from_cleaner_code(
                        CodeSchemes.MOGADISHU_SUB_DISTRICT,
                        CodeSchemes.MOGADISHU_SUB_DISTRICT.get_code_with_control_code(Codes.NOT_CODED),
                        Metadata.get_call_location(),
                    )
                    td.append_data({"district_coded": nc_label.to_dict()},
                                   Metadata(user, Metadata.get_call_location(), time.time()))

        # Output survey responses to coda for manual verification + coding
        IOUtils.ensure_dirs_exist(coda_output_dir)
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            TracedDataCodaV2IO.compute_message_ids(user, data, plan.raw_field, plan.id_field)

            coda_output_path = path.join(coda_output_dir, plan.coda_filename)
            with open(coda_output_path, "w") as f:
                TracedDataCodaV2IO.export_traced_data_iterable_to_coda_2(
                    data, plan.raw_field, plan.time_field, plan.id_field,
                    {cc.coded_field: cc.code_scheme for cc in plan.coding_configurations},
                    f
                )

        return data
