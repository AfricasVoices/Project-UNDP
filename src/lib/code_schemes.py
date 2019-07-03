import json

from core_data_modules.data_models import Scheme


def _open_scheme(filename):
    with open(f"code_schemes/{filename}", "r") as f:
        firebase_map = json.load(f)
        return Scheme.from_firebase_map(firebase_map)


class CodeSchemes(object):
    SOMALIA_OPERATOR = _open_scheme("somalia_operator.json")

    S03E01_BOSSASO_REASONS = _open_scheme("s03e01_bossaso_reasons.json")
    S03E02_BOSSASO_REASONS = _open_scheme("s03e02_bossaso_reasons.json")
    S03E03_BOSSASO_REASONS = _open_scheme("s03e03_bossaso_reasons.json")
    S03E04_BOSSASO_REASONS = _open_scheme("s03e04_bossaso_reasons.json")

    AGE = _open_scheme("age.json")
    GENDER = _open_scheme("gender.json")
    RECENTLY_DISPLACED = _open_scheme("recently_displaced.json")
