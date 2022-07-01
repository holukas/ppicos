import sys

import file_formats as ff
from main import format_to_icos

format_icos = format_to_icos(file_format=ff.f_13_meteo_nabel(), max_age_days=10)

sys.exit()
