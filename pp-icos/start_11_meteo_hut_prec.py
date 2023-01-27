import sys

import file_formats as ff
from main import format_to_icos

max_age_days = 4

format_icos = format_to_icos(file_format=ff.f_11_meteo_hut_prec(), max_age_days=max_age_days)

sys.exit()
