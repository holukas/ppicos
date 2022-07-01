import sys

import file_formats as ff
from main import format_to_icos

max_age_days = 4

format_icos = format_to_icos(file_format=ff.f_12_meteo_forest_floor(forest_floor=1, table=1), max_age_days=max_age_days)
format_icos = format_to_icos(file_format=ff.f_12_meteo_forest_floor(forest_floor=2, table=1), max_age_days=max_age_days)
format_icos = format_to_icos(file_format=ff.f_12_meteo_forest_floor(forest_floor=3, table=1), max_age_days=max_age_days)
format_icos = format_to_icos(file_format=ff.f_12_meteo_forest_floor(forest_floor=4, table=1), max_age_days=max_age_days)
format_icos = format_to_icos(file_format=ff.f_12_meteo_forest_floor(forest_floor=5, table=1), max_age_days=max_age_days)

sys.exit()
