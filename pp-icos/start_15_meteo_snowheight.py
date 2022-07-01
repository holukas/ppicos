import file_formats as ff
from main import format_to_icos

format_icos = format_to_icos(file_format=ff.f_15_meteo_snowheight(), max_age_days=8)

