import file_formats as ff
from main import format_to_icos

format_icos = format_to_icos(file_format=ff.f_17_meteo_profile(), max_age_days=10)
