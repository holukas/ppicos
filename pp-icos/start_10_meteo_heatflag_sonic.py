import file_formats as ff
from main import format_to_icos

format_icos = format_to_icos(file_format=ff.f_10_meteo_heatflag_sonic(), max_age_days=10)
