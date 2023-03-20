import sys

import filesettings
from main import IcosFormat

MAX_AGE_DAYS = 10

icosformat = IcosFormat(filesettings=filesettings.f_12_meteo_forest_floor(forest_floor=1, table=1), max_age_days=MAX_AGE_DAYS)
icosformat.run()
icosformat = IcosFormat(filesettings=filesettings.f_12_meteo_forest_floor(forest_floor=2, table=1), max_age_days=MAX_AGE_DAYS)
icosformat.run()
icosformat = IcosFormat(filesettings=filesettings.f_12_meteo_forest_floor(forest_floor=3, table=1), max_age_days=MAX_AGE_DAYS)
icosformat.run()
icosformat = IcosFormat(filesettings=filesettings.f_12_meteo_forest_floor(forest_floor=4, table=1), max_age_days=MAX_AGE_DAYS)
icosformat.run()
icosformat = IcosFormat(filesettings=filesettings.f_12_meteo_forest_floor(forest_floor=5, table=1), max_age_days=MAX_AGE_DAYS)
icosformat.run()
sys.exit()
