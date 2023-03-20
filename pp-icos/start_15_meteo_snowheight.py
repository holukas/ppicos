import sys

import filesettings
from main import IcosFormat

MAX_AGE_DAYS = 10
FILESETTINGS = filesettings.f_15_meteo_snowheight()

icosformat = IcosFormat(filesettings=FILESETTINGS, max_age_days=MAX_AGE_DAYS)
icosformat.run()
sys.exit()
