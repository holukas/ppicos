import sys

import filesettings
from main import IcosFormat

MAX_AGE_DAYS = 19
FILESETTINGS = filesettings.f_10_meteo()

icosformat = IcosFormat(filesettings=FILESETTINGS, max_age_days=MAX_AGE_DAYS)
icosformat.run()
sys.exit()
