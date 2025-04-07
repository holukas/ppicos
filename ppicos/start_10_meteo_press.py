import sys

import filesettings
from main import IcosFormat

MAX_AGE_DAYS = 2
FILESETTINGS = filesettings.f_10_meteo_press()

icosformat = IcosFormat(filesettings=FILESETTINGS, max_age_days=MAX_AGE_DAYS)
icosformat.run()
sys.exit()
