import sys

import filesettings
from main import IcosFormat

MAX_AGE_DAYS = 20
FILESETTINGS = filesettings.f_13_meteo_nabel()

icosformat = IcosFormat(filesettings=FILESETTINGS, max_age_days=MAX_AGE_DAYS)
icosformat.run()
sys.exit()
