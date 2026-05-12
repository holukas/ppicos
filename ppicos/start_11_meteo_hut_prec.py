import sys

from ppicos import filesettings
from ppicos.main import IcosFormat

MAX_AGE_DAYS = 14
FILESETTINGS = filesettings.f_11_meteo_hut_prec()

icosformat = IcosFormat(filesettings=FILESETTINGS, max_age_days=MAX_AGE_DAYS)
icosformat.run()
sys.exit()
