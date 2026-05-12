import sys

from ppicos import filesettings
from ppicos.main import IcosFormat

MAX_AGE_DAYS = 14
FILESETTINGS = filesettings.f_30_profile_ghg()

icosformat = IcosFormat(filesettings=FILESETTINGS, max_age_days=MAX_AGE_DAYS)
icosformat.run()
sys.exit()
