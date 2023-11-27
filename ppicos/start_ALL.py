import datetime
import sys

import filesettings
from main import IcosFormat

script_start = datetime.datetime.now()

MAX_AGE_DAYS = 28

loop_settings = {
    '10_meteo': filesettings.f_10_meteo(),
    '10_meteo_heatflag_sonic': filesettings.f_10_meteo_heatflag_sonic(),
    '11_meteo_hut_prec': filesettings.f_11_meteo_hut_prec(),
    '12_meteo_forest_floor_1': filesettings.f_12_meteo_forest_floor(forest_floor=1, table=1),
    '12_meteo_forest_floor_2': filesettings.f_12_meteo_forest_floor(forest_floor=2, table=1),
    '12_meteo_forest_floor_3': filesettings.f_12_meteo_forest_floor(forest_floor=3, table=1),
    '12_meteo_forest_floor_4': filesettings.f_12_meteo_forest_floor(forest_floor=4, table=1),
    '12_meteo_forest_floor_5': filesettings.f_12_meteo_forest_floor(forest_floor=5, table=1),
    '13_meteo_backup_eth': filesettings.f_13_meteo_backup_eth(),
    '13_meteo_meteoswiss': filesettings.f_13_meteo_meteoswiss(),
    '13_meteo_nabel': filesettings.f_13_meteo_nabel(),
    '15_meteo_snowheight': filesettings.f_15_meteo_snowheight(),
    '17_meteo_profile': filesettings.f_17_meteo_profile(),
    '30_profile_ghg': filesettings.f_30_profile_ghg()
}

# Loop through settings
run_successful = []
run_not_successful = []
for filetype, filetypesettings in loop_settings.items():
    try:
        icosformat = IcosFormat(filesettings=filetypesettings, max_age_days=MAX_AGE_DAYS)
        icosformat.run()
        run_successful.append(filetype)
    except:
        run_not_successful.append(filetype)

# Runtime
total_seconds = datetime.datetime.now() - script_start
print(f"\n\n\n{'=' * 40}\nRuntime for all filetypes: {total_seconds}")
print("\nSuccesful ppicos runs:")
[print(f"    OK:  {r}") for r in run_successful]
print("\nNOT succesful ppicos runs:")
[print(f"    NOT OK:  {r}") for r in run_not_successful]

sys.exit()
