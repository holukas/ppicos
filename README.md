# pp-icos

`pp-icos` reads raw data files recorded at the ICOS site CH-DAV and converts 
their formats to ICOS-conform file formats.

**No raw data values are changed during this process.**

Modifications of the raw data files are limited to (with examples):
- **Renaming of filenames**: `CH-DAV_iDL_T1_35_1_TBL1_2018_08_17_0000.dat` is changed to `CH-Dav_BM_20180817_L02_F03.csv`
- **Renaming of columns**: `tre200s0` is changed to `TA_3_1_1`
- **Renaming of columns**: `_Avg` suffix is removed from original variable name
- **Compressing files:** `CH-Dav_BM_20180817_L02_F03.csv` is compressed to `CH-Dav_BM_20180817_L02_F03.zip`
- **Formatting of timestamps**: `%Y-%m-%d %H:%M:%S` is formatted to `%Y%m%d%H%M%S`
- **Limiting time range of files**: some external data providers transfer more than one day of data each day. These
files are modified to contain data only from the most recent day before the files are transferred to ICOS.

In the source folder `pp-icos`, the `start_*.py` files are the scripts that start the conversion of a specific
filetype (e.g., `10_meteo` files) to ICOS-conform formats. These start scripts are executed automatically each
day. The resulting ICOS-conform files are then moved to a separate folder, from where they are picked up by
another script and transferred to the ICOS server.
