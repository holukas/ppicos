![](images/logo_ppicos1_256px.png)

# ppicos

`ppicos` (**p**ost-**p**rocessing for ICOS) reads raw data files recorded at the ICOS site CH-DAV and converts
their formats to ICOS-conform file formats.

**No raw data values are changed during this process.**

## File modifications

Modifications of the raw data files are limited to (with examples):

- **Renaming of filenames**: `CH-DAV_iDL_T1_35_1_TBL1_2018_08_17_0000.dat` is changed to
  `CH-Dav_BM_20180817_L02_F03.csv`. In this example the file was renamed and the logger number and file number
  were added to the filename.
- **Renaming of columns**: `tre200s0` is changed to `TA_3_1_1`. This is necessary because external data providers
  have established variable names that have been in use for decades.
- **Renaming of columns**: `_Avg` suffix is removed from original variable name.
- **Compressing files:** `CH-Dav_BM_20180817_L02_F03.csv` is compressed to `CH-Dav_BM_20180817_L02_F03.zip`
- **Formatting of timestamps**: `%Y-%m-%d %H:%M:%S` is formatted to `%Y%m%d%H%M%S`
- **Limiting time range of files**: some external data providers transfer more than one day of data each day. These
  files are modified to contain data from the most recent day only before the files are transferred to ICOS.

In the source folder `ppicos`, the `start_*.py` files are the scripts that start the conversion of a specific
filetype (e.g., `10_meteo` files) to ICOS-conform formats. These start scripts are executed automatically each
day. The resulting ICOS-conform files are then moved to a separate folder, from where they are picked up by
another script and transferred to the ICOS server.

## File settings

The file settings in `filesettings.py` define how the respective filetype is modified.

- `DATA_COMPLEMENT_WITH_PREVIOUS_DATE`: `True` or `False`
- `DATA_HEADER_OUTPUT_TO_FILE`: `True` or `False`
- `DATA_HEADER_REMOVE_SUFFIX_FROM_VARIABLE_NAMES`: xxx, e.g. `['_Avg']`
- `DATA_HEADER_ROWS`: xxx, e.g. `[1, 2]`
- `DATA_ICOS_TIMESTAMP_FORMAT`: xxx, e.g. `'%Y%m%d%H%M'`
- `DATA_KEEP_ONLY_RENAMED_COLUMNS`: `True` or `False`
- `DATA_RENAME_COLUMNS`: `False` or a dictionary of old (key) and new (value) columns names, e.g. `renaming_map`, which
  is a Python `dict` such as `renaming_map = {'tre200s0': 'TA_3_1_1', 'gre000z0': 'SW_IN_3_1_1'}`
- `DATA_SEPARATOR`: Character that separates data columns in original data file, e.g. `','`
- `DATA_SKIP_ROWS`: Index of rows that are skipped when reading data files, e.g. `[3]`
- `DATA_TIMESTAMP_COL`: Column index of timestamp column, e.g. `0` for first column
- `DATA_TIMESTAMP_FORMAT`: Timestamp format in original data files, e.g. `'%Y-%m-%d %H:%M:%S'`
- `DATA_TIMESTAMP_KEEP_NON_ICOS`: `True` or `False`
- `DIR_OUT_ICOS`: Base folder for output,
  e.g. `Path('//grasslandserver.ethz.ch/processing/CH-DAV_Davos/01_ICOS_TRANSFER/12_meteo_forestfloor')`
- `DIR_OUT_LOGFILE`: Subfolder for logfile, e.g. `Path('log')`
- `DIR_SOURCE_FILES`: Base folder of source files,
  e.g. `Path('//grasslandserver.ethz.ch/archive/FluxData/CH-DAV_Davos/12_meteo_forestfloor')`
- `FILENAME_DAY_POSITION`: Start and end position of day in filename, e.g. `[32, 34]`
- `FILENAME_FOR_ICOS`: Format of ICOS-compliant filename,
  e.g. `'CH-Dav_BM_{year}{month:02d}{day:02d}_L{logger}_F{file}.csv'`
- `FILENAME_HOUR_POSITION`: [35, 37]
- `FILENAME_LENGTH`: 43
- `FILENAME_MINUTE_POSITION`: [37, 39]
- `FILENAME_MONTH_POSITION`: [29, 31]
- `FILENAME_YEAR_POSITION`: [24, 28]
- `OUTFILE_COMPRESSION`: True
- `OUTFILE_DELETE_UNCOMPRESSED`: True
