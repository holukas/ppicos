![](images/logo_ppicos1_256px.png)

# ppicos

`ppicos` (**p**ost-**p**rocessing for ICOS) reads raw data files recorded at the ICOS site CH-DAV and converts
their formats to ICOS-conform file formats.

**No raw data values are changed during this process.**

## Installation & Usage

### Install

```bash
pip install .
# or with uv (recommended)
uv sync
```

### Quick Start

After installation, use the `ppicos` command:

```bash
# Show available file types
ppicos --list

# Run all file types
ppicos

# Run specific file type (e.g., 10_meteo)
ppicos --type 10_meteo

# Run forest floor with specific instance (1-5)
ppicos --type 12_meteo_forest_floor --instance 2

# Override search window (default: 14 days)
ppicos --max-age-days 30

# Show all options
ppicos --help
```

### CLI Examples

Run all file types:

```bash
ppicos --max-age-days 14
```

Run specific processors (all with search window of 14 days):

```bash
ppicos --type 10_meteo --max-age-days 14
ppicos --type 10_meteo_press --max-age-days 14
ppicos --type 10_meteo_heatflag_sonic --max-age-days 14
ppicos --type 11_meteo_hut_prec --max-age-days 14
ppicos --type 13_meteo_backup_eth --max-age-days 14
ppicos --type 13_meteo_meteoswiss --max-age-days 14
ppicos --type 13_meteo_nabel --max-age-days 14
ppicos --type 15_meteo_snowheight --max-age-days 14
ppicos --type 17_meteo_profile --max-age-days 14
ppicos --type 30_profile_ghg --max-age-days 14
```

Forest floor (all 5 instances with table=1):

```bash
ppicos --type 12_meteo_forest_floor --instance 1 --table 1 --max-age-days 14
ppicos --type 12_meteo_forest_floor --instance 2 --table 1 --max-age-days 14
ppicos --type 12_meteo_forest_floor --instance 3 --table 1 --max-age-days 14
ppicos --type 12_meteo_forest_floor --instance 4 --table 1 --max-age-days 14
ppicos --type 12_meteo_forest_floor --instance 5 --table 1 --max-age-days 14
```

Discovery:

```bash
ppicos --list
ppicos --help
```

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

If `ppicos` is executed, the file `filesettings.py` has to reside in the same folder as the `start_*.py` scripts.

### General settings

- `DATA_COMPLEMENT_WITH_PREVIOUS_DATE`: `True` or `False`
- `DATA_HEADER_OUTPUT_TO_FILE`: `True` or `False`
- `DATA_HEADER_REMOVE_SUFFIX_FROM_VARIABLE_NAMES`: Suffix to remove from variable names, e.g. `['_Avg']`
- `DATA_HEADER_ROWS`: Defines where the variable names , e.g. `[1, 2]`
- `DATA_ICOS_TIMESTAMP_FORMAT`: Timestamp format in output files as required by ICOS, e.g. `'%Y%m%d%H%M'`
- `DATA_KEEP_ONLY_RENAMED_COLUMNS`: `True` or `False`
- `DATA_RENAME_COLUMNS`: `False` or a dictionary of old (key) and new (value) columns names, e.g. `renaming_map`, which
  is a Python `dict` such as `renaming_map = {'tre200s0': 'TA_3_1_1', 'gre000z0': 'SW_IN_3_1_1'}`
- `DATA_SEPARATOR`: Character that separates data columns in original data file, e.g. `','`
- `DATA_SKIP_ROWS`: Index of rows that are skipped when reading data files, e.g. `[3]`
- `DATA_TIMESTAMP_COL`: Column index of timestamp column, e.g. `0` for first column
- `DATA_TIMESTAMP_FORMAT`: Timestamp format in original data files, e.g. `'%Y-%m-%d %H:%M:%S'`
- `DATA_TIMESTAMP_KEEP_NON_ICOS`: `True` or `False`
- `DIR_OUT_ICOS`: Base folder for output,
  e.g. `Path('//server/share/processing/CH-DAV_Davos/01_ICOS_TRANSFER/12_meteo_forestfloor')`
- `DIR_OUT_LOGFILE`: Subfolder for logfile, e.g. `Path('log')`
- `DIR_SOURCE_FILES`: Base folder of source files,
  e.g. `Path('//server/share/archive/FluxData/CH-DAV_Davos/12_meteo_forestfloor')`
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

### Example settings

- Here is an example entry for the file setting `f_17_meteo_profile` in `filesettings.py`.
- These settings are used if the start script `start_17_meteo_profile.py` is executed.

```
def f_17_meteo_profile():
    # example filename: CH-DAV_meteo-profile_20250401.dat (current)
    
    renaming_map = {
        'TA_T1_1_1_Avg': 'TA_1_1_1',
        'TA_T1_2_1_Avg': 'TA_1_2_1',
        'TA_T1_10_1_Avg': 'TA_1_3_1',
        'TA_T1_20_1_Avg': 'TA_1_4_1',
        'TA_T1_25_1_Avg': 'TA_1_5_1',
        'TA_T1_35_1_Avg': 'TA_1_6_1',
        'RH_T1_1_1_Avg': 'RH_1_1_1',
        'RH_T1_2_1_Avg': 'RH_1_2_1',
        'RH_T1_10_1_Avg': 'RH_1_3_1',
        'RH_T1_20_1_Avg': 'RH_1_4_1',
        'RH_T1_25_1_Avg': 'RH_1_5_1',
        'RH_T1_35_1_Avg': 'RH_1_6_1',
    }

    file_info = {
        'DATA_COMPLEMENT_WITH_PREVIOUS_DATE': False,
        'DATA_FREQUENCY': '10S',
        'DATA_HEADER_OUTPUT_TO_FILE': True,
        'DATA_HEADER_REMOVE_SUFFIX_FROM_VARIABLE_NAMES': [],
        'DATA_HEADER_ROWS': [1],
        'DATA_ICOS_TIMESTAMP_FORMAT': '%Y%m%d%H%M%S',
        'DATA_KEEP_ONLY_RENAMED_COLUMNS': True,
        'DATA_RENAME_COLUMNS': renaming_map,
        'DATA_SEPARATOR': ',',
        'DATA_SKIP_ROWS': [2, 3],
        'DATA_TIMESTAMP_COL': 0,
        'DATA_TIMESTAMP_FORMAT': '%Y-%m-%d %H:%M:%S',  # 2025-04-06 00:00:10
        'DATA_TIMESTAMP_KEEP_NON_ICOS': True,
        'DIR_OUT_ICOS': Path('//server/share/processing/CH-DAV_Davos/01_ICOS_TRANSFER/17_meteo_profile'),
        'DIR_OUT_LOGFILE': Path('log'),
        'DIR_SOURCE_FILES': Path('//server/share/rawdata/FluxData/CH-DAV_Davos/17_meteo_profile'),
        'FILE_FILEGROUP': '17_meteo_profile',
        'FILENAME_FOR_ICOS': 'CH-Dav_BM_{year}{month:02d}{day:02d}_L{logger}_F{file}.csv',
        'FILENAME_ID': 'CH-DAV_meteo-profile_*.dat',
        'FILENAME_LENGTH': 33,
        'FILENAME_POSITION_HOUR': [],
        'FILENAME_POSITION_MINUTE': [],
        'FILENAME_POSITION_DAY': [27, 29],
        'FILENAME_POSITION_MONTH': [25, 27],
        'FILENAME_POSITION_YEAR': [21, 25],
        'OUTFILE_COMPRESSION': True,
        'OUTFILE_DELETE_UNCOMPRESSED': True,
        'OUTFILE_ICOS_FILENUMBER_FN': '09',
        'OUTFILE_ICOS_LOGGERNUMBER_LN': '01'
    }

    return file_info
```