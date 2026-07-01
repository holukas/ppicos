![](images/logo_ppicos1_256px.png)

# ppicos

`ppicos` (**p**ost-**p**rocessing for ICOS) reads raw data files recorded at the ICOS flux tower
site CH-DAV in Davos, Switzerland, and converts them to the CSV formats required by the ICOS
network. It only reformats the files. **No raw data values are changed.**

> 📖 **Understand the pipeline:** [WORKFLOW.md](WORKFLOW.md) documents the four processing stages,
> architecture, and design decisions. [FLOWCHART.md](FLOWCHART.md) walks through what happens to a
> single file type step by step.

## Contents

- [What gets changed](#what-gets-changed)
- [How it runs](#how-it-runs)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [File settings reference](#file-settings-reference)
- [Architecture](#architecture)
- [Documentation](#documentation)

## What gets changed

ppicos changes the format of the files, not the measured values. The transformations are:

- **Filenames** are renamed, adding the logger and file number. For example,
  `CH-DAV_iDL_T1_35_1_TBL1_2018_08_17_0000.dat` becomes `CH-Dav_BM_20180817_L02_F03.csv`.
- **Column names** are renamed to ICOS variable names, e.g. `tre200s0` becomes `TA_3_1_1`. External
  data providers use variable names that have been established for decades, so the renaming maps
  them to the ICOS convention.
- **Variable-name suffixes** such as `_Avg` are removed.
- **Timestamps** are reformatted, e.g. `%Y-%m-%d %H:%M:%S` becomes `%Y%m%d%H%M%S`.
- **Output files** are compressed to ZIP, e.g. `CH-Dav_BM_20180817_L02_F03.csv` to
  `CH-Dav_BM_20180817_L02_F03.zip`.
- **Multi-day files** are trimmed to a single day. Some providers send more than one day of data
  per file; only the most recent day is kept before transfer.

## How it runs

The `ppicos` command is the interface for all file types. A scheduled task runs it daily. Each run
searches the source folders for recent files, reformats the matching ones, and writes one
ICOS-compliant CSV per day. A separate transfer step then picks up the output and sends it to the
ICOS server.

The older `start_*.py` scripts still work and call the same code, but the CLI is the recommended
way to run ppicos.

## Installation

### Requirements

- Python 3.12 or newer (installed automatically by uv)
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### Quick install

From the project root:

```bash
uv sync          # recommended
# or
pip install .
```

### Install on another machine

These steps set up ppicos from scratch. uv handles both the Python version and the dependencies,
so Python does not need to be installed beforehand.

1. **Install uv.**

   Windows (PowerShell):
   ```powershell
   powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
   ```

   macOS / Linux:
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

   Open a new terminal afterwards so `uv` is on the `PATH`.

2. **Get the code.** Clone the repository (or copy the project folder to the machine):
   ```bash
   git clone https://github.com/holukas/ppicos.git
   cd ppicos
   ```

3. **Create the environment and install everything.** From the project root:
   ```bash
   uv sync
   ```
   This creates a `.venv` folder, installs a matching Python (3.12 or newer, as pinned in
   `pyproject.toml`), and installs ppicos with all its dependencies.

4. **Check that it works:**
   ```bash
   uv run ppicos --list
   uv run ppicos --help
   ```

5. **Configure the data paths** (see [Configuration](#configuration) below).

6. **Preview before running for real.** `--dry-run` reads the settings and previews every step
   without creating or modifying any files:
   ```bash
   uv run ppicos --dry-run
   ```

Prefix commands with `uv run` to use the project environment without activating it manually
(e.g. `uv run ppicos --type 10_meteo`). Alternatively, activate the venv once
(`.venv\Scripts\activate` on Windows, `source .venv/bin/activate` elsewhere) and call `ppicos`
directly.

## Configuration

The source and output roots are site infrastructure paths and are kept out of the code. They live
in `paths.toml`, which is gitignored and never committed. Copy the template and set the two roots
for your machine:

```bash
cp paths.example.toml paths.toml   # Windows: copy paths.example.toml paths.toml
```

Then edit `paths.toml`:

- `rawdata`: root folder holding the raw source files.
- `transfer`: root folder for the ICOS output.

Each file type appends its own subfolder to the appropriate root (e.g. `rawdata_root / '10_meteo'`).
Network paths (for example the ETH NAS) may need VPN access and valid credentials. To keep the
config file elsewhere, point the `PPICOS_PATHS_FILE` environment variable at it instead.

## Usage

After installation, use the `ppicos` command. All examples below also work prefixed with `uv run`.

### Run all file types

Running with no arguments processes every file type in parallel, one worker per file type
(default: 3 workers). Local-test file types are excluded from the batch.

```bash
ppicos                       # run all with 3 parallel workers
ppicos --workers 5           # run all with 5 parallel workers
ppicos --max-age-days 30     # widen the search window (default: 14 days)
```

### Run a single file type

```bash
ppicos --type 10_meteo
ppicos --type 10_meteo_press --max-age-days 14
```

### Forest floor

Forest floor has five instances. Select one with `--instance` (1-5) and, optionally, a table with
`--table` (default: 1). The run-all batch covers all five instances at table 1 automatically.

```bash
ppicos --type 12_meteo_forest_floor --instance 2
ppicos --type 12_meteo_forest_floor --instance 2 --table 1 --max-age-days 14
```

### Dry run

`--dry-run` previews every step without creating or modifying any files.

```bash
ppicos --dry-run                  # preview all file types
ppicos --type 10_meteo --dry-run  # preview a single file type
```

### Discover options

```bash
ppicos --list           # list available file types
ppicos --list-numbers   # list the ICOS logger (LN) and file (FN) numbers in use
ppicos --help           # show all options
```

## File settings reference

`filesettings.py` defines how each file type is processed. One function per file type (for example
`f_17_meteo_profile()`) returns a settings dictionary. To add or change a file type, edit or add a
function there.

### Settings keys

- `DATA_COMPLEMENT_WITH_PREVIOUS_DATE`: `True` or `False`
- `DATA_HEADER_OUTPUT_TO_FILE`: `True` or `False`
- `DATA_HEADER_REMOVE_SUFFIX_FROM_VARIABLE_NAMES`: Suffix to remove from variable names, e.g. `['_Avg']`
- `DATA_HEADER_ROWS`: Row indices holding the variable names, e.g. `[1, 2]`
- `DATA_ICOS_TIMESTAMP_FORMAT`: Timestamp format in output files as required by ICOS, e.g. `'%Y%m%d%H%M'`
- `DATA_KEEP_ONLY_RENAMED_COLUMNS`: `True` or `False`
- `DATA_RENAME_COLUMNS`: `False`, or a dictionary mapping old (key) to new (value) column names, e.g.
  `renaming_map = {'tre200s0': 'TA_3_1_1', 'gre000z0': 'SW_IN_3_1_1'}`
- `DATA_SEPARATOR`: Character that separates data columns in the source file, e.g. `','`
- `DATA_SKIP_ROWS`: Row indices to skip when reading, e.g. `[3]`
- `DATA_TIMESTAMP_COL`: Column index of the timestamp column, e.g. `0` for the first column
- `DATA_TIMESTAMP_FORMAT`: Timestamp format in the source files, e.g. `'%Y-%m-%d %H:%M:%S'`
- `DATA_TIMESTAMP_KEEP_NON_ICOS`: `True` or `False`
- `DIR_OUT_ICOS`: Output folder. Built from the `transfer` root in `paths.toml` plus the file type's
  subfolder, e.g. `transfer_root / '12_meteo_forestfloor'`
- `DIR_OUT_LOGFILE`: Subfolder for the logfile, e.g. `Path('log')`
- `DIR_SOURCE_FILES`: Source folder. Built from the `rawdata` root in `paths.toml` plus the file
  type's subfolder, e.g. `rawdata_root / '12_meteo_forestfloor'`
- `FILENAME_FOR_ICOS`: Output filename template,
  e.g. `'CH-Dav_BM_{year}{month:02d}{day:02d}_L{logger}_F{file}.csv'`
- `FILENAME_LENGTH`: Expected length of the source filename, e.g. `43`
- `FILENAME_POSITION_YEAR` / `_MONTH` / `_DAY` / `_HOUR` / `_MINUTE`: Start and end string positions
  of each date part in the source filename, e.g. `[24, 28]`
- `OUTFILE_COMPRESSION`: `True` to also write a `.zip`
- `OUTFILE_DELETE_UNCOMPRESSED`: `True` to delete the `.csv` after zipping
- `OUTFILE_ICOS_LOGGERNUMBER_LN`: Logger number in the output filename, e.g. `'01'`
- `OUTFILE_ICOS_FILENUMBER_FN`: File number in the output filename, e.g. `'09'`

The logger and file numbers currently assigned across all file types can be listed with
`ppicos --list-numbers`.

### Example

An example settings function, `f_17_meteo_profile()`:

```python
def f_17_meteo_profile():
    # example filename: CH-DAV_meteo-profile_20250401.dat (current)
    rawdata_root, transfer_root = config.roots()

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
        'DIR_OUT_ICOS': transfer_root / '17_meteo_profile',
        'DIR_OUT_LOGFILE': Path('log'),
        'DIR_SOURCE_FILES': rawdata_root / '17_meteo_profile',
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

## Architecture

ppicos is a Python package:

- `main.py` holds the `IcosFormat` class that runs the processing pipeline.
- `filesettings.py` defines the per-file-type settings.
- `config.py` reads the source and output roots from `paths.toml`.
- `cli.py` parses command-line arguments and selects processors.
- `tools.py` and `logger.py` hold helpers for file discovery, logging, and timestamps.

## Documentation

- **[WORKFLOW.md](WORKFLOW.md)**: the full data processing pipeline, covering the four stages (file
  discovery, reading, formatting, export), component architecture, and design decisions.
- **[FLOWCHART.md](FLOWCHART.md)**: a step-by-step flowchart of what happens when ppicos processes
  a single file type, using `10_meteo` as the worked example.
