# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**ppicos** (post-processing for ICOS) is a data processing pipeline that reads raw data files recorded at the ICOS flux tower site CH-DAV (Davos, Switzerland) and converts them to ICOS-compliant CSV file formats for submission to the Integrated Carbon Observation System network.

No raw data values are modified—only formatting transformations are applied:
- Column renaming (e.g., `tre200s0` → `TA_3_1_1`)
- Timestamp format conversion (e.g., `%Y-%m-%d %H:%M:%S` → `%Y%m%d%H%M%S`)
- Variable suffix removal (e.g., `_Avg`, `_Tot`)
- File compression to ZIP (optional)
- Extraction of daily data from multi-day raw files

**See [WORKFLOW.md](WORKFLOW.md) for detailed technical documentation of the data processing pipeline, architecture, and design decisions.**

## Git Workflow

**Never commit, stage, or add files to git.** Write commit messages only; the user handles all git operations.

**When writing prose (commit messages, changelog entries, documentation):** Always use the `/llm-detox` skill to ensure natural, human-sounding text before presenting it.

## Installation & Usage

### Prerequisites
- Python 3.12 or higher
- uv (recommended) or pip

### Installation

Install via uv:
```bash
uv sync
```

Or via pip:
```bash
pip install .
```

### Running ppicos

After installation, use the `ppicos` command:

```bash
# Show help and available options
ppicos --help

# List all available file types
ppicos --list

# Run all file types
ppicos

# Run specific file type
ppicos --type 10_meteo

# Run forest floor with specific instance
ppicos --type 12_meteo_forest_floor --instance 2

# Run with custom max age (days)
ppicos --max-age-days 30
```

The CLI automatically:
1. Searches source directories for files within the date window
2. Reads matching raw files
3. Merges and formats to ICOS standards
4. Exports daily CSV files
5. Tracks processed files to avoid re-processing
6. Logs all operations

## Architecture

### Core Components

**`main.py` - IcosFormat Class**
- Central orchestration class that handles the full processing pipeline
- Constructor takes a `filesettings` dict and `max_age_days` parameter (default 5)
- Main entry point: `run()` method executes the complete pipeline

**Processing Pipeline (in `IcosFormat.run()`)**
1. `_generate_file_list()` — Search source directories for matching files within the date window
2. `_readfiles()` — Read raw CSV files using pandas with configurable separators/headers
3. `_format_data()` — Apply transformations (rename cols, remove duplicates, reindex timestamps, insert ICOS timestamp format)
4. `_export_data()` — Group by date and save daily ICOS-compliant CSV files (optionally zipped)

**Key Data Transformations (in `_format_data()`)**
- Column renaming via `DATA_RENAME_COLUMNS` map
- Duplicate timestamp removal (keeps last)
- Timestamp reindexing to continuous series based on `DATA_FREQUENCY`
- Partial day removal (discards days with incomplete timestamp coverage)
- Data from today's date removed (only yesterday's data included in output)
- Timestamp index converted to TIMESTAMP_MIDDLE (halfway through averaging interval) for daily file generation

**`filesettings.py` - File Type Configuration**
- Contains function definitions like `f_10_meteo()`, `f_12_meteo_forest_floor()`, etc.—one function per file type
- Each returns a dictionary with ~25 settings keys
- Settings control: source/output directories, filename patterns, timestamp formats, column mappings, compression behavior

**`start_*.py` - Processor Scripts (Legacy)**
- Scripts like `start_10_meteo.py`, `start_12_meteo_forest_floor.py`, etc.
- Each imports a file type function from `filesettings.py` and instantiates `IcosFormat` with those settings
- Can still be called individually for backward compatibility
- Example: `start_12_meteo_forest_floor.py` loops through 5 forest floors × 2 tables (10 file type combinations)
- **Note**: Use the `ppicos` CLI command instead (see Installation & Usage above)

**`cli.py` - Command-line Interface (Recommended)**
- Main entry point for ppicos package
- Handles argument parsing (--type, --instance, --list, etc.)
- Called via `ppicos` command after installation
- Uses consistent logging and error handling across all processors

**`tools.py` - Utility Functions**
- `set_search_window()` — Defines date range: now - max_age_days to yesterday
- `set_search_folders()` — Generates monthly subfolder paths (source_dir/YYYY/MM) for the search window
- `get_datetime_from_filename()` — Extracts date from raw filename using position indices
- `get_subdir_from_date()` — Creates output subdirectory structure (YYYY/MM) and ensures it exists
- `make_run_id()` — Generates unique run identifiers with timestamps

**`logger.py` - Logging**
- `Logger` class writes to both console and file (logs/ppicos_[filetype]_[timestamp].log)
- `section()` context manager — Wraps processing phases with automatic timing and guaranteed cleanup via try/finally
- Tracks `current_section` internally; no section_name parameter passing needed

## File Settings Dictionary

Each file type function in `filesettings.py` returns a dict with these key settings:

**Data Format & Parsing**
- `DATA_TIMESTAMP_COL` — Column index of timestamp in raw file (usually 0)
- `DATA_TIMESTAMP_FORMAT` — Input timestamp format string (e.g., `'%Y-%m-%d %H:%M:%S'`)
- `DATA_ICOS_TIMESTAMP_FORMAT` — Output ICOS format (e.g., `'%Y%m%d%H%M%S'`)
- `DATA_SEPARATOR` — CSV delimiter (`,`, `;`)
- `DATA_HEADER_ROWS` — Row indices containing column names (e.g., `[1, 2]` for 2-row headers)
- `DATA_SKIP_ROWS` — Row indices to skip when reading (e.g., `[3]` for units row)
- `DATA_FREQUENCY` — Expected sampling interval (e.g., `'10S'`, `'1T'`, `'10T'`)

**Column Transformation**
- `DATA_RENAME_COLUMNS` — Dict mapping old column names to ICOS names, or `False` if no renaming
- `DATA_HEADER_REMOVE_SUFFIX_FROM_VARIABLE_NAMES` — List of suffixes to strip (e.g., `['_Avg']`)
- `DATA_KEEP_ONLY_RENAMED_COLUMNS` — `True` to export only renamed columns, `False` for all

**Filename & Directory**
- `FILENAME_ID` — Glob pattern to match source filenames (e.g., `'CH-DAV_iDL_T1_35_1_TBL1_*.dat'`)
- `FILENAME_POSITION_YEAR`, `_MONTH`, `_DAY`, `_HOUR`, `_MINUTE` — String indices to extract date from filename
- `DIR_SOURCE_FILES` — Path object pointing to source directory (e.g., network share)
- `DIR_OUT_ICOS` — Path object for ICOS output (organized as YYYY/MM subdirs)
- `DIR_OUT_LOGFILE` — Subdirectory for log files relative to `DIR_OUT_ICOS`

**Output File Control**
- `FILENAME_FOR_ICOS` — Template for output filenames (e.g., `'CH-Dav_BM_{year}{month:02d}{day:02d}_L{logger}_F{file}.csv'`)
- `OUTFILE_ICOS_LOGGERNUMBER_LN` — Logger ID in output filename (2-char string)
- `OUTFILE_ICOS_FILENUMBER_FN` — File number in output filename (2-char string)
- `OUTFILE_COMPRESSION` — `True` to create .zip, `False` for .csv only
- `OUTFILE_DELETE_UNCOMPRESSED` — `True` to delete .csv after zipping

**Processing Options**
- `DATA_COMPLEMENT_WITH_PREVIOUS_DATE` — `True` to include previous day's data (needed for some file types with offset starts)
- `DATA_HEADER_OUTPUT_TO_FILE` — `True` to write column names to output CSV
- `DATA_TIMESTAMP_KEEP_NON_ICOS` — `True` to retain original timestamp in output (added as `_TIMESTAMP_OLD` column)
- `FILE_FILEGROUP` — Identifier for this file type (used in log filenames and tracking)

## Running a Processor

To trigger processing for a specific file type:

```powershell
# From ppicos root directory
python ppicos/start_10_meteo.py
python ppicos/start_12_meteo_forest_floor.py
python ppicos/start_ALL.py  # Run all file types sequentially
```

The script will:
1. Search source directory for files modified in the last `MAX_AGE_DAYS` (defined in start script)
2. Read matching raw files
3. Merge and format to ICOS standards
4. Export daily CSV files to `DIR_OUT_ICOS/YYYY/MM/`
5. Create/update processing log: `ppicos_[filetype]_files-already-processed.log` (tracks which files have been processed to avoid re-processing)
6. Write detailed operation log: `ppicos-[timestamp]_[filetype].log`

## Key Concepts

**Search Window**
- Defined by `MAX_AGE_DAYS` parameter passed to `IcosFormat()`
- Searches for files dated from (today - MAX_AGE_DAYS) through yesterday
- Today's data is explicitly removed during formatting (partial day)
- Example: MAX_AGE_DAYS=14 searches 14 days back, ensuring overlap for multi-day consolidations

**File Matching**
- Raw filenames matched against `FILENAME_ID` pattern (using fnmatch)
- Date extracted from filename position indices, then compared against search window
- File must be readable (permission check performed)

**Timestamp Reindexing**
- After reading, timestamps are reindexed to a continuous series with gaps filled (NaN)
- Frequency defined by `DATA_FREQUENCY` (e.g., 10-second data becomes every 10S)
- Days with incomplete coverage (missing timestamps) are removed

**Daily File Generation**
- Data grouped by date (using TIMESTAMP_MIDDLE for grouping)
- Each day exported as separate file: `CH-Dav_BM_YYYYMMDD_L{logger}_F{file}.csv`
- Output directory structure: `DIR_OUT_ICOS/YYYY/MM/filename.csv` (created automatically)

## Adding a New File Type

1. **Create settings function** in `filesettings.py`:
   ```python
   def f_XX_description():
       renaming_map = {...}  # if needed
       file_info = {
           'DATA_TIMESTAMP_COL': 0,
           'DATA_TIMESTAMP_FORMAT': '%Y-%m-%d %H:%M:%S',
           'DATA_FREQUENCY': '10S',
           ...
       }
       return file_info
   ```

2. **Create start script** `start_XX_description.py` (optional, for backward compatibility):
   ```python
   import sys
   from ppicos import filesettings
   from ppicos.main import IcosFormat
   
   MAX_AGE_DAYS = 14
   icosformat = IcosFormat(filesettings=filesettings.f_XX_description(), max_age_days=MAX_AGE_DAYS)
   icosformat.run()
   sys.exit()
   ```
   
   **Recommended**: Use the CLI instead (`ppicos --type XX_description`)

3. **Test locally** with temporary directories before pointing to production network paths in `filesettings.py`

## Modifying File Settings

When updating a file type's settings (e.g., because raw file format changed):
- Edit the corresponding function in `filesettings.py`
- Update datetime position indices if filename pattern changed
- Update column renaming map if header changed
- Test with a small batch of files before enabling scheduled runs
- Document changes in CHANGELOG.md with version bump and description

## Dependencies

**Python version**: 3.12 or higher

**Required packages**:
- pandas ≥1.5.3 (data manipulation)
- Jinja2 ≥3.1.2 (HTML template generation)
- numpy (numeric operations, pulled in by pandas)

**Built-in modules**: csv, datetime, fnmatch, os, sys, zipfile, pathlib, argparse

**Package manager**:
- Recommended: [uv](https://docs.astral.sh/uv/) (faster, simpler)
  ```bash
  uv sync
  ```
- Alternative: pip
  ```bash
  pip install -e .
  ```

## Output Structure

```
DIR_OUT_ICOS/
├── YYYY/MM/
│   ├── CH-Dav_BM_YYYYMMDD_L01_F03.csv
│   ├── CH-Dav_BM_YYYYMMDD_L01_F03.zip
│   └── ...
├── log/YYYY/MM/
│   └── ppicos-YYYYMMDDHHMMSS_10_meteo.log
└── ppicos_10_meteo_files-already-processed.log
```

## Troubleshooting

**No files found**
- Check `DIR_SOURCE_FILES` path exists and is readable
- Verify `MAX_AGE_DAYS` is large enough to capture recent files
- Check `FILENAME_ID` pattern matches actual filenames

**Timestamp errors**
- Verify `DATA_TIMESTAMP_FORMAT` matches raw file timestamps
- Check `DATA_TIMESTAMP_COL` points to correct column
- Ensure `DATA_FREQUENCY` matches the actual data sampling interval

**Permission errors**
- Network paths may require VPN/authentication
- Check read/write permissions on source and output directories
- Windows network paths should use forward slashes (e.g., `//server/share`)

**Re-processing files**
- Edit `ppicos_[filetype]_files-already-processed.log` to remove filenames, then re-run processor

## Recent Improvements (v6.0.0)

### PHASE 1: Critical Compatibility ✅
- Removed deprecated `date_parser` parameter (pandas 2.0+ compatible)
- Fixed ZipFile resource management (proper context manager usage)

### PHASE 2: Package Structure ✅
- Created proper Python package with `__init__.py`
- Converted all relative imports to absolute imports (`from ppicos import module`)
- Renamed `html.py` to `html_generator.py` (avoid shadowing built-in module)
- Ready for `pip install` and `uv sync`

### PHASE 3: CLI Enhancement ✅
- Created `cli.py` with full argument parsing
- Added `ppicos` command-line interface
- Support for selective processor execution (`--type`, `--instance`)
- Discoverability (`--help`, `--list`)
- Updated entry point in `pyproject.toml`

### PHASE 4: Code Quality ✅
- **Consolidated file validation**: Merged 3 validation methods into single `_validate_file()` with early returns
- **Consolidated file export**: Merged 3 export methods into single `_save_daily_file()` handling CSV, compression, and cleanup
- **Eliminated logging parameter noise**: Removed `section_name` parameter from 15+ methods by moving section context tracking to Logger instance variable
- **Logger refactoring**: Converted section tracking to `Logger.current_section` attribute, changed `section()` from standalone function to context manager method
- **Code cleanup**: Removed dead code (`_detect_unique_dates`), fixed list comprehensions, removed `inplace=True` in pandas, removed commented-out code blocks
- **Result**: 56 lines saved in main.py (616 → 560), cleaner method signatures, improved readability
- **PEP 8 compliant**
