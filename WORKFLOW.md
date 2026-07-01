# ppicos Data Processing Workflow

## Overview

ppicos (post-processing for ICOS) is a data processing pipeline that reads raw data files recorded at flux tower sites and converts them to ICOS-compliant CSV file formats for submission to the Integrated Carbon Observation System network.

The workflow operates in four sequential stages:
1. **File Discovery** — Find matching raw files within a date window
2. **Data Loading** — Read raw CSV files into memory
3. **Formatting** — Transform data to ICOS standards (rename columns, reformat timestamps, etc.)
4. **Export** — Save daily ICOS-compliant files

No raw data values are modified—only formatting transformations are applied.

---

## High-Level Architecture

```
User invokes ppicos (CLI or script)
            ↓
    Parse command-line arguments
            ↓
    Load file type configuration (filesettings)
            ↓
    Instantiate IcosFormat processor
            ↓
    ┌─────────────────────────────────────┐
    │  IcosFormat.run()                   │
    │  ┌──────────────────────────────┐   │
    │  │ 1. Generate file list        │   │
    │  └──────────────────────────────┘   │
    │           ↓                          │
    │  ┌──────────────────────────────┐   │
    │  │ 2. Read all files            │   │
    │  └──────────────────────────────┘   │
    │           ↓                          │
    │  ┌──────────────────────────────┐   │
    │  │ 3. Format to ICOS standards  │   │
    │  └──────────────────────────────┘   │
    │           ↓                          │
    │  ┌──────────────────────────────┐   │
    │  │ 4. Export daily files        │   │
    │  └──────────────────────────────┘   │
    └─────────────────────────────────────┘
            ↓
    Write logs, generate summary
            ↓
    Exit with status code
```

---

## Component Roles

### 1. CLI Module (`cli.py`)
**Entry point for command-line usage**

- Parses command-line arguments (`--type`, `--instance`, `--list`, `--max-age-days`)
- Lists available file types
- Instantiates and runs processors for single or all file types
- Handles multi-instance file types (e.g., forest floor instances 1-5)
- Returns appropriate exit codes (0 = success, 1 = failure)
- Provides summary of successful/failed runs when running all processors

### 2. Main Module (`main.py`)
**Core processing orchestrator**

The `IcosFormat` class contains the entire processing pipeline:

#### Initialization
- Creates unique run identifier with timestamp
- Sets up logger for this processor
- Reads tracking logfile to identify already-processed files
- Configures ICOS timestamp column handling

#### Run Method (orchestrates the pipeline)
1. Calls `_generate_file_list()` → returns DataFrame of matching files (validates each file with `_validate_file()`)
2. Calls `_readfiles()` → reads raw data into merged DataFrame
3. Calls `_format_data()` → transforms to ICOS format
4. Calls `_export_data()` → saves daily output files (exports with `_save_daily_file()`)
5. Logs total runtime

#### Key Consolidations
- **File validation**: Single `_validate_file(filepath, start_date)` method replaces 3 separate validation checks
- **File export**: Single `_save_daily_file(df, csv_path, zip_path)` method handles CSV save, compression, and cleanup
- **Logging**: All methods now use `self.logger.section()` context manager; no section_name parameter passing

### 3. File Settings Module (`filesettings.py`)
**Configuration for each file type**

Contains function definitions for each file type (e.g., `f_10_meteo()`, `f_12_meteo_forest_floor()`). Each function returns a configuration dictionary with ~25 keys controlling:

- **Data parsing**: timestamp format, column separator, header rows, skip rows
- **Transformations**: column name mapping, suffix removal, frequency reindexing
- **I/O paths**: source directory, output directory, filename patterns
- **Output control**: compression, filename template, logger/file numbers
- **Special handling**: multi-instance support, complementary date data

### 4. Tools Module (`tools.py`)
**Utility functions**

- `set_search_window()` → defines date range: now - max_age_days to yesterday
- `set_search_folders()` → generates monthly subfolder paths for search window
- `get_datetime_from_filename()` → extracts date from filename using position indices
- `get_subdir_from_date()` → creates output directory structure and ensures paths exist
- `make_run_id()` → generates unique run identifier with human-readable timestamp

### 5. Logger Module (`logger.py`)
**Structured logging with timing**

- `Logger` class → writes to both console and file simultaneously
- `section()` context manager → wraps processing sections with automatic timing measurement
- Guarantees cleanup even on errors via try/finally pattern

### 6. HTML Generator Module (`html_generator.py`)
**Optional HTML reporting**

Generates index.html file showing processing history and file statistics.

---

## Detailed Processing Pipeline

### Stage 1: File Discovery (`_generate_file_list`)

```
Input: file type configuration
Output: DataFrame with columns [ETH_FILEPATH, ETH_FILEDATE, ETH_FILENAME]

Steps:
  1. Calculate search date range
     - start_date = today - max_age_days
     - end_date = yesterday
     
  2. Generate monthly subfolder list
     - e.g., source_dir/2025/01, source_dir/2025/02, ...
     
  3. Search for matching files
     - Use fnmatch against FILENAME_ID pattern
     - Extract date from filename using position indices
     - Filter by search date range
     
  4. Create tracking entry
     - Check if file was already processed
     - Skip if in tracking logfile, otherwise add to queue
     
  5. Return DataFrame of files to process
```

**Key concept**: Filename dating via position indices allows flexible extraction even if timestamp isn't in standard format.

### Stage 2: Data Loading (`_readfiles`)

```
Input: list of file paths
Output: merged DataFrame with all data concatenated

Steps:
  1. For each input file:
     - Read CSV with configurable:
       • Separator (comma, semicolon, etc.)
       • Header row(s) — some files have multi-row headers
       • Skip rows — e.g., units row
       • Timestamp format
     
  2. Convert timestamp column
     - Parse using DATA_TIMESTAMP_FORMAT
     - Set as DataFrame index
     - Errors='coerce' → invalid dates become NaT
     
  3. Concatenate all files
     - Stack DataFrames vertically
     - Sort by timestamp
     
  4. Return merged result
```

**Key detail**: Multi-row headers are handled by combining header rows and using tuples as column names.

### Stage 3: Formatting (`_format_data`)

```
Input: merged DataFrame from _readfiles
Output: formatted DataFrame ready for export

Processing steps:

  A. Remove duplicate timestamps
     - Keep last occurrence (most recent value)
     
  B. Rename columns
     - Map old names to ICOS standard names via DATA_RENAME_COLUMNS dict
     - Remove configured suffixes (_Avg, _Tot, etc.)
     
  C. Reindex to continuous timestamp series
     - Create index at DATA_FREQUENCY (e.g., every 10 seconds)
     - Fill gaps with NaN
     - This ensures consistent timestamps across days
     
  D. Handle partial days
     - Optional: complement with previous date's data (for files with offset start times)
     - Remove today's date (incomplete data)
     - Remove days with insufficient timestamp coverage
     
  E. Create TIMESTAMP_MIDDLE
     - For daily file generation, use middle of averaging interval
     - Example: 10-second data → 5-second offset within 10-second interval
     - Used for grouping data by date
     
  F. Filter to relevant columns
     - If DATA_KEEP_ONLY_RENAMED_COLUMNS = True, keep only renamed columns
     - Otherwise keep all columns
```

**Key transformations**: The reindexing step ensures every day has timestamps at the exact expected frequency, even if raw files have gaps.

### Stage 4: Export (`_export_data`)

```
Input: formatted DataFrame
Output: daily ICOS CSV files (optionally zipped)

Processing steps:

  1. Group by date
     - Use TIMESTAMP_MIDDLE for grouping
     - Ensures date boundaries align with averaging intervals
     
  2. For each daily group:
     
     a. Check if already processed
        - Look up filename in tracking logfile
        - Skip if found, log message
        - Otherwise proceed
        
     b. Create ICOS filename
        - Format: CH-Dav_BM_YYYYMMDD_L{logger}_F{file}.csv
        - Use settings for logger number and file number
        
     c. Create output directory
        - DIR_OUT_ICOS/YYYY/MM/
        - Create if doesn't exist
        
     d. Write CSV file
        - Include header row with column names
        - TIMESTAMP column in ICOS format (YYYYMMDDHHmmss)
        - NaN values written as "NaN"
        - Line endings: CRLF (\r\n)
        
     e. Optionally compress
        - If OUTFILE_COMPRESSION = True:
          • Create ZIP archive
          • Add CSV to archive
          • Optionally delete uncompressed CSV
        
     f. Update tracking logfile
        - Append filename to ppicos_[filetype]_files-already-processed.log
        - Prevents re-processing on future runs
```

**Deduplication strategy**: File is identified as already-processed by checking its filename in the tracking logfile. Combined with date windowing (search backward max_age_days), this prevents re-processing.

---

## File Settings Configuration Dictionary

Each file type function returns a dict with these keys:

**Data Format & Parsing**
```python
'DATA_TIMESTAMP_COL': 0                          # Column index with timestamp
'DATA_TIMESTAMP_FORMAT': '%Y-%m-%d %H:%M:%S'    # Input format string
'DATA_ICOS_TIMESTAMP_FORMAT': '%Y%m%d%H%M%S'    # Output ICOS format
'DATA_SEPARATOR': ','                            # CSV delimiter
'DATA_HEADER_ROWS': [1, 2]                       # Row indices with column names
'DATA_SKIP_ROWS': [3]                            # Row indices to skip (e.g., units)
'DATA_FREQUENCY': '10S'                          # Sampling interval (10S, 1T, 10T, etc.)
```

**Column Transformation**
```python
'DATA_RENAME_COLUMNS': {                         # Old name → ICOS name mapping
    'tre200s0': 'TA_3_1_1',
    'rre150z0': 'P_3_1_1',
    ...
}
'DATA_HEADER_REMOVE_SUFFIX_FROM_VARIABLE_NAMES': ['_Avg', '_Tot']
'DATA_KEEP_ONLY_RENAMED_COLUMNS': True          # Keep only renamed, drop others?
```

**I/O Configuration**
```python
'FILENAME_ID': 'CH-DAV_iDL_T1_35_1_TBL1_*.dat'  # Glob pattern for matching
'FILENAME_POSITION_YEAR': [0, 4]                # String indices for date extraction
'FILENAME_POSITION_MONTH': [4, 6]
'FILENAME_POSITION_DAY': [6, 8]
'FILENAME_POSITION_HOUR': [8, 10]               # None if not in filename
'FILENAME_POSITION_MINUTE': [10, 12]
'DIR_SOURCE_FILES': Path('/mnt/server/source')
'DIR_OUT_ICOS': Path('/local/output/icos')
'DIR_OUT_LOGFILE': 'log'                        # Subdirectory for logs
```

**Output File Control**
```python
'FILENAME_FOR_ICOS': 'CH-Dav_BM_{year}{month:02d}{day:02d}_L{logger}_F{file}.csv'
'OUTFILE_ICOS_LOGGERNUMBER_LN': '01'           # Logger ID in output
'OUTFILE_ICOS_FILENUMBER_FN': '03'             # File number in output
'OUTFILE_COMPRESSION': True                     # Zip the file?
'OUTFILE_DELETE_UNCOMPRESSED': True             # Delete CSV after zipping?
```

**Processing Options**
```python
'DATA_COMPLEMENT_WITH_PREVIOUS_DATE': False     # Include previous day's data?
'DATA_HEADER_OUTPUT_TO_FILE': True              # Write column names?
'DATA_TIMESTAMP_KEEP_NON_ICOS': False           # Keep original timestamp?
'FILE_FILEGROUP': '10_meteo'                    # Identifier for logs/tracking
```

---

## Logging & Error Handling

### Logging Architecture

Each processor run generates:

1. **Console output** — Real-time progress to stdout
2. **Processing logfile** — Detailed operation record
   - Location: `DIR_OUT_ICOS/log/YYYY/MM/ppicos-[timestamp]_[filetype].log`
   - Contains: section timings, file counts, data transformations
3. **Tracking logfile** — Processed file registry
   - Location: `DIR_OUT_ICOS/ppicos_[filetype]_files-already-processed.log`
   - Contents: one filename per line
   - Used to skip re-processing on future runs

### Section Timing

Processing is organized into sections with automatic timing:

```python
with self.logger.section('[section name]'):
    # Do work
    # Timing and logging automatically handled
```

The context manager:
- Records section start time
- Logs section start message
- Tracks current section internally (`self.logger.current_section`)
- Measures elapsed time on exit
- Logs section end with duration
- Guarantees cleanup via try/finally
- Restores previous section state on exit

### Error Handling

- **File not found** → Logged, skipped
- **Permission denied** → Logged, file skipped
- **Invalid timestamp** → Row removed via errors='coerce'
- **Missing columns** → Logged warning, processing continues if possible
- **Write failure** → Exception raised, caught by CLI, exit code 1

---

## Multi-Instance File Types

Some file types require processing multiple "instances" (e.g., forest floor has 5 separate sensor sets).

**Forest Floor (12_meteo_forest_floor)**
- 5 instances (FF1-FF5) × 2 tables (TBL1, TBL2) = 10 output files per run
- CLI usage: `ppicos --type 12_meteo_forest_floor --instance 2`
- Without `--instance`, runs all 5 instances sequentially
- Each instance has separate source files and output directory structure

---

## Common Workflows

### Run Single Processor
```bash
ppicos --type 10_meteo
```
Processes all 10_meteo files from the last 14 days (default).

### Run Specific Forest Floor Instance
```bash
ppicos --type 12_meteo_forest_floor --instance 3
```
Processes forest floor 3 with default 14-day window.

### Run All Processors
```bash
ppicos
```
Sequentially processes all 11 file types, prints summary at end.

### Custom Date Window
```bash
ppicos --type 10_meteo --max-age-days 30
```
Looks back 30 days instead of default 14.

### List Available Types
```bash
ppicos --list
```

---

## Key Design Decisions

### 1. Date Window + Filename Tracking
- Searches back N days to capture recently-modified files
- Tracks processed filenames to prevent re-processing
- Combination provides overlap (multi-day consolidation) + deduplication

### 2. Continuous Reindexing
- After reading raw files, reindex to continuous timestamp series
- Ensures every day has exactly the expected number of timestamps
- Gaps become NaN, removed days are handled during export

### 3. Timestamp Grouping via TIMESTAMP_MIDDLE
- When exporting daily files, group by TIMESTAMP_MIDDLE
- This offset aligns with the averaging interval semantics
- Prevents day-boundary edge cases

### 4. File Type Configuration as Dict
- Centralizes all settings in one place per file type
- Easy to update when raw file format changes
- Can be extended without modifying core processing logic

### 5. Context Manager for Logging
- `with logger.section()` eliminates boilerplate
- Timing is automatic and guaranteed
- No risk of forgetting to log section end

---

## Output Directory Structure

```
DIR_OUT_ICOS/
├── 2025/01/
│   ├── CH-Dav_BM_20250112_L01_F03.csv
│   ├── CH-Dav_BM_20250112_L01_F03.zip
│   ├── CH-Dav_BM_20250113_L01_F03.csv
│   └── ...
├── 2025/02/
│   └── [daily files for February]
├── log/2025/01/
│   ├── ppicos-20250115133022_10_meteo.log
│   └── ...
├── ppicos_10_meteo_files-already-processed.log
├── ppicos_12_meteo_forest_floor_files-already-processed.log
└── [other tracking files for other file types]
```

---

## Troubleshooting Guide

| Issue | Likely Cause | Solution |
|-------|--------------|----------|
| No files found | Source directory wrong or unreachable | Check `DIR_SOURCE_FILES` path in filesettings |
| Files found but not processed | Already in tracking logfile | Edit ppicos_[filetype]_files-already-processed.log to remove filename |
| Timestamp errors | Format mismatch | Verify `DATA_TIMESTAMP_FORMAT` matches actual file format |
| Missing columns in output | Column rename mapping incomplete | Check `DATA_RENAME_COLUMNS` dict in filesettings |
| Permission denied | Network path auth required | Verify VPN/authentication, check read/write permissions |
| Incomplete daily file | Data gap at day boundary | Check `DATA_COMPLEMENT_WITH_PREVIOUS_DATE` setting for files with offset starts |
