"""Post-processing of raw data files to ICOS-compliant format."""
import csv
import datetime
import fnmatch
import os
import zipfile as zf
from pathlib import Path

import numpy as np
import pandas as pd
from pandas import DataFrame

from ppicos import html_generator as html
from ppicos import logger
from ppicos import tools
from ppicos.logger import Logger


class NoFilesFoundError(Exception):
    """Raised when no input files are available for a filetype in the search window."""


class IcosFormat:
    """
    Format original raw data files to ICOS-compliant format
    """

    def __init__(self,
                 filesettings: dict,
                 max_age_days: int = 5,
                 dry_run: bool = False,
                 echo_console: bool = True):
        self.filesettings = filesettings
        self.max_age_days = max_age_days
        # dry_run: preview only, never create or modify any file
        self.dry_run = dry_run
        # echo_console: print operation output to the console (off for quiet
        # parallel workers, which still write their per-filetype log files)
        self.echo_console = echo_console

        # Make identifier for this run
        self.run_id, self.run_start_datestr, self.run_start_dt = tools.make_run_id()

        # Init logger
        self.logger = self._setup_logger()

        # Location of filetype logfile, stores names of already-processed files
        logfilename_alreadyprocessed = f"ppicos_{self.filesettings['FILE_FILEGROUP']}_files-already-processed.log"
        self.logfilepath_alreadyprocessed = self.filesettings['DIR_OUT_ICOS'] / logfilename_alreadyprocessed
        self.filetype_logfile_contents = self._read_filetype_logfile()

        # ICOS timestamp column
        if len(self.filesettings['DATA_HEADER_ROWS']) > 1:
            self.icos_timestamp_col = ('TIMESTAMP', 'TS')
        else:
            self.icos_timestamp_col = 'TIMESTAMP'

    def run(self):
        """Run ppicos processing chain"""

        # Search files in source and make a dataframe
        input_files_df = self._generate_file_list()

        # Read all data from input files to dataframe
        merged_df = self._readfiles(filepaths=input_files_df['ETH_FILEPATH'].to_list())

        # Format data to ICOS formats
        merged_df = self._format_data(df=merged_df)

        # Export data as daily files
        self._export_data(df=merged_df)

        # Script runtime
        script_runtime = datetime.datetime.now() - self.run_start_dt
        self.logger.log_info(f'\nSCRIPT RUNTIME FOR THIS FILETYPE: {script_runtime.total_seconds():.4f}s')

    def _make_html(self):
        """Make the index html for this site"""
        html.make_file_overview(filetype=self.filesettings['FILE_FILEGROUP'],
                                site_html_outdir=self.filesettings['DIR_OUT_ICOS'],
                                settings_dict=self.filesettings,
                                run_id=self.run_id,
                                run_date=self.run_start_datestr,
                                table=self.filetype_logfile_contents)

    def _create_icos_filename(self, year, month, day) -> str:
        """Create ICOS filename"""
        outfilename_icos = \
            self.filesettings['FILENAME_FOR_ICOS'].format(
                year=year, month=month, day=day,
                logger=self.filesettings['OUTFILE_ICOS_LOGGERNUMBER_LN'],
                file=self.filesettings['OUTFILE_ICOS_FILENUMBER_FN'])
        return outfilename_icos

    def _check_if_already_processed(self, filename, grp_date) -> bool:
        if filename in self.filetype_logfile_contents:
            self.logger.log_info(f"{self.logger.current_section}    --| NOT creating daily file {filename} "
                                 f"for date {grp_date}, already listed in filetype logfile")
            return False
        return True

    def _export_data(self, df):
        """Export data to ICOS daily files"""
        with self.logger.section('[exporting daily files]'):
            self.logger.log_info(f"{self.logger.current_section} Working on merged data ({len(df)} values "
                                 f"between {df.index[0]} and {df.index[-1]})")
            self.logger.log_info(f"{self.logger.current_section} Filetype logfile: {self.logfilepath_alreadyprocessed}")

            # Group data by date, this works because the
            # timestamp index TIMESTAMP_MIDDLE is used for grouping
            grouped_daily = df.groupby(df.index.date)
            for grp_date, grp_df in grouped_daily:

                # Make filename for ICOS
                filename = self._create_icos_filename(year=grp_date.year, month=grp_date.month, day=grp_date.day)

                # Full output path to output files
                dir_out = tools.get_subdir_from_date(date=grp_date,
                                                     outpath=self.filesettings['DIR_OUT_ICOS'],
                                                     create=not self.dry_run)
                csv_path = dir_out / filename
                zip_path = dir_out / f"{Path(filename).stem}.zip"

                # Detect which filename to write to the filetype processing logfile
                output_filename = \
                    str(zip_path.name) \
                        if self.filesettings['OUTFILE_COMPRESSION'] \
                        else str(csv_path.name)

                # Check if filename already processed, if yes skip this file
                checkok = self._check_if_already_processed(filename=output_filename, grp_date=grp_date)
                if not checkok:
                    continue

                verb = "would create" if self.dry_run else "Creating"
                self.logger.log_info(f"{self.logger.current_section}    --> {verb} daily file {output_filename} "
                                     f"for date {grp_date}")

                # Basic data info
                firstdate = grp_df[self.icos_timestamp_col].iloc[0]
                lastdate = grp_df[self.icos_timestamp_col].iloc[-1]
                self.logger.log_info(f"{self.logger.current_section}        "
                                     f"(i) Data from {firstdate} to {lastdate} "
                                     f"({len(grp_df)} values, {len(grp_df.columns)} columns)")

                # Save daily file (CSV, optionally zipped, optionally delete uncompressed)
                self._save_daily_file(df=grp_df, csv_path=csv_path, zip_path=zip_path)

                # Record this file as processed (skipped during a dry run)
                if not self.dry_run:
                    self._add_filename_to_filetype_logfile(filename=output_filename)

    def _save_daily_file(self, df, csv_path, zip_path) -> None:
        """Save daily file: CSV → optionally ZIP → optionally delete uncompressed"""

        # Dry run: preview the file operations without touching the filesystem
        if self.dry_run:
            sec = self.logger.current_section
            self.logger.log_info(f"{sec}        * would save uncompressed ICOS file: {csv_path}")
            if self.filesettings['OUTFILE_COMPRESSION']:
                self.logger.log_info(f"{sec}        * would save compressed ICOS ZIP file: {zip_path}")
            if self.filesettings['OUTFILE_DELETE_UNCOMPRESSED']:
                self.logger.log_info(f"{sec}        * would delete uncompressed ICOS file: {csv_path}")
            return

        # Write CSV with ICOS formatting
        header = self.filesettings['DATA_HEADER_OUTPUT_TO_FILE']
        df.to_csv(csv_path,
                  quotechar='"',
                  quoting=csv.QUOTE_NONNUMERIC,
                  index=False,
                  header=header,
                  na_rep='NaN',
                  lineterminator='\r\n')
        self.logger.log_info(f"{self.logger.current_section}        * saved uncompressed ICOS file: {csv_path}")

        # Optionally compress to ZIP
        if self.filesettings['OUTFILE_COMPRESSION']:
            with zf.ZipFile(zip_path, 'w') as zipped_file:
                zipped_file.write(csv_path, compress_type=zf.ZIP_DEFLATED,
                                  arcname=os.path.basename(csv_path))
            self.logger.log_info(f"{self.logger.current_section}        * saved compressed ICOS ZIP file: {zip_path}")
        else:
            self.logger.log_info(f"{self.logger.current_section}        * no compressed ZIP file was created")

        # Optionally delete uncompressed CSV
        if self.filesettings['OUTFILE_DELETE_UNCOMPRESSED']:
            os.remove(csv_path)
            self.logger.log_info(f"{self.logger.current_section}        * deleted uncompressed ICOS file: {csv_path}")
        else:
            self.logger.log_info(f"{self.logger.current_section}        * uncompressed ICOS file {csv_path} was not deleted")

    def _format_data(self, df) -> DataFrame:
        with self.logger.section('[formatting data]'):
            self.logger.log_info(f"{self.logger.current_section} Working on merged data ({len(df)} values "
                                 f"between {df.index[0]} and {df.index[-1]})")

            # Rename columns
            df = self._rename_columns(df=df)

            # Remove duplicate indexes, keep last
            df = self._remove_duplicates(df=df)

            # Keep renamed columns only
            df = self._keep_only_renamed_columns(df=df)

            # Remove suffix from variables names
            df = self._delete_suffix_from_variable_names(df=df)

            # Make sure timestamp is continuous
            df = self._reindex_data_to_continuous_timestamp(df=df)

            # Insert ICOS timestamp as column for correct CSV export
            df = self._insert_icos_timestamp(
                df=df, keep_non_icos_timestamp=self.filesettings['DATA_TIMESTAMP_KEEP_NON_ICOS'])

            # Remove data from today's date
            df = self._remove_today_data(df=df)

            # Convert original timestamp index to TIMESTAMP_MIDDLE, for exporting correct daily files
            df = self._convert_index_to_middle_timestamp(df=df)

            # Remove partial days, when timestamp does not cover the full day
            df = self._remove_partial_days(df=df)

        return df

    def _remove_partial_days(self, df) -> DataFrame:
        """Remove partial days based on (filled) timestamp completeness"""
        n_expected_records_per_day = pd.Timedelta('1D') / self.filesettings['DATA_FREQUENCY']
        n_timestamps_per_day = df.groupby(df.index.date).count()[self.icos_timestamp_col]
        ix_equals_expected = n_timestamps_per_day == n_expected_records_per_day
        okdates = ix_equals_expected[ix_equals_expected]
        notokdates = ix_equals_expected[~ix_equals_expected]
        df['__DATE_AUX__'] = df.index.date
        df = df.loc[df['__DATE_AUX__'].isin(okdates.index)].copy()
        df = df.drop('__DATE_AUX__', axis=1)
        if len(notokdates) > 0:
            removed_dates = [f"{x}" for x in notokdates.index]
            self.logger.log_info(f"{self.logger.current_section}    * removed dates with timestamps not covering the "
                                 f"full day (partial days) {removed_dates}")
        else:
            self.logger.log_info(f"{self.logger.current_section}    * keeping all days, no partial days found")
        return df

    def _remove_today_data(self, df) -> DataFrame:
        """Remove data for today's date, some files go beyond midnight"""
        today_date = datetime.datetime.now().date()
        last_allowed_timestamp = datetime.datetime(year=today_date.year, month=today_date.month,
                                                   day=today_date.day, hour=0, minute=0, second=0)
        is_not_today = df.index <= last_allowed_timestamp
        is_today = ~is_not_today

        if np.sum(is_today) > 0:
            removed_dt = [f"{x}" for x in df[is_today].index]
            self.logger.log_info(f"{self.logger.current_section}    * removed {np.sum(is_today)} records "
                                 f"with today's date (today's data always ignored) {removed_dt}")
        else:
            self.logger.log_info(f"{self.logger.current_section}    * no records ({np.sum(is_today)} values) "
                                 f"with today's date found, nothing removed (today's data always ignored)")
        df = df[is_not_today].copy()
        return df

    def _convert_index_to_middle_timestamp(self, df) -> DataFrame:
        """Convert timestamp index to show MIDDLE of averaging interval"""
        # Original timestamp shows the END
        df.index = df.index - pd.to_timedelta(df.index.freq / 2)
        df.index.name = ('TIMESTAMP_MIDDLE', 'TS')
        self.logger.log_info(f"{self.logger.current_section}    * original timestamp was converted to "
                             f"TIMESTAMP_MIDDLE (only used for creating daily files, "
                             f"ICOS timestamp remains unchanged)")
        return df

    def _reindex_data_to_continuous_timestamp(self, df) -> DataFrame:
        """Generate continuous timestamp index b/w first and last date"""
        index_orig = df.index
        index_new = pd.date_range(start=df.index[0], end=df.index[-1], freq=self.filesettings['DATA_FREQUENCY'])
        is_equal = index_new.equals(index_orig)
        if not is_equal:
            different = index_new.difference(index_orig)
            df = df.reindex(index_new)
            self.logger.log_info(f"{self.logger.current_section}    * timestamp was not continuous, fixed "
                                 f"(added {len(different)} timestamps) ")
        else:
            self.logger.log_info(f"{self.logger.current_section}    * timestamp is already continuous, nothing changed")
        df = df.asfreq(self.filesettings['DATA_FREQUENCY'])
        return df

    def _delete_suffix_from_variable_names(self, df) -> DataFrame:
        if self.filesettings['DATA_HEADER_REMOVE_SUFFIX_FROM_VARIABLE_NAMES']:
            # data_df.columns = data_df.columns.get_level_values(0).str.replace('_Avg', '')
            for sfx in self.filesettings['DATA_HEADER_REMOVE_SUFFIX_FROM_VARIABLE_NAMES']:
                # the first row contains variable names
                df.columns = pd.MultiIndex.from_tuples([(x[0].replace(sfx, ''), x[1]) for x in df.columns])
            self.logger.log_info(
                f"{self.logger.current_section}    * removed the following suffices from the variable names: "
                f"{self.filesettings['DATA_HEADER_REMOVE_SUFFIX_FROM_VARIABLE_NAMES']}")
        else:
            self.logger.log_info(f"{self.logger.current_section}    * no suffices were removed from variable names")
        return df

    def _keep_only_renamed_columns(self, df) -> DataFrame:
        if self.filesettings['DATA_RENAME_COLUMNS']:
            keep_cols = []  # Make list of kept columns
            if self.icos_timestamp_col in df.columns:  # Always keep ICOS timestamp which is already in df
                keep_cols.append(self.icos_timestamp_col)
            for key, val in self.filesettings['DATA_RENAME_COLUMNS'].items():
                keep_cols.append(val)
            if self.filesettings['DATA_KEEP_ONLY_RENAMED_COLUMNS']:
                df = df[[c for c in df.columns if c in keep_cols]]  # Now keep only renamed cols
            self.logger.log_info(f"{self.logger.current_section}    * keeping only renamed columns: {keep_cols}")
        else:
            self.logger.log_info(f"{self.logger.current_section}    * keeping ALL columns")
        return df

    def _remove_duplicates(self, df) -> DataFrame:
        """Remove duplicates, keep last"""
        n_duplicates = df.index.duplicated().sum()
        if n_duplicates > 0:
            df = df[~df.index.duplicated(keep='last')]
            self.logger.log_info(f"{self.logger.current_section}    * removed {n_duplicates} duplicate records, "
                                 f"kept last (same timestamp)")
        else:
            self.logger.log_info(f"{self.logger.current_section}    * no duplicate records found (no timestamp duplicates)")
        return df

    def _rename_columns(self, df) -> DataFrame:
        if self.filesettings['DATA_RENAME_COLUMNS']:
            for old, new in self.filesettings['DATA_RENAME_COLUMNS'].items():
                df = df.rename(index=str, columns={old: new})
            self.logger.log_info(f"{self.logger.current_section}    * renamed columns: "
                                 f"{self.filesettings['DATA_RENAME_COLUMNS']}")
        else:
            self.logger.log_info(f"{self.logger.current_section}    * no columns were renamed")
        df.index = pd.to_datetime(df.index)  # Make sure index is datetime
        return df

    def _insert_icos_timestamp(self, df, keep_non_icos_timestamp) -> DataFrame:

        # Insert timestamp index as regular column (i.e. not pandas index)
        df.insert(0, self.icos_timestamp_col, df.index)
        df[self.icos_timestamp_col] = df[self.icos_timestamp_col].dt.strftime(
            self.filesettings['DATA_ICOS_TIMESTAMP_FORMAT'])
        self.logger.log_info(f"{self.logger.current_section}    * inserted ICOS timestamp with name "
                             f"{self.icos_timestamp_col} as first column")

        # Original timestamp
        if keep_non_icos_timestamp:
            df['_TIMESTAMP_OLD'] = df.index
            self.logger.log_info(f"{self.logger.current_section}    * keeping non-ICOS timestamp as _TIMESTAMP_OLD")
        else:
            self.logger.log_info(f"{self.logger.current_section}    * NOT keeping non-ICOS (original) timestamp")

        return df

    def _readfiles(self, filepaths: list):
        with self.logger.section('[reading file data]'):
            # Merge data from all files
            merged_df = pd.DataFrame()
            for filepath in filepaths:
                file_df = self._readfile(filepath=filepath)
                merged_df = pd.concat([merged_df, file_df], axis=0)  # add to data from this day

            self.logger.log_info(f"{self.logger.current_section}   {'-' * 40}\n"
                                 f"{self.logger.current_section}   {len(merged_df)} records are available "
                                 f"for further processing.")

        return merged_df

    def _readfile(self, filepath):

        # read data to df
        filedata_df = pd.read_csv(filepath,
                                  index_col=self.filesettings['DATA_TIMESTAMP_COL'],
                                  header=self.filesettings['DATA_HEADER_ROWS'],
                                  skiprows=self.filesettings['DATA_SKIP_ROWS'],
                                  encoding='utf-8',
                                  sep=self.filesettings['DATA_SEPARATOR'],
                                  on_bad_lines='skip',
                                  na_values=['NAN', 'inf'])  # 'inf' added in v4.0.15

        # Convert index to datetime (replaces deprecated date_parser parameter)
        filedata_df.index = pd.to_datetime(filedata_df.index,
                                           format=self.filesettings['DATA_TIMESTAMP_FORMAT'],
                                           errors='coerce')

        # Log
        n_rows = filedata_df.shape[0]
        n_cols = filedata_df.shape[1]
        datasize = filedata_df.size
        dtypes = filedata_df.dtypes
        self.logger.log_info(f'{self.logger.current_section}   Reading file {filepath.name} successful '
                             f'rows: {n_rows} / columns: {n_cols}  / datasize: {datasize} '
                             f'({filepath})')

        # Convert to numeric where possible
        for col in filedata_df.columns:
            try:
                filedata_df[col] = filedata_df[col].astype(np.float64)
            except ValueError as e:
                self.logger.log_info(
                    f"{self.logger.current_section}       (!)WARNING column {col} could not be converted to numeric ({e}), "
                    f"instead the column was converted to string")
                filedata_df[col] = filedata_df[col].astype(str)

        return filedata_df

    def _set_monthly_search_folders(self):
        """Set time range for search window and detect valid source folders"""

        # Search window
        start_date, end_date = tools.set_search_window(max_age_days=self.max_age_days)

        # Check in which subfolders we can start the search for new files
        search_dirs, start_date = \
            tools.set_search_folders(source_dir=self.filesettings['DIR_SOURCE_FILES'],
                                     search_firstdate=start_date,
                                     search_lastdate=end_date)

        # found search dirs
        self.logger.log_info(f"{self.logger.current_section} Searching for new files in:")
        for ix, search_dir in enumerate(search_dirs):
            self.logger.log_info(f"{self.logger.current_section}      DIR {ix}: {search_dir}")

        return search_dirs, start_date

    def _search_files(self, search_dirs) -> list:
        """Make list of all files in search dirs, store complete path to file"""
        files = []
        for search_dir in search_dirs:
            for root, dirs, filenames in os.walk(search_dir):
                for filename in filenames:
                    filepath = Path(root) / filename
                    files.append(filepath)
        return files

    def _validate_file(self, filepath, start_date) -> bool:
        """Validate file: check name pattern, read permission, and date range"""
        filename = filepath.name

        # Check filename matches expected pattern
        if not fnmatch.fnmatch(filename, self.filesettings['FILENAME_ID']):
            self.logger.log_info(f"{self.logger.current_section} (!) SKIPPING FILE {filename} "
                                 f"- not matching pattern ({self.filesettings['FILENAME_ID']})")
            return False

        # Check file is readable
        try:
            with open(filepath) as f:
                f.read(5)
        except PermissionError as err:
            self.logger.log_info(f"{self.logger.current_section} (!) SKIPPING FILE - NO READ PERMISSION: {filepath} ({err})")
            return False

        # Check filedate is within search window
        file_date = tools.get_datetime_from_filename(filename=filename, filesettings=self.filesettings).date()
        if file_date < start_date:
            self.logger.log_info(f"{self.logger.current_section} (!) SKIPPING FILE {filename} "
                                 f"- filedate {file_date} older than start date {start_date}")
            return False

        return True

    def _remove_files_already_processed(self, df, section_name) -> DataFrame:
        """Remove files that were already processed"""
        already_processed = df.index.isin(self.prev_run_log_df.index)
        df = df.loc[~already_processed].copy()
        msg = f"{section_name} (!) FILES REMOVED: ALREADY PROCESSED\n" \
              f"{section_name} files that already appear in the log as processed are ignored\n" \
              f"{df.loc[already_processed].sort_index()}"
        self.logger.log_info(msg)
        return df

    def _generate_file_list(self):
        """Search valid files and store info in dataframe"""
        with self.logger.section('[generate_file_list]'):
            # Expand time range to include previous date,
            # in case complementary data from the previous date is needed
            if self.filesettings['DATA_COMPLEMENT_WITH_PREVIOUS_DATE']:
                self.max_age_days += 1

            # Set source dirs for searching files
            search_dirs, start_date = self._set_monthly_search_folders()

            # Make list of all files in search dirs
            files = self._search_files(search_dirs=search_dirs)

            # Dataframe to collect valid files
            files_df = pd.DataFrame()

            for filepath in files:

                # Validate file (pattern, permissions, date range)
                if not self._validate_file(filepath=filepath, start_date=start_date):
                    continue

                # Get date from filename
                file_date = tools.get_datetime_from_filename(filename=filepath.name, filesettings=self.filesettings)

                # Add file to df
                # the index is the date contained in the filename
                files_df.loc[file_date, 'RUN_ID'] = self.run_id
                files_df.loc[file_date, 'RUN_DATETIME'] = self.run_start_dt
                files_df.loc[file_date, 'ETH_FILENAME'] = filepath.name
                files_df.loc[file_date, 'ETH_FILEPATH'] = filepath
                files_df.loc[file_date, 'ETH_FILEDATE'] = file_date.date()

            files_df = files_df.sort_index()

            # Check if there is at least one file available, otherwise stop script
            checkok = self._check_if_files_available(files_df=files_df)
            if not checkok:
                raise NoFilesFoundError(
                    f"No files found for {self.filesettings['FILE_FILEGROUP']}")

            # Log
            available_files = files_df['ETH_FILEPATH'].to_list()
            for file in available_files:
                msg = f"{self.logger.current_section}  ++ ADDING FILE   {file}   - for further processing"
                self.logger.log_info(msg)
            self.logger.log_info(f"{self.logger.current_section}   {'-' * 40}\n"
                                 f"{self.logger.current_section}   {len(files_df)} files are available "
                                 f"for further processing.")

        return files_df

    def _check_if_files_available(self, files_df) -> bool:
        """Check if at least one file is available for further processing"""
        if not files_df.empty:
            checkok = True
        else:
            checkok = False
            msg = '\n{s}\n(!) No files found (DataFrame of found files is empty). Stopping script.\n{s}'.format(
                s='*' * 80)
            self.logger.log_info(msg)
        return checkok

    def _read_filetype_logfile(self):
        """Read logfile of already-processed files"""
        if self.logfilepath_alreadyprocessed.is_file():
            with open(self.logfilepath_alreadyprocessed) as f:
                contents = f.read()
        elif self.dry_run:
            # Dry run must not create any file
            contents = ""
        else:
            # Create logfile if it does not exist yet
            with open(self.logfilepath_alreadyprocessed, 'w') as f:
                f.write(f"================================================\n")
                f.write(f"FILES ALREADY PROCESSED AND CREATED WITH ppicos\n")
                f.write(f"================================================\n")
                f.write(f"* Files listed here are not re-processed\n")
                f.write(f"* Delete files from list this enable re-processing with ppicos\n")
                f.write(f"------------------------------------------------\n")
            contents = ""
        return contents

    def _add_filename_to_filetype_logfile(self, filename: str) -> None:
        """
        Add filename to filetype logfile that stores names
        of files that were already processed
        """
        # Add filename to logfile
        writemode = 'a' if self.logfilepath_alreadyprocessed.is_file() else 'w'
        with open(self.logfilepath_alreadyprocessed, writemode) as f:
            f.write(f"{filename}    created {datetime.datetime.now()}")
            f.write('\n')

    def _setup_logger(self):
        """Setup text output to console and file"""
        logpath = tools.get_subdir_from_date(
            date=self.run_start_dt,
            outpath=self.filesettings['DIR_OUT_ICOS'] / self.filesettings['DIR_OUT_LOGFILE'],
            create=not self.dry_run)
        logger = Logger(run_id=self.run_id,
                        logdir=logpath,
                        filetype=self.filesettings['FILE_FILEGROUP'],
                        write_file=not self.dry_run,
                        echo_console=self.echo_console)  # initialize logging
        facts = {
            'ID': self.filesettings['FILENAME_ID'],
            'Start': self.run_start_datestr,
            'Run ID': self.run_id,
            'Source': self.filesettings['DIR_SOURCE_FILES'],
            'Output': self.filesettings['DIR_OUT_ICOS'],
            'Log file': 'not written (dry run)' if self.dry_run else logpath,
        }
        logger.startup(title=self.filesettings['FILE_FILEGROUP'],
                       facts=facts,
                       settings=self.filesettings,
                       source_dir=self.filesettings['DIR_SOURCE_FILES'])
        return logger
