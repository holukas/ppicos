"""

=======
pp-icos
=======

Post-processing of raw data files:
Format original raw data files to ICOS-compliant format.

"""
import csv
import datetime
import fnmatch
import os
import sys
import zipfile as zf
from pathlib import Path

import numpy as np
import pandas as pd
from pandas import DataFrame

import html

pd.options.display.width = None
pd.options.display.max_columns = None
# pd.set_option('display.max_rows', 3000)
pd.set_option('display.max_columns', 3000)
import logger
import tools
from logger import Logger


class IcosFormat:
    """
    Format original raw data files to ICOS-compliant format
    """

    def __init__(self,
                 filesettings: dict,
                 max_age_days: int = 5):
        self.filesettings = filesettings
        self.max_age_days = max_age_days

        # Make identifier for this run
        self.run_id, self.run_start_datestr, self.run_start_dt = tools.make_run_id()

        # Init logger
        self.logger = self._setup_logger()

        # Location of filetype logfile, stores names of already-processed files
        logfilename_alreadyprocessed = f"pp-icos_{self.filesettings['FILE_FILEGROUP']}_files-already-processed.log"
        self.logfilepath_alreadyprocessed = self.filesettings['DIR_OUT_ICOS'] / logfilename_alreadyprocessed
        self.filetype_logfile_contents = self._read_filetype_logfile()

        # ICOS timestamp column
        if len(self.filesettings['DATA_HEADER_ROWS']) > 1:
            self.icos_timestamp_col = ('TIMESTAMP', 'TS')
        else:
            self.icos_timestamp_col = 'TIMESTAMP'

    def run(self):
        """Run pp-icos processing chain"""

        # Search files in source and make a dataframe
        input_files_df = self._generate_file_list()

        # Read all data from input files to dataframe
        merged_df = self._readfiles(filepaths=input_files_df['ETH_FILEPATH'].to_list())

        # Format data to ICOS formats
        merged_df = self._format_data(df=merged_df)

        # Export data as daily files
        self._export_data(df=merged_df)

        # # Make HTML overview list
        # self._make_html()

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

    def _check_if_already_processed(self, filename, grp_date, section_name) -> bool:
        if filename in self.filetype_logfile_contents:
            checkok = False
            self.logger.log_info(f"{section_name}    --| NOT creating daily file {filename} "
                                 f"for date {grp_date}, already listed in filetype logfile")
        else:
            checkok = True
        return checkok

    def _export_data(self, df):
        """Export data to ICOS daily files"""

        # Start section
        section_name = '[exporting daily files]'
        tic = logger.section_start(logger=self.logger, section_name=section_name)
        self.logger.log_info(f"{section_name} Working on merged data ({len(df)} values "
                             f"between {df.index[0]} and {df.index[-1]})")
        self.logger.log_info(f"{section_name} Filetype logfile: {self.logfilepath_alreadyprocessed}")

        # Group data by date, this works because the
        # timestamp index TIMESTAMP_MIDDLE is used for grouping
        grouped_daily = df.groupby(df.index.date)
        for grp_date, grp_df in grouped_daily:

            # Make filename for ICOS
            outfilename_icos = self._create_icos_filename(year=grp_date.year, month=grp_date.month, day=grp_date.day)

            # Full output path to output files
            # Detect output path with subdir from filedate
            outpath = tools.get_subdir_from_date(date=grp_date, outpath=self.filesettings['DIR_OUT_ICOS'])
            icos_uncompressed_outfilepath = outpath / outfilename_icos
            icos_zipped_outfilepath = outpath / f"{Path(outfilename_icos).stem}.zip"

            # Detect which filename to write to the filetype processing logfile
            filename_for_filetype_logfile = \
                str(icos_zipped_outfilepath.name) \
                    if self.filesettings['OUTFILE_COMPRESSION'] \
                    else str(icos_uncompressed_outfilepath.name)

            # Check if filename already processed, if yes skip this file
            checkok = self._check_if_already_processed(filename=filename_for_filetype_logfile,
                                                       grp_date=grp_date,
                                                       section_name=section_name)
            if not checkok: continue

            self.logger.log_info(f"{section_name}    --> Creating daily file {filename_for_filetype_logfile} "
                                 f"for date {grp_date}")

            # Basic data info
            firstdate = grp_df[self.icos_timestamp_col][0]
            lastdate = grp_df[self.icos_timestamp_col][-1]
            self.logger.log_info(f"{section_name}        "
                                 f"(i) Data from {firstdate} to {lastdate} "
                                 f"({len(grp_df)} values, {len(grp_df.columns)} columns)")

            # Save uncompressed ICOS file
            self._save_uncompressed_icos_file(df=grp_df,
                                              outfilepath=icos_uncompressed_outfilepath,
                                              section_name=section_name)

            # Save zipped ICOS file (if required)
            self._save_zipped_icos_file(filepath_to_compress=icos_uncompressed_outfilepath,
                                        outfilepath=icos_zipped_outfilepath,
                                        section_name=section_name)

            # Delete uncompressed ICOS file (if required)
            self._delete_uncompressed_icos_file(outfilepath_uncompressed=icos_uncompressed_outfilepath,
                                                section_name=section_name)

            # Get info about previous script runs
            self._add_filename_to_filetype_logfile(filename=filename_for_filetype_logfile)

        # End section
        logger.section_end(logger=self.logger, section_name=section_name, tic=tic)

    def _delete_uncompressed_icos_file(self, outfilepath_uncompressed, section_name) -> None:
        """Delete uncompressed file if needed (optional)"""
        if self.filesettings['OUTFILE_DELETE_UNCOMPRESSED']:
            os.remove(outfilepath_uncompressed)
            self.logger.log_info(
                f"{section_name}        * deleted uncompressed ICOS file: {outfilepath_uncompressed}")
        else:
            self.logger.log_info(
                f"{section_name}        * uncompressed ICOS file {outfilepath_uncompressed} was not deleted")

    def _save_zipped_icos_file(self, filepath_to_compress, outfilepath, section_name) -> None:
        """Saves compressed (zipped) ICOS file"""
        if self.filesettings['OUTFILE_COMPRESSION']:

            # Write to ZIP
            zipped_file = zf.ZipFile(outfilepath, 'w')
            # note the arcname argument: it enables to directly zip the file w/o including the
            # file-containing folder in the zip file
            zipped_file.write(filepath_to_compress, compress_type=zf.ZIP_DEFLATED,
                              arcname=os.path.basename(filepath_to_compress))
            zipped_file.close()
            self.logger.log_info(f"{section_name}        * saved compressed ICOS ZIP file: {outfilepath}")

            # f_hash = self.hash_value_for_file(file_full_path=outfilepath_zip)
            # self.file_list_df.loc[orig_filedate, 'saved_icos_zip_file_hash'] = '{0}'.format(f_hash)
        else:
            self.logger.log_info(f"{section_name}        * no compressed ZIP file was created")

    def _save_uncompressed_icos_file(self, df, outfilepath, section_name) -> None:
        """ Saves uncompressed data as CSV
        - File is saved w/ ICOS filename
        - CSV is generated using quote arguments to output double quotes for header, index and NaNs
        """

        # check if we need to output the header column names to the file
        header = True if self.filesettings['DATA_HEADER_OUTPUT_TO_FILE'] else False

        # Save dataframe to file
        # - note the quote arguments: these make sure that the header, row indices and also NANs
        #   are output with double quotes. also, index=False b/c timestamp was inserted as regular column
        # - na_rep is used to fix representation for NaNs in output csv file
        #   for the ICOS files, NaN is used, in combination with the quote args NaN is output as "NaN"
        #   we need it like this b/c we defined missing values for the files as "NaN"
        df.to_csv(outfilepath,
                  quotechar='"',
                  quoting=csv.QUOTE_NONNUMERIC,
                  index=False,
                  header=header,
                  na_rep='NaN',
                  lineterminator='\r\n')

        self.logger.log_info(f"{section_name}        * saved uncompressed ICOS file: {outfilepath}")

    def _detect_unique_dates(self, df, section_name):
        """Detect unique dates in dataframe"""
        unique_dates = list(df.index.date)
        unique_dates = np.unique(unique_dates)
        self.logger.log_info(f"{section_name}    Found {len(unique_dates)} unique dates in merged data:")
        for ud in unique_dates:
            self.logger.log_info(f"{section_name}        Found date {ud}")

    def _format_data(self, df) -> DataFrame:

        # Start section
        section_name = '[formatting data]'
        tic = logger.section_start(logger=self.logger, section_name=section_name)
        self.logger.log_info(f"{section_name} Working on merged data ({len(df)} values "
                             f"between {df.index[0]} and {df.index[-1]})")

        # Rename columns
        df = self._rename_columns(df=df, section_name=section_name)

        # Remove duplicate indexes, keep last
        df = self._remove_duplicates(df=df, section_name=section_name)

        # Keep renamed columns only
        df = self._keep_only_renamed_columns(df=df, section_id=section_name)

        # Remove suffix from variables names
        df = self._delete_suffix_from_variable_names(df=df, section_name=section_name)

        # Make sure timestamp is continuous
        df = self._reindex_data_to_continuous_timestamp(df=df, section_name=section_name)

        # Insert ICOS timestamp as column for correct CSV export
        df = self._insert_icos_timestamp(
            df=df, keep_non_icos_timestamp=self.filesettings['DATA_TIMESTAMP_KEEP_NON_ICOS'],
            section_name=section_name)

        # Remove data from today's date
        df = self._remove_today_data(df=df, section_name=section_name)

        # Convert original timestamp index to TIMESTAMP_MIDDLE, for exporting correct daily files
        df = self._convert_index_to_middle_timestamp(df=df, section_name=section_name)

        # Remove partial days, when timestamp does not cover the full day
        df = self._remove_partial_days(df=df, section_name=section_name)

        # End section
        logger.section_end(logger=self.logger, section_name=section_name, tic=tic)

        return df

    def _remove_partial_days(self, df, section_name) -> DataFrame:
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
            removed_dates = []
            [removed_dates.append(f"{x}") for x in notokdates.index]
            self.logger.log_info(f"{section_name}    * removed dates with timestamps not covering the "
                                 f"full day (partial days) {removed_dates}")
        else:
            self.logger.log_info(f"{section_name}    * keeping all days, no partial days found")
        return df

    def _remove_today_data(self, df, section_name) -> DataFrame:
        """Remove data for today's date, some files go beyond midnight"""
        today_date = datetime.datetime.now().date()
        last_allowed_timestamp = datetime.datetime(year=today_date.year, month=today_date.month,
                                                   day=today_date.day, hour=0, minute=0, second=0)
        is_not_today = df.index <= last_allowed_timestamp
        is_today = ~is_not_today

        if np.sum(is_today) > 0:
            removed_dt = []
            [removed_dt.append(f"{x}") for x in df[is_today].index]
            self.logger.log_info(f"{section_name}    * removed {np.sum(is_today)} records "
                                 f"with today's date (today's data always ignored) {removed_dt}")
        else:
            self.logger.log_info(f"{section_name}    * no records ({np.sum(is_today)} values) "
                                 f"with today's date found, nothing removed (today's data always ignored)")
        df = df[is_not_today].copy()
        return df

    def _convert_index_to_middle_timestamp(self, df, section_name) -> DataFrame:
        """Convert timestamp index to show MIDDLE of averaging interval"""
        # Original timestamp shows the END
        df.index = df.index - pd.to_timedelta(df.index.freq / 2)
        df.index.name = ('TIMESTAMP_MIDDLE', 'TS')
        self.logger.log_info(f"{section_name}    * original timestamp was converted to "
                             f"TIMESTAMP_MIDDLE (only used for creating daily files, "
                             f"ICOS timestamp remains unchanged)")
        return df

    def _reindex_data_to_continuous_timestamp(self, df, section_name) -> DataFrame:
        """Generate continuous timestamp index b/w first and last date"""
        index_orig = df.index
        index_new = pd.date_range(start=df.index[0], end=df.index[-1], freq=self.filesettings['DATA_FREQUENCY'])
        is_equal = index_new.equals(index_orig)
        if not is_equal:
            different = index_new.difference(index_orig)
            df = df.reindex(index_new)
            self.logger.log_info(f"{section_name}    * timestamp was not continuous, fixed "
                                 f"(added {len(different)} timestamps) ")
        else:
            self.logger.log_info(f"{section_name}    * timestamp is already continuous, nothing changed")
        df = df.asfreq(self.filesettings['DATA_FREQUENCY'])
        return df

    def _delete_suffix_from_variable_names(self, df, section_name) -> DataFrame:
        if self.filesettings['DATA_HEADER_REMOVE_SUFFIX_FROM_VARIABLE_NAMES']:
            # data_df.columns = data_df.columns.get_level_values(0).str.replace('_Avg', '')
            for sfx in self.filesettings['DATA_HEADER_REMOVE_SUFFIX_FROM_VARIABLE_NAMES']:
                # the first row contains variable names
                df.columns = pd.MultiIndex.from_tuples([(x[0].replace(sfx, ''), x[1]) for x in df.columns])
            self.logger.log_info(
                f"{section_name}    * removed the following suffices from the variable names: "
                f"{self.filesettings['DATA_HEADER_REMOVE_SUFFIX_FROM_VARIABLE_NAMES']}")
        else:
            self.logger.log_info(f"{section_name}    * no suffices were removed from variable names")
        return df

    def _keep_only_renamed_columns(self, df, section_id) -> DataFrame:
        if self.filesettings['DATA_RENAME_COLUMNS']:
            keep_cols = []  # Make list of kept columns
            if self.icos_timestamp_col in df.columns:  # Always keep ICOS timestamp which is already in df
                keep_cols.append(self.icos_timestamp_col)
            for key, val in self.filesettings['DATA_RENAME_COLUMNS'].items():
                keep_cols.append(val)
            if self.filesettings['DATA_KEEP_ONLY_RENAMED_COLUMNS']:
                df = df[[c for c in df.columns if c in keep_cols]]  # Now keep only renamed cols
            self.logger.log_info(f"{section_id}    * keeping only renamed columns: {keep_cols}")
        else:
            self.logger.log_info(f"{section_id}    * keeping ALL columns")
        return df

    def _remove_duplicates(self, df, section_name) -> DataFrame:
        """Remove duplicates, keep last"""
        n_duplicates = df.index.duplicated().sum()
        if n_duplicates > 0:
            df = df[~df.index.duplicated(keep='last')]
            self.logger.log_info(f"{section_name}    * removed {n_duplicates} duplicate records, "
                                 f"kept last (same timestamp)")
        else:
            self.logger.log_info(f"{section_name}    * no duplicate records found (no timestamp duplicates)")
        return df

    def _rename_columns(self, df, section_name) -> DataFrame:
        if self.filesettings['DATA_RENAME_COLUMNS']:
            for old, new in self.filesettings['DATA_RENAME_COLUMNS'].items():
                df.rename(index=str, columns={old: new}, inplace=True)
            self.logger.log_info(f"{section_name}    * renamed columns: "
                                 f"{self.filesettings['DATA_RENAME_COLUMNS']}")
        else:
            self.logger.log_info(f"{section_name}    * no columns were renamed")
        df.index = pd.to_datetime(df.index)  # Make sure index is datetime
        return df

    def _insert_icos_timestamp(self, df, keep_non_icos_timestamp, section_name) -> DataFrame:

        # Insert timestamp index as regular column (i.e. not pandas index)
        df.insert(0, self.icos_timestamp_col, df.index)
        df[self.icos_timestamp_col] = df[self.icos_timestamp_col].dt.strftime(
            self.filesettings['DATA_ICOS_TIMESTAMP_FORMAT'])
        self.logger.log_info(f"{section_name}    * inserted ICOS timestamp with name "
                             f"{self.icos_timestamp_col} as first column")

        # Original timestamp
        if keep_non_icos_timestamp:
            df['_TIMESTAMP_OLD'] = df.index
            self.logger.log_info(f"{section_name}    * keeping non-ICOS timestamp as _TIMESTAMP_OLD")
        else:
            self.logger.log_info(f"{section_name}    * NOT keeping non-ICOS (original) timestamp")

        return df

    def _readfiles(self, filepaths: list):

        # Start section
        section_name = '[reading file data]'
        tic = logger.section_start(logger=self.logger, section_name=section_name)

        # Merge data from all files
        merged_df = pd.DataFrame()
        for filepath in filepaths:
            file_df = self._readfile(filepath=filepath, section_name=section_name)
            merged_df = pd.concat([merged_df, file_df], axis=0)  # add to data from this day

        # End section
        self.logger.log_info(f"{section_name}   {'-' * 40}\n"
                             f"{section_name}   {len(merged_df)} records are available "
                             f"for further processing.")
        logger.section_end(logger=self.logger, section_name=section_name, tic=tic)

        return merged_df

    def _readfile(self, filepath, section_name: str = None):

        # kudos: https://stackoverflow.com/a/46545843
        dateparse = lambda date: pd.to_datetime(date, format=self.filesettings['DATA_TIMESTAMP_FORMAT'],
                                                errors='coerce')

        # read data to df
        filedata_df = pd.read_csv(filepath,
                                  parse_dates=True,
                                  date_parser=dateparse,
                                  index_col=self.filesettings['DATA_TIMESTAMP_COL'],
                                  header=self.filesettings['DATA_HEADER_ROWS'],
                                  skiprows=self.filesettings['DATA_SKIP_ROWS'],
                                  encoding='utf-8',
                                  sep=self.filesettings['DATA_SEPARATOR'],
                                  on_bad_lines='skip',
                                  na_values=['NAN', 'inf'])  # 'inf' added in v4.0.15

        # # Indexes of rows that contain 'inf'
        # data_df.index[np.isinf(data_df).any(1)]

        # Log
        n_rows = filedata_df.shape[0]
        n_cols = filedata_df.shape[1]
        datasize = filedata_df.size
        dtypes = filedata_df.dtypes
        self.logger.log_info(f'{section_name}   Reading file {filepath.name} successful '
                             f'rows: {n_rows} / columns: {n_cols}  / datasize: {datasize} '
                             f'({filepath})')

        # Convert to numeric where possible
        for col in filedata_df.columns:
            try:
                filedata_df[col] = filedata_df[col].astype(np.float64)
            except ValueError as e:
                self.logger.log_info(f"{section_name}       (!)WARNING column {col} could not be converted to numeric ({e}), "
                                     f"instead the column was converted to string")
                filedata_df[col] = filedata_df[col].astype(str)

        # # NOT DONE FOR ICOS FILES: fill date range, no date gaps, needs freq
        # first_date = data_df.index[0]
        # last_date = data_df.index[-1]

        return filedata_df

    def _set_monthly_search_folders(self, section_name: str = None):
        """Set time range for search window and detect valid source folders"""

        # Search window
        search_firstdate, search_lastdate = tools.set_search_window(max_age_days=self.max_age_days)

        # Check in which subfolders we can start the search for new files
        search_dirs, search_firstdate = \
            tools.set_search_folders(source_dir=self.filesettings['DIR_SOURCE_FILES'],
                                     search_firstdate=search_firstdate,
                                     search_lastdate=search_lastdate)

        # found search dirs
        self.logger.log_info(f"{section_name} Searching for new files in:")
        for ix, search_dir in enumerate(search_dirs):
            self.logger.log_info(f"{section_name}      DIR {ix}: {search_dir}")

        return search_dirs, search_firstdate

    # def _check_read_permission(self, search_dir: str, section_name: str = None) -> bool:
    #     """Check folder read permission"""
    #     checkok = False
    #     try:
    #         # If search dir exists, try to access it
    #         if os.path.isdir(search_dir):
    #             os.listdir(search_dir)
    #             checkok = True
    #     except PermissionError as err:
    #         self.logger.log_info(f"{section_name} (!) SKIPPING SEARCH FOLDER {search_dir} "
    #                              f"- no read permission on server ({err})")
    #         checkok = False
    #     return checkok

    def _search_files(self, search_dirs) -> list:
        """Make list of all files in search dirs, store complete path to file"""
        fileslist = []
        for search_dir in search_dirs:
            for root, dirs, files in os.walk(search_dir):
                for file_ix, file in enumerate(files):
                    filepath = Path(root) / file
                    fileslist.append(filepath)
        return fileslist

    def _check_filename_id(self, filename, section_name: str = None) -> bool:
        if not fnmatch.fnmatch(filename, self.filesettings['FILENAME_ID']):
            msg = f"{section_name} (!) SKIPPING FILE {filename} " \
                  f"- not matching pattern ({self.filesettings['FILENAME_ID']})"
            self.logger.log_info(msg)
            checkok = False
        else:
            checkok = True
        return checkok

    def _check_date_in_filename(self, filename: str, search_firstdate, section_name: str = None) -> bool:
        filename_date = tools.get_datetime_from_filename(filename=filename, filesettings=self.filesettings)
        filename_date = filename_date.date()
        today_date = datetime.datetime.now().date()
        if (filename_date >= search_firstdate) & (filename_date != today_date):
            checkok = True
        elif filename_date == today_date:
            checkok = False
            msg = f"{section_name} (!) SKIPPING FILE {filename} - filedate {filename_date} is today (today's file is ignored)"
            self.logger.log_info(msg)
        else:
            checkok = False
            msg = f"{section_name} (!) SKIPPING FILE {filename} - filedate {filename_date} older than start date {search_firstdate}"
            self.logger.log_info(msg)
        return checkok

    def _remove_files_already_processed(self, df, section_name) -> DataFrame:
        """Remove files that were already processed"""
        already_processed = df.index.isin(self.prev_run_log_df.index)
        df = df.loc[~already_processed].copy()
        msg = f"{section_name} (!) FILES REMOVED: ALREADY PROCESSED\n" \
              f"{section_name} files that already appear in the log as processed are ignored\n" \
              f"{df.loc[already_processed].sort_index()}"
        self.logger.log_info(msg)
        return df

    def _check_file_read_permission(self, filepath, section_name) -> bool:
        """Remove files that cannot be accessed"""
        try:
            with open(filepath) as f:
                s = f.read(5)
            checkok = True
        except PermissionError as err:
            msg = f"{section_name} (!) SKIPPING FILE - NO READ PERMISSION: {filepath} ({err})"
            self.logger.log_info(msg)
            checkok = False
        return checkok

    def _generate_file_list(self):
        """Search valid files and store info in dataframe"""

        # Start section
        section_name = '[generate_file_list]'
        tic = logger.section_start(logger=self.logger, section_name=section_name)

        # Expand time range to include previous date,
        # in case complementary data from the previous date is needed
        if self.filesettings['DATA_COMPLEMENT_WITH_PREVIOUS_DATE']:
            self.max_age_days += 1

        # Set source dirs for searching files
        search_dirs, search_firstdate = self._set_monthly_search_folders(section_name=section_name)

        # Make list of all files in search dirs
        fileslist = self._search_files(search_dirs=search_dirs)

        # Dataframe to collect valid files
        files_df = pd.DataFrame()

        for filepath in fileslist:

            kwargs = dict(section_name=section_name)

            # Check filename ID
            checkok = self._check_filename_id(filename=filepath.name, **kwargs)
            if not checkok: continue

            # Check if file has read permissions
            checkok = self._check_file_read_permission(filepath=filepath, **kwargs)
            if not checkok: continue

            # Check if filedate is within search window
            checkok = self._check_date_in_filename(filename=filepath.name, search_firstdate=search_firstdate, **kwargs)
            if not checkok: continue

            # Get date from filename
            filename_dt = tools.get_datetime_from_filename(filename=filepath.name, filesettings=self.filesettings)

            # Add file to df
            # the index is the date contained in the filename
            files_df.loc[filename_dt, 'RUN_ID'] = self.run_id
            files_df.loc[filename_dt, 'RUN_DATETIME'] = self.run_start_dt
            files_df.loc[filename_dt, 'ETH_FILENAME'] = filepath.name
            files_df.loc[filename_dt, 'ETH_FILEPATH'] = filepath
            files_df.loc[filename_dt, 'ETH_FILEDATE'] = filename_dt.date()

        files_df = files_df.sort_index()

        # Check if there is at least one file available, otherwise stop script
        checkok = self._check_if_files_available(files_df=files_df)
        if not checkok: sys.exit(-1)

        # Log
        available_files = files_df['ETH_FILEPATH'].to_list()
        for file in available_files:
            msg = f"{section_name}  ++ ADDING FILE   {file}   - for further processing"
            self.logger.log_info(msg)
        self.logger.log_info(f"{section_name}   {'-' * 40}\n"
                             f"{section_name}   {len(files_df)} files are available "
                             f"for further processing.")
        logger.section_end(logger=self.logger, section_name=section_name, tic=tic)

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
        else:
            # Create logfile if it does not exist yet
            with open(self.logfilepath_alreadyprocessed, 'w') as f:
                f.write(f"================================================\n")
                f.write(f"FILES ALREADY PROCESSED AND CREATED WITH pp-icos\n")
                f.write(f"================================================\n")
                f.write(f"* Files listed here are not re-processed\n")
                f.write(f"* Delete files from list this enable re-processing with pp-icos\n")
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
            outpath=self.filesettings['DIR_OUT_ICOS'] / self.filesettings['DIR_OUT_LOGFILE'])
        tools.check_if_path_exists(path=logpath)
        logger = Logger(run_id=self.run_id,
                        logdir=logpath,
                        filetype=self.filesettings['FILE_FILEGROUP'])  # initialize logging
        logger.log_info('\n\n\n\n\n{s}\n\n     {f}\n\n{s}'.format(s='=' * 120, f=self.filesettings['FILE_FILEGROUP']))
        logger.log_info('FILETYPE:      {}'.format(self.filesettings['FILE_FILEGROUP']))
        logger.log_info('FILETYPE ID:   {}'.format(self.filesettings['FILENAME_ID']))
        logger.log_info('START TIME:    {}'.format(self.run_start_datestr))
        logger.log_info('RUN ID:        {}'.format(self.run_id))
        logger.log_info('LOG FILE PATH: {}'.format(logpath))
        tools.print_settings_dict(settings_dict=self.filesettings, logger=logger)  # Log and print settings
        # Source folder, for testing relative to the script folder
        logger.log_info('\nsource dir:  {}'.format(self.filesettings['DIR_SOURCE_FILES']))
        return logger
