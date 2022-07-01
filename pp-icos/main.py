import multiprocessing
import csv
import datetime as dt
import fnmatch
import os
import sys
import time
import zipfile as zf
from pathlib import Path
import numpy as np

import pandas as pd

import file_formats as ff
import html
import tools
from logger import Logger


class format_to_icos(object):
    def __init__(self, file_format, max_age_days):

        # search for files in the last x days
        self.max_age_days = max_age_days

        # make identifier for this run
        self.run_id, self.run_start_datestr, self.run_start_dt = tools.make_run_id()

        # get info for file and store it in dictionary
        self.f_settings = file_format

        # logging (to console & file)
        logpath = tools.get_subdir_from_date(
            date=self.run_start_dt,
            outpath=self.f_settings['icos_file_outpath'] / self.f_settings['f_log_subdir'])
        tools.check_if_path_exists(path=logpath)
        self.logger = Logger(run_id=self.run_id,
                             logdir=logpath,
                             filetype=self.f_settings['f_type'])  # initialize logging

        self.logger.log_info('\n\n\n\n\n{s}\n\n     {f}\n\n{s}'.format(s='=' * 120, f=self.f_settings['f_type']))
        self.logger.log_info('FILETYPE:      {}'.format(self.f_settings['f_type']))
        self.logger.log_info('FILETYPE ID:   {}'.format(self.f_settings['fname_id']))
        self.logger.log_info('START TIME:    {}'.format(self.run_start_datestr))
        self.logger.log_info('RUN ID:        {}'.format(self.run_id))
        self.logger.log_info('LOG FILE PATH: {}'.format(logpath))

        # log settings
        tools.print_settings_dict(settings_dict=self.f_settings, logger=self.logger)

        # source folder, for testing relative to the script folder
        self.logger.log_info('\nsource dir:  {}'.format(self.f_settings['f_source_dir']))

        # get info about previous PP-ICOS runs
        self.previous_runs()

        # search files in source and make a DataFrame
        self.input_files_df = self.generate_file_list()
        self.logger.log_info('\nfound files:  {}'.format(self.input_files_df))

        # cycle through all found files
        if self.input_files_df.empty:
            self.logger.log_info(
                '\n{s}\nNo files found (DataFrame of found files is empty). Stopping script.\n{s}'.format(
                    s='*' * 80))
            # sys.exit(0)
        else:
            self.file_loop()

        # add loop info to overview file
        self.prev_run_log_df = self.prev_run_log_df.append(self.input_files_df)
        self.prev_run_log_df.to_csv(self.prev_run_log_filepath)

        # ----------------------------------------------------------
        # MAKE THE INDEX HTML FOR THIS SITE
        html.make_file_overview(filetype=self.f_settings['f_type'],
                                site_html_outdir=self.f_settings['icos_file_outpath'],
                                settings_dict=self.f_settings,
                                run_id=self.run_id,
                                run_date=self.run_start_datestr,
                                table=self.prev_run_log_df.to_html())

        # runtime
        script_runtime = dt.datetime.now() - self.run_start_dt
        self.logger.log_info('\nSCRIPT RUNTIME FOR THIS FILETYPE: {:.4f}s'.format(script_runtime.total_seconds()))

    def previous_runs(self):
        """ Checks if the overview log file of exists. If not, overview log file is created. """
        self.prev_run_log_filepath = \
            self.f_settings['icos_file_outpath'] / 'PP-ICOS_{}_overview_log.csv'.format(self.f_settings['f_type'])
        if self.prev_run_log_filepath.is_file():
            # datetimes can have different formats in the file, depending on the file resolution
            try:
                self.prev_run_log_df = \
                    tools.read_prev_run_logfile(filepath=self.prev_run_log_filepath,
                                                dateparser_format='%Y-%m-%d %H:%M:%S')
            except:
                pass

            try:
                self.prev_run_log_df = \
                    tools.read_prev_run_logfile(filepath=self.prev_run_log_filepath,
                                                dateparser_format='%Y-%m-%d')
            except:
                self.logger.log_info('(!) Error parsing previous runs log file, define additional dateparser_format.')
        else:
            self.prev_run_log_df = pd.DataFrame()
            self.prev_run_log_df.to_csv(self.prev_run_log_filepath)

        return None

    def data_files_per_date(self, unique_dates, section_id):
        # show which data files we have for which date
        self.logger.log_info('{s}\n{s} DATA FILES PER DATE'.format(s=section_id))
        for unique_date in unique_dates:
            same_date_input_files_df = self.input_files_df.loc[self.input_files_df['ETH_filedate'] == unique_date, :]
            filenames = same_date_input_files_df['ETH_filename'].values
            self.logger.log_info('{}     {}'.format(section_id, unique_date))
            for file in filenames:
                self.logger.log_info('{}          {}'.format(section_id, file))
        self.logger.log_info('{}'.format(section_id))
        return None

    def file_loop(self):

        loop_tic, section_id = tools.section_start(logger=self.logger, section_name='[file_loop]')

        # sometimes there are multiple files for a day, resulting in the same filedate for multiple files
        # here we check if there is more than one filename for a specific date
        # this generates a list w/ unique dates that can then be used in the loop
        unique_dates = self.input_files_df['ETH_filedate'].unique()

        # -------------------------------------------------------------------
        # DATA FILES PER DATE
        # check which data files we have for which date
        self.data_files_per_date(unique_dates=unique_dates, section_id=section_id)

        for unique_date in unique_dates:

            tic = time.time()  # runtime timer
            data_df = pd.DataFrame()  # empty dataframe that will contain all data from this date

            self.logger.log_info('{s}\n{s}\n{s}{sp}\n{s}     {d}\n{s}{sp}\n{s} reading data for date {d}'.format(
                sp='-' * 80, s=section_id, d=unique_date))
            self.logger.log_info('{s}'.format(s=section_id))

            # make subgroup that contains all files with the same filedate
            same_date_input_files_df = self.input_files_df.loc[self.input_files_df['ETH_filedate'] == unique_date, :]

            # In case there are multiple files for one day AND no renaming is done in post-processing,
            # the name of the first file for the day is used as template.
            use_this_eth_filename = same_date_input_files_df['ETH_filename'][0]

            # show data files for this date
            self.logger.log_info('{}     found files:'.format(section_id))
            filenames = same_date_input_files_df['ETH_filename'].values
            for file in filenames:
                self.logger.log_info('{}          {}'.format(section_id, file))
            self.logger.log_info('{}     number of found files: {}'.format(section_id, len(same_date_input_files_df)))

            # -------------------------------------------------------------------
            # READ DATA
            # now cycle through all files in the subgroup, collect all data for this date in one DF
            file_counter = 0
            for ix, row in same_date_input_files_df.iterrows():
                # get the datetime info contained in the original ETH filename
                # filedatetime is the index, i.e. the row name in this case
                # eth_filedatetime = row.name

                # read data from file to DataFrame
                # data from the first file in the subgroup are merged with the still empty data_df
                # data from the second file onwards are merged with the already filled data_df
                new_data_df = self.read_data(row=row, section_id=section_id)
                data_df = pd.concat([data_df, new_data_df])  # add to data from this day
                file_counter += 1

            # -------------------------------------------------------------------
            # MERGE CHECK
            # now all available data from this day are collected, check if multiple files were merged
            merged = True if file_counter > 1 else False
            if merged:
                self.logger.log_info('{s}\n'
                                     '{s}   >>>>>> merged {fc} files for date {ud}\n'
                                     '{s}   >>>>>> merged data rows: {r}\n'
                                     '{s}   >>>>>> merged data cols: {c}\n'
                                     '{s}   >>>>>> merged data size: {sz}\n'
                                     '{s}'.format(
                    s=section_id, fc=file_counter, ud=unique_date,
                    r=data_df.shape[0], c=data_df.shape[1], sz=data_df.size))
            else:
                self.logger.log_info(
                    '{}      >>> no merging needed for data from data {}, only {} file'.format(section_id, unique_date,
                                                                                               file_counter))

            # -------------------------------------------------------------------
            # DESTINATION PATH
            # destination dir for ICOS output file from unique date in filename
            outpath = tools.get_subdir_from_date(date=unique_date,
                                                 outpath=self.f_settings['icos_file_outpath'])

            # -------------------------------------------------------------------
            # LIMIT DATA
            # (optional) limit data to most recent day; needs datetime index
            # if set, overrides filedate
            data_df, icos_filedate = \
                self.limit_data_to_most_recent_day(data_df=data_df,
                                                   orig_filedate=unique_date,
                                                   section_id=section_id)

            print(data_df)

            # -------------------------------------------------------------------
            # INSERT ICOS TIMESTAMP
            data_df = self.insert_icos_timestamp(data_df=data_df,
                                                 keep_non_icos_timestamp=self.f_settings['d_timestamp_keep_non_icos'],
                                                 section_id=section_id)

            # -------------------------------------------------------------------
            # (optional) RENAME COLUMNS
            data_df = self.rename_columns(data_df=data_df,
                                          section_id=section_id)

            # -------------------------------------------------------------------
            # (optional) KEEP ONLY RENAMED COLUMNS
            data_df = self.keep_only_renamed_columns(data_df=data_df,
                                                     section_id=section_id)

            # -------------------------------------------------------------------
            # (optional) REMOVE SUFFIX FROM VARIABLE NAMES
            data_df = self.delete_suffix_from_variable_names(data_df=data_df,
                                                             section_id=section_id)

            # -------------------------------------------------------------------
            # SAVE UNCOMPRESSED ICOS FILE
            icos_uncompressed_outfilepath = self.save_uncompressed_icos_file(data_df=data_df,
                                                                             icos_filedate=icos_filedate,
                                                                             eth_filename=use_this_eth_filename,
                                                                             outpath=outpath,
                                                                             section_id=section_id)

            # -------------------------------------------------------------------
            # (optional) SAVE COMPRESSED (ZIP) ICOS FILE
            # save compressed (ZIP) ICOS file
            self.save_zipped_icos_file(filepath_to_compress=icos_uncompressed_outfilepath,
                                       outpath_zip=outpath,
                                       section_id=section_id)

            # -------------------------------------------------------------------
            # (optional) DELETE UNCOMPRESSED FILE
            self.delete_uncompressed_icos_file(outfilepath_uncompressed=icos_uncompressed_outfilepath,
                                               section_id=section_id)

            # -------------------------------------------------------------------
            # FINISH LOOP FOR THIS FILE
            f_runtime = time.time() - tic
            self.logger.log_info('{}     time needed for data from date {}: {:.4f}s'.format(
                section_id, unique_date, f_runtime))

        tools.section_end(logger=self.logger, section_name=section_id, tic=loop_tic)

        # else:
        #     self.logger.log_info('limit_data_to_most_recent_day not registered for this f_type. Stopping script.')
        #     sys.exit(-1)

    def keep_only_renamed_columns(self, data_df, section_id):

        if self.f_settings['d_rename_columns']:
            #  make list of kept columns
            keep_cols = []
            for key, val in self.f_settings['d_rename_columns'].items():
                keep_cols.append(val)

            if self.f_settings['d_keep_only_renamed_columns']:
                # now keep only renamed cols
                data_df = data_df[[c for c in data_df.columns if c in keep_cols]]

            self.logger.log_info("{}          * keeping only renamed columns: {}".format(section_id, keep_cols))

        else:
            self.logger.log_info("{}          * keeping ALL columns".format(section_id))

        return data_df

    def limit_data_to_most_recent_day(self, data_df, orig_filedate, section_id):
        """ Limits data in the ICOS file to data from only the most recent day.
            Necessary for these data files that contain data from more than only the most recent day:
                * 13_meteo_meteoswiss (contains last 3 days)
                * 13_meteo_nabel (contains last 4 days)
            For ICOS, we only send the newest data from the most recent day.
        """

        if self.f_settings['d_only_most_recent_day']:
            # note that the timestamp refers to the end of the measurements period
            # create new column that only gives the day of measurement
            # for this we need to subtract x minutes from the timestamp
            # e.g. for 2018-09-06 00:00 we subtract 60 min to get 2018-09-05 23:00 (date therefore 2018-09-05)
            data_df['measurement_day'] = data_df.index - dt.timedelta(minutes=1)
            data_df['measurement_day'] = data_df['measurement_day'].dt.date

            if self.f_settings['f_type'] == '13_meteo_meteoswiss':
                # get last day of file (=newest day)
                measurement_day = data_df['measurement_day'][-1]

            elif self.f_settings['f_type'] == '13_meteo_nabel':
                # last entry cannot be used b/c files have 1 extra line at the end
                measurement_day = data_df['measurement_day'][-2]

            else:
                self.logger.log_info('limit_data_to_most_recent_day not registered for this f_type. Stopping script.')
                sys.exit(-1)

            # only use data for this last day
            # select all rows where measurement day is the most recent day, select all columns
            data_df = data_df.loc[data_df['measurement_day'] == measurement_day, :]

            icos_filedate = measurement_day
            self.logger.log_info("{}          * limiting data to most recent day".format(section_id))
            self.logger.log_info(
                "{}            (ICOS filedate {} is different than ETH filedate {})".format(section_id, icos_filedate,
                                                                                            orig_filedate))

        else:
            icos_filedate = orig_filedate
            self.logger.log_info("{}          * NOT limiting data to most recent day".format(section_id))
            self.logger.log_info(
                "{}            (ICOS filedate {} is the same as ETH filedate {})".format(section_id, icos_filedate,
                                                                                         orig_filedate))

        return data_df, icos_filedate

    def rename_columns(self, data_df, section_id):
        if self.f_settings['d_rename_columns']:
            for old, new in self.f_settings['d_rename_columns'].items():
                data_df.rename(index=str, columns={old: new}, inplace=True)
            self.logger.log_info("{}          * renamed columns: {}".format(
                section_id, self.f_settings['d_rename_columns']))
        else:
            self.logger.log_info("{}          * no columns were renamed".format(section_id))

        return data_df

    def delete_suffix_from_variable_names(self, data_df, section_id):
        if self.f_settings['d_header_remove_suffix_from_variable_names']:
            # data_df.columns = data_df.columns.get_level_values(0).str.replace('_Avg', '')

            for sfx in self.f_settings['d_header_remove_suffix_from_variable_names']:
                # varnames_row = self.f_settings['d_header_row_with_variable_names']
                # the first row contains variable names
                data_df.columns = pd.MultiIndex.from_tuples([(x[0].replace(sfx, ''), x[1]) for x in data_df.columns])

            data_df.index.name = ('TIMESTAMP', 'TS')

            self.logger.log_info(
                "{}          * removed the following suffices from the variable names: {}".format(
                    section_id, self.f_settings['d_header_remove_suffix_from_variable_names']))

        else:
            self.logger.log_info("{}          * no suffices were removed from variable names".format(section_id))

        return data_df

    def read_data(self, row, section_id):

        # log file
        self.logger.log_info('{s}\n{s}\n{s}{sp}'.format(s=section_id, sp='-' * 80))
        self.logger.log_info('{}     now reading file {}'.format(section_id, row['ETH_filename']))
        self.logger.log_info('{}          filepath: {}'.format(section_id, row['ETH_filepath']))
        self.logger.log_info('{}          filedate: {}'.format(section_id, row['ETH_filedate']))

        # define a function for parsing dates, including exception handling
        # dateparse = lambda date: tools.dateparser(date, date_format=self.f_settings['d_timestamp_format'],
        #                                           logger=self.logger, filepath=Path(row['ETH_filepath']),
        #                                           section_id=section_id)
        # dateparse = lambda date: pd.datetime.strptime(date, self.f_settings['d_timestamp_format'])

        # kudos: https://stackoverflow.com/a/46545843
        dateparse = lambda date: pd.to_datetime(date, format=self.f_settings['d_timestamp_format'], errors='coerce')

        # read data to df
        data_df = pd.read_csv(Path(row['ETH_filepath']),
                              parse_dates=True,
                              date_parser=dateparse,
                              index_col=self.f_settings['d_timestamp_col'],
                              header=self.f_settings['d_header_rows'],
                              skiprows=self.f_settings['d_skip_rows'],
                              encoding='utf-8',
                              sep=self.f_settings['d_separator'],
                              error_bad_lines=False,
                              na_values='NAN')

        # Convert to numeric where possible
        for col in data_df.columns:
            try:
                data_df[col] = data_df[col].astype(np.float64)
            except ValueError as e:
                self.logger.log_info('(!)WARNING column {} could not be converted to numeric: {}'.format(col, e))
                pass


        # log file
        self.logger.log_info('{}          data rows: {}'.format(section_id, data_df.shape[0]))
        self.logger.log_info('{}          data columns: {}'.format(section_id, data_df.shape[1]))
        self.logger.log_info('{}          data size: {}'.format(section_id, data_df.size))
        self.logger.log_info('{}          data types:\n {}'.format(section_id, data_df.dtypes))

        # # NOT DONE FOR ICOS FILES: fill date range, no date gaps, needs freq
        # first_date = data_df.index[0]
        # last_date = data_df.index[-1]

        return data_df

    def delete_uncompressed_icos_file(self, outfilepath_uncompressed, section_id):
        # (optional) delete uncompressed file if needed
        if self.f_settings['f_delete_uncompressed']:
            os.remove(outfilepath_uncompressed)
            self.logger.log_info(
                "{}          * deleted uncompressed ICOS file: {}".format(section_id, outfilepath_uncompressed))
        else:
            self.logger.log_info(
                "{}          * uncompressed ICOS file {} was not deleted".format(section_id, outfilepath_uncompressed))

        return None

    def save_zipped_icos_file(self, filepath_to_compress, outpath_zip, section_id):
        """ Saves compressed (zipped) ICOS file and calculated hash value """
        if self.f_settings['f_compression']:
            filename_no_ext = tools.get_filename_without_ext_from_filepath(filepath=filepath_to_compress)
            outfilepath_zip = outpath_zip / "{}.zip".format(filename_no_ext)

            zipped_file = zf.ZipFile(outfilepath_zip, 'w')
            # note the arcname argument: it enables to directly zip the file w/o including the
            # file-containing folder in the zip file
            zipped_file.write(filepath_to_compress, compress_type=zf.ZIP_DEFLATED,
                              arcname=os.path.basename(filepath_to_compress))
            zipped_file.close()
            self.logger.log_info("{}          * saved compressed ICOS ZIP file: {}".format(section_id, outfilepath_zip))

            # f_hash = self.hash_value_for_file(file_full_path=outfilepath_zip)
            # self.file_list_df.loc[orig_filedate, 'saved_icos_zip_file_hash'] = '{0}'.format(f_hash)
        else:
            outfilepath_zip = 'None'
            self.logger.log_info("{}          * no compressed ZIP file was created".format(section_id))

        return None

    def save_uncompressed_icos_file(self, data_df, icos_filedate, eth_filename, outpath, section_id):
        """ Saves uncompressed data as .csv
            - file is saved w/ ICOS filename
            - csv is generated using quote arguments to output double quotes for header, index and NaNs
        """

        if self.f_settings['fname_generate_icos_filename']:
            # define ICOS filename for output, month and day are output in 2-digit format w/ leading zeros if needed
            outfilename = \
                self.f_settings['fname_generate_icos_filename'].format(
                    year=icos_filedate.year, month=icos_filedate.month, day=icos_filedate.day,
                    logger=self.f_settings['f_LN'], file=self.f_settings['f_FN'])
        else:
            # in this case, we use the original filename, but w/ the .csv file extension
            outfilename = tools.get_filename_without_ext_from_filepath(eth_filename)
            outfilename = '{}.csv'.format(outfilename)

        # full path to uncompressed output file
        icos_uncompressed_outfilepath = outpath / outfilename

        # insert row index (timestamp) as regular column (i.e. not pandas index)
        if len(self.f_settings['d_header_rows']) > 1:
            data_df.insert(0, ('TIMESTAMP', 'TS'), data_df.index)
        else:
            data_df.insert(0, 'TIMESTAMP', data_df.index)

        # check if we need to output the header column names to the file
        header = True if self.f_settings['d_header_output_to_file'] else False

        # save DataFrame to file
        # - note the quote arguments: these make sure that the header, row indices and also NANs
        #   are output with double quotes. also, index=False b/c timestamp was inserted as regular column
        # - na_rep is used to fix representation for NaNs in output csv file
        #   for the ICOS files, NaN is used, in combination with the quote args NaN is output as "NaN"
        #   we need it like this b/c we defined missing values for the files as "NaN"
        data_df.to_csv(icos_uncompressed_outfilepath,
                       quotechar='"',
                       quoting=csv.QUOTE_NONNUMERIC,
                       index=False,
                       header=header,
                       na_rep='NaN',
                       line_terminator='\n')

        self.logger.log_info("{}          * saved uncompressed ICOS file: {}".format(
            section_id, icos_uncompressed_outfilepath))

        return icos_uncompressed_outfilepath

    def insert_icos_timestamp(self, data_df, keep_non_icos_timestamp, section_id):
        """
            Inserts new ICOS-formatted timestamp column in first column as *STRING*

            This is done for ALL files to guarantee uniformity, even for those files
            in which the timestamp is already ICOS conform.
            The old timestamp column is kept in the file, in the last column.

        """
        if keep_non_icos_timestamp:
            data_df['_TIMESTAMP_OLD'] = data_df.index
            self.logger.log_info("{}          * keeping non-ICOS timestamp".format(section_id))
        else:
            self.logger.log_info("{}          * NOT keeping non-ICOS timestamp".format(section_id))

        # format timestamp to ICOS format
        data_df.index = data_df.index.strftime(self.f_settings['d_icos_timestamp_format'])
        self.logger.log_info("{}          * inserted ICOS timestamp".format(section_id))

        return data_df

    def generate_file_list(self):
        # start section
        tic, section = tools.section_start(logger=self.logger,
                                           section_name='[generate_file_list]')

        # check in which subfolders we can start the search for new files
        search_dirs, search_start_date = \
            tools.file_list_search_folders(logger=self.logger,
                                           section_name=section,
                                           source_dir=self.f_settings['f_source_dir'],
                                           max_age_days=self.max_age_days)

        # empty DataFrame for results
        found_files_df = pd.DataFrame()

        for ix, search_dir in enumerate(search_dirs):
            self.logger.log_info("{} DIR {}: Searching for new files in {}".format(section, ix, search_dir))

            # ----------------------------------
            # check folder read permission
            try:
                # if search dir exists, try to access it
                if os.path.isdir(search_dir):
                    os.listdir(search_dir)
            except PermissionError as err:
                self.logger.log_info(
                    "{} (!) SKIPPING SEARCH FOLDER {} - no read permission on server ({})".format(
                        section, search_dir, err))
                continue

            # start search in search_dir, walk through subdirs (if there are any)
            # os.walk: in case search_dir does not exist, search_dif is skipped
            for root, dirs, files in os.walk(search_dir):
                # current_walk_directory = root.split('\\')
                # current_walk_directory = current_walk_directory[-1]

                for file in files:

                    # ----------------------------------
                    # check if pattern matches
                    if not fnmatch.fnmatch(file, self.f_settings['fname_id']):
                        # if pattern does not match, skip file
                        self.logger.log_info(
                            "{} (!) SKIPPING FILE {} - not matching pattern ({})".format(
                                section, file, self.f_settings['fname_id']))
                        continue

                    # get file date
                    f_datetime = tools.get_datetime_from_filename(fname=file, f_settings=self.f_settings)

                    # ----------------------------------
                    # check if file is from last 7 days
                    if f_datetime < search_start_date:
                        self.logger.log_info(
                            "{} (!) SKIPPING FILE {} - older than search start date ({})".format(
                                section, file, search_start_date))
                        continue

                    # ----------------------------------
                    # TODO DONT FORGET TO REACTIVATE AFTER TESTING
                    # check if date with this filedate is already in overview list
                    if f_datetime in self.prev_run_log_df.index:
                        self.logger.log_info(
                            "{} (!) SKIPPING FILE {} - file date already in post-processing list".format(
                                section, file))
                        continue

                    # full path to found file
                    filepath = Path(root) / file
                    # filepath = search_dir / file

                    # ----------------------------------
                    # check file read permission
                    try:
                        with open(filepath) as f:
                            s = f.read(5)  # read 5 bytes to check read access
                            # print('read', len(s), 'bytes.')
                    except PermissionError as err:
                        self.logger.log_info(
                            "{} (!) SKIPPING FILE {} - no read permission on server ({})".format(
                                section, file, err))
                        continue

                    # ----------------------------------
                    # if all checks passed, add file to df
                    # the index is the date contained in the filename
                    found_files_df.loc[f_datetime, 'run_id'] = self.run_id
                    found_files_df.loc[f_datetime, 'run_datetime'] = self.run_start_dt
                    found_files_df.loc[f_datetime, 'ETH_filename'] = file
                    found_files_df.loc[f_datetime, 'ETH_filepath'] = filepath
                    found_files_df.loc[f_datetime, 'ETH_filedate'] = f_datetime.date()

                    self.logger.log_info("{} + ADDING FILE {}".format(section, file))

        tools.section_end(logger=self.logger, section_name=section, tic=tic)

        return found_files_df


if __name__ == '__main__':
    """
        For testing purposes.
    """

    max_age_days = 10

    tic = time.time()

    # # todo for testing executed in parallel
    # p1 = format_to_icos(file_format=ff.f_10_meteo(), max_age_days=max_age_days)
    # p2 = format_to_icos(file_format=ff.f_10_meteo_heatflag_sonic(), max_age_days=max_age_days)
    # p3 = format_to_icos(file_format=ff.f_12_meteo_forest_floor(table=1, forest_floor=1), max_age_days=max_age_days)
    # p4 = format_to_icos(file_format=ff.f_12_meteo_forest_floor(table=1, forest_floor=2), max_age_days=max_age_days)
    # p5 = format_to_icos(file_format=ff.f_12_meteo_forest_floor(table=1, forest_floor=3), max_age_days=max_age_days)
    # p6 = format_to_icos(file_format=ff.f_12_meteo_forest_floor(table=1, forest_floor=4), max_age_days=max_age_days)
    # p7 = format_to_icos(file_format=ff.f_12_meteo_forest_floor(table=1, forest_floor=5), max_age_days=max_age_days)
    # p8 = format_to_icos(file_format=ff.f_13_meteo_backup_eth(), max_age_days=max_age_days)
    # p9 = format_to_icos(file_format=ff.f_13_meteo_meteoswiss(), max_age_days=max_age_days)
    # p10 = format_to_icos(file_format=ff.f_13_meteo_nabel(), max_age_days=max_age_days)
    # p11 = format_to_icos(file_format=ff.f_17_meteo_profile(), max_age_days=max_age_days)
    # p12 = format_to_icos(file_format=ff.f_30_profile_ghg(), max_age_days=max_age_days)

    # executed in sequence
    format_to_icos(file_format=ff.f_10_meteo(), max_age_days=max_age_days)
    format_to_icos(file_format=ff.f_10_meteo_heatflag_sonic(), max_age_days=max_age_days)
    format_to_icos(file_format=ff.f_12_meteo_forest_floor(table=1, forest_floor=1), max_age_days=max_age_days)
    format_to_icos(file_format=ff.f_12_meteo_forest_floor(table=1, forest_floor=2), max_age_days=max_age_days)
    format_to_icos(file_format=ff.f_12_meteo_forest_floor(table=1, forest_floor=3), max_age_days=max_age_days)
    format_to_icos(file_format=ff.f_12_meteo_forest_floor(table=1, forest_floor=4), max_age_days=max_age_days)
    format_to_icos(file_format=ff.f_12_meteo_forest_floor(table=1, forest_floor=5), max_age_days=max_age_days)
    format_to_icos(file_format=ff.f_13_meteo_backup_eth(), max_age_days=max_age_days)
    format_to_icos(file_format=ff.f_13_meteo_meteoswiss(), max_age_days=max_age_days)
    format_to_icos(file_format=ff.f_13_meteo_nabel(), max_age_days=max_age_days)
    format_to_icos(file_format=ff.f_17_meteo_profile(), max_age_days=max_age_days)
    format_to_icos(file_format=ff.f_30_profile_ghg(), max_age_days=max_age_days)

    toc = time.time() - tic
    print('\n\n\nTIME NEEDED: {}s'.format(toc))

    sys.exit()
