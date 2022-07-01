import datetime as dt
import hashlib
import ntpath
import os
import time
from pathlib import Path

import pandas as pd


def file_list_search_folders(logger, section_name, source_dir, max_age_days):
    # define a time range in which we should search for new (not yet processed) files
    # by default, search for new files of the last 7 days
    search_end_date = dt.datetime.now()
    search_end_dir = Path(source_dir,
                          '{:04}'.format(search_end_date.year),
                          '{:02}'.format(search_end_date.month))

    # define yearly and monthly subfolder for search
    search_start_date = search_end_date - dt.timedelta(days=max_age_days)
    search_start_dir = Path(source_dir,
                            '{:04}'.format(search_start_date.year),
                            '{:02}'.format(search_start_date.month))

    # search for files in the two subdirs
    if search_start_dir == search_end_dir:
        search_dirs = [search_start_dir]
    else:
        search_dirs = [search_start_dir, search_end_dir]

    # found search dirs
    logger.log_info("{} Searching for new files in:".format(section_name))
    for ix, search_dir in enumerate(search_dirs):
        logger.log_info("{}      DIR {}: {}".format(section_name, ix, search_dir))

    return search_dirs, search_start_date


def section_start(logger, section_name):
    tic = time.time()
    logger.log_info("\n\n\n{}\n{} SECTION START".format('-' * 80, section_name))
    return tic, section_name


def section_end(logger, section_name, tic):
    section_runtime = time.time() - tic
    logger.log_info('{} SECTION END. Runtime: {:.4f}s'.format(section_name, section_runtime))
    return None


def make_run_id():
    now_time_dt = dt.datetime.now()
    now_time_str = now_time_dt.strftime("%Y%m%d%H%M%S")
    now_time_easyread_str = now_time_dt.strftime("%Y-%m-%d %H:%M:%S")
    run_id = 'PP-ICOS-' + now_time_str
    return run_id, now_time_easyread_str, now_time_dt


def print_settings_dict(settings_dict, logger):
    """ Prints the contents of a settings_dict in a more readable form. """

    logger.log_info("\n\n-----------------------------------------------")
    logger.log_info("FOUND SETTINGS FOR THIS RUN")
    for d in settings_dict:
        logger.log_info("{}: {}".format(d, settings_dict[d]))
    logger.log_info("-----------------------------------------------")
    return None


def get_filename_without_ext_from_filepath(filepath):
    """
    extracts filename w/o extension from filepath
    filepath can also be a filename with extension, e.g. 'furrycat.csv' will return 'furrycat'
    """
    head, tail = ntpath.split(filepath)
    filename = os.path.splitext(tail)[0]
    return filename


def get_subdir_from_date(date, outpath):
    # get yearly and monthly destination folder for the file from filedate
    # if these folders do not exist, they are created
    subdir_year = str(date.year).zfill(4)
    subdir_month = str(date.month).zfill(2)
    subdir = Path("{}/{}".format(subdir_year, subdir_month))
    outpath = outpath / subdir
    check_if_path_exists(path=outpath)

    return outpath


def check_if_path_exists(path):
    if os.path.isdir(path):
        pass
    else:
        os.makedirs(path)

    return None


def hash_value_for_file(file_full_path):
    # https://tutorials.technology/tutorials/51-How-to-calculate-hash-of-big-files-with-python.html

    block_size = 2 ** 20

    with open(file_full_path, 'rb') as input_file:
        sha1 = hashlib.sha1()

        while True:
            # we use the read passing the size of the block to avoid heavy ram usage
            data = input_file.read(block_size)
            if not data:
                break  # if we don't have any more data to read, stop.
            # we partially calculate the hash
            sha1.update(data)

    return sha1.digest()


def get_datetime_from_filename(fname, f_settings):
    # get datetime numbers from fname
    f_year = int(fname[f_settings['fname_year_position'][0]:f_settings['fname_year_position'][1]])

    # in case the year is only given as 2 digits (e.g. 18 instead of 2018),
    # we assume the 21st century is meant; for ICOS, this is only the case for 13_meteo_nabel
    if len(str(f_year)) == 2:
        f_year = int('20{}'.format(f_year))

    f_month = int(fname[f_settings['fname_month_position'][0]:f_settings['fname_month_position'][1]])
    f_day = int(fname[f_settings['fname_day_position'][0]:f_settings['fname_day_position'][1]])

    if f_settings['fname_hour_position']:
        f_hour = int(fname[f_settings['fname_hour_position'][0]:f_settings['fname_hour_position'][1]])
        f_minute = int(
            fname[f_settings['fname_minute_position'][0]:f_settings['fname_minute_position'][1]])
        f_datetime = dt.datetime(f_year, f_month, f_day, f_hour, f_minute)
    else:
        f_datetime = dt.datetime(f_year, f_month, f_day)
    return f_datetime


def read_prev_run_logfile(filepath, dateparser_format):
    dateparser = lambda x: pd.datetime.strptime(x, dateparser_format)
    df = pd.read_csv(filepath, parse_dates=True, date_parser=dateparser,
                     index_col=0, header=0, encoding='utf-8')
    return df


def dateparser(date, date_format, logger, filepath, section_id):
    """ Date parser for use in the pandas read_csv method (as lambda date function).
        Checks for errors during parsing.
    """

    try:
        return pd.datetime.strptime(date, date_format)
        # date = pd.datetime.strptime(date, date_format)
        # lambda date: print(date)
        # date = pd.datetime.strptime(date, date_format)

    except TypeError as err:
        logger.log_info("{s} (!)WARNING: ERROR READING DATE IN AT LEAST ONE ROW IN FILE\n"
                        "{s}       ¦  {f}\n"
                        "{s}       ¦  Error message: {e}".format(
            s=section_id, f=filepath, e=err))

