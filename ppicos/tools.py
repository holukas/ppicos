import datetime as dt
import hashlib
import ntpath
import os
from pathlib import Path

import pandas as pd


def set_search_window(max_age_days: int = 5) -> tuple[dt.date, dt.date]:
    """Set start and end date for file search"""

    # Define yearly and monthly subfolder for search
    search_firstdate = dt.datetime.now().date()
    search_firstdate = search_firstdate - dt.timedelta(days=max_age_days)

    # Search window always ends with yesterday's date
    search_lastdate = dt.datetime.now().date()
    search_lastdate = search_lastdate - dt.timedelta(days=1)  # Last searched day is always yesterday

    return search_firstdate, search_lastdate


def set_search_folders(source_dir, search_firstdate, search_lastdate):
    """Detect folder names for search window

    kudos: https://stackoverflow.com/questions/37890391/how-to-include-end-date-in-pandas-date-range-method
    """

    # Generate monthly dates between first and last date
    search_firstdate_str = search_firstdate.strftime('%Y-%m')
    search_lastdate_str = search_lastdate.strftime('%Y-%m')
    dates_str = [search_firstdate_str, search_lastdate_str]
    _index = pd.date_range(*(pd.to_datetime(dates_str) + pd.offsets.MonthEnd()), freq='M')

    # Generate paths to search dirs
    searchdirs = []
    for m in _index:
        searchdirs.append(Path(source_dir) / f'{m.year:04}' / f'{m.month:02}')

    return searchdirs, pd.to_datetime(search_firstdate).date()


def make_run_id():
    now_time_dt = dt.datetime.now()
    now_time_str = now_time_dt.strftime("%Y%m%d%H%M%S")
    now_time_easyread_str = now_time_dt.strftime("%Y-%m-%d %H:%M:%S")
    run_id = 'ppicos-' + now_time_str
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


def get_datetime_from_filename(filename: str, filesettings: dict):
    # get datetime numbers from fname
    f_year = int(filename[filesettings['FILENAME_POSITION_YEAR'][0]:filesettings['FILENAME_POSITION_YEAR'][1]])

    # in case the year is only given as 2 digits (e.g. 18 instead of 2018),
    # we assume the 21st century is meant; for ICOS, this is only the case for 13_meteo_nabel
    if len(str(f_year)) == 2:
        f_year = int('20{}'.format(f_year))

    f_month = int(filename[filesettings['FILENAME_POSITION_MONTH'][0]:filesettings['FILENAME_POSITION_MONTH'][1]])
    f_day = int(filename[filesettings['FILENAME_POSITION_DAY'][0]:filesettings['FILENAME_POSITION_DAY'][1]])

    if filesettings['FILENAME_POSITION_HOUR']:
        f_hour = int(filename[filesettings['FILENAME_POSITION_HOUR'][0]:filesettings['FILENAME_POSITION_HOUR'][1]])
        f_minute = int(
            filename[filesettings['FILENAME_POSITION_MINUTE'][0]:filesettings['FILENAME_POSITION_MINUTE'][1]])
        f_datetime = dt.datetime(f_year, f_month, f_day, f_hour, f_minute)
    else:
        f_datetime = dt.datetime(f_year, f_month, f_day)
    return f_datetime
