# Changelog

## v4.0.15 | 2023-02-12

- When reading the raw data csv, `inf` values (they come directly from the logger)
  are now considered as missing values `NaN`.

## v4.0.14 | 2023-01-27

### Complement data with data from preceeding files

- In `file_formats.py`, added settings for `f_11_meteo_hut_prec`. These files contain
  precipitation data in 1MIN resolution but have the issue that they do not exactly
  start at the desired timestamp, e.g. `2023-01-27 00:00:04` instead of the desired
  timestamp `2023-01-27 00:00:01`. To solve for this issue, new setting options were
  added.
- Added new setting `d_complement_data_with_previous_date` (bool) in `file_formats.py`.
  If `True`, data from the previous date are added to data from the current date. This
  is necessary e.g. for `f_11_meteo_hut_prec` because the daily files of this filetype
  start some seconds after midnight, but they should start exactly at `00:00:01`.
  The first few seconds of data for each day are stored at the end of the previous day's
  data file. This setting allows to fetch this previous data and add it to the current date
  to then generate a daily file that starts at the desired timestamp.
- Accordingly, the method `main.format_to_icos.limit_data_to_most_recent_day` has been
  adjusted: in case data from a previous date were added to the current date, the
  resulting dataframe is restricted to data from the current date.
- In the case of `f_11_meteo_hut_prec`, the following settings in `file_formats.py` are
  important: `'d_only_most_recent_day': True`and `'d_complement_data_with_previous_date': True`.
  The first settings adds data from the previous date to the current date, the second setting
  makes sure that only data from the current date remains in the daily file.

### Other changes

- The line terminator (end of line) is now explicitely given as `line_terminator='\r\n'`
  when the data are saved to a csv. This creates the required `CR``LF` at the end of each
  line in the data file.

## v4.0.13 | 2022-01-05

- In file_formats.py, adjusted settings for new `f_15_meteo_snowheight` format

## v 4.0.12 | 2021-11-01

- In file_formats.py, added settings for `f_15_meteo_snowheight`

## v 4.0.11 | 2021-10-24

- In file_formats.py, for f_12_meteo_forest_floor: For FF1, FF2 and FF3: changed file number to "F04", starting 23 Oct
  2021

## v 4.0.10 | 2020-09-24

- In file_formats.py, for f_13_meteo_backup: Changed logger number to "L23" and file number to "F01".

## v 4.0.9 | 2020-08-14

- In file_formats.py, for f_13_meteo_meteoswiss: Changed file number to "F02".

## v 4.0.8 | 2020-07-28

- In file_formats.py, for f_13_meteo_meteoswiss:
  The file format of the meteoswiss files changed. There is now only one header row in the very first row.

For the start of the file, instead of this format:
MeteoSchweiz / MeteoSuisse / MeteoSvizzera / MeteoSwiss

            stn;time;tre200s0;gre000z0;rre150z0;ure200s0
            DAV;202007080000;6.8;0;0.0;79.6
            DAV;202007080010;6.6;0;0.0;80.4
            DAV;202007080020;6.6;0;0.0;81.2

This format is used:
Station/Location;Date;tre200s0;gre000z0;rre150z0;ure200s0
DAV;202007080000;6.8;0;0.0;79.6
DAV;202007080010;6.6;0;0.0;80.4
DAV;202007080020;6.6;0;0.0;81.2

## v 4.0.7 | 2020-06-24

- In file_formats.py, for f_12_meteo_forest_floor: The file number FN is now assigned separately for each FF.

## v 4.0.6 | 2019-09-06

- Created requirements.txt for (conda) environment
- 13_meteo_backup_eth now contains only SW_IN_4_1_1 (backup meteo): Atmos-41 sensor that
  delivered these data was dismounted, no longer transferred to ETC for ICOS.

## v 4.0.5 | 2019-04-24

- Changed logic from last update:
    - dtype=np.float64 is no longer used b/c it caused problems when column consisted of only strings
    - Instead, columns are now read and *after that* converted to numeric if possible, using try / except clause.
    - in: main.py -->         for col in data_df.columns: try: [ ... ]

## v 4.0.4 | 2019-04-22

    * In some files, some numeric columns were read as dtype 'object', which caused the
      output file to contain strings that should be numbers, e.g. "4.563876". Therefore,
      the data columns are now strictly read as floats. In addition, the nan values that
      are found in the logger files (NAN) are now specifically considered when a csv file
      is read.
      (in main.py: data_df = pd.read_csv(Path(row['ETH_filepath']), ...)
      The following arguments were added when reading csv files:
        na_values='NAN', dtype=np.float64
      Original ICOS error message:
        "   CH-Dav_BM_20190421_L04_F01.csv
            ERROR -> Found '"4.460778"' for TS_IU_1_6_1 in the data file that is
            not a numeric value. Please check all along the file
        "

    * Although the output file looks fine, I specifically added line_terminator='\n' as
      argument when writing uncompressed csv.
        (in def save_uncompressed_icos_file: line_terminator='\n')
        Original ICOS error message:
        "   CH-Dav_SAHEAT_20190421_L02_F02.csv
            ERROR -> End of line in the data file is in a wrong format: it must be CRLF. Please check all the lines
            ERROR -> End of line in the data file is in a wrong format: it must be CRLF. Please check all the lines
        "
        From https://stackoverflow.com/questions/12747722/what-is-the-difference-between-a-line-feed-and-a-carriage-return:
            A line feed means moving one line forward. The code is \n.
            A carriage return means moving the cursor to the beginning of the line. The code is \r.
            Windows editors often still use the combination of both as \r\n in text files. Unix uses mostly only the \n.

## v 4.0.3 | 2019-04-19

* in file_formats.py f_10_meteo_heatflag_sonic:
  Files are no longer compressed to zip. Instead, the unzipped files are sent
  to ICOS:
  'f_compression': False,
  'f_delete_uncompressed': False

## v 4.0.2 | 2019-03-04

* in function save_uncompressed_icos_file:
  na_rep argument is used to fix representation for NaNs in output csv file,
  NaNs are output as "NaN" in the ICOS csv file for the Carbon Portal

## version 4.0.1 | 2019-02-01

* implemented additional check that checks if dir exists before the access check
  in main.py: # if search dir exists, try to access it
  if os.path.isdir(search_dir):
  os.listdir(search_dir)

version: 4 (2018-11-13):    * added 13_meteo_nabel

* added option 'd_keep_only_renamed_columns': True/False, to settings dict
  if TRUE, only renamed columns are written to the ICOS output file
* method limit_data_to_most_recent_day: now defined explicitely for
  13_meteo_meteoswiss and f_13_meteo_nabel
* implemented merging of data files for the same day
* optimized the file_loop
* optimized log output
* changed date_parser in read_csv for data files, now using pd.to_datetime:

                                    dateparse = lambda date: pd.to_datetime(date,
                                    format=self.f_settings['d_timestamp_format'], errors='coerce')

                                    This seems like a much better option because it catches erroneous date rows via
                                    errors='coerce'. Rows with erroneous data are simply removed from the output.
                                    This means, there is a date gap in case of date errors, e.g. during conversions:

                                    INPUT (3 rows)           OUTPUT (2 rows)
                                    09.11.2018 23:12         2018-11-09 23:12
                                    9652x236u8!66+43         2018-11-09 23:14
                                    09.11.2018 23:14

                                    (previously done with pd.datetime.strptime(date, date_format) that was in
                                    a separate function, but it started to produce errors)
                            * TESTING OK:
                                    10_meteo, 10_sonic_heatflag, 12_meteo_forest_floor,
                                    13_meteo_meteoswiss, 13_meteo_backup_eth, 13_meteo_nabel,
                                    17_meteo_profile, 30_ghg_storage

version: 3 (2018-09-30):    * added 13 meteo backup eth

version: 2 (2018-09-22):    * added several checks and file formats

version: 1 (2018-08-18):    init

ready to transfer:

STATUS FILETYPE TRANSFER FILES STARTING FROM
started 10_meteo 2018-08-16 (maybe a bit ealier possible)
started 10_sonic_heatflag 2018-09-05
started 12_meteo_forest_floor 2018-09-20
started 13_meteo_backup_eth 2018-09-29
started 13_meteo_meteoswiss 2018-09-18 (but much earlies is possible)
13_meteo_nabel ???
started 20_sonic_EC (WE)
started 17_meteo_profile 2018-09-20 (earlier would be possible, but there was a gap for some days)
started 30_ghg_storage 2018-09-20 (earlier would be possible, but there was an error for some days)
