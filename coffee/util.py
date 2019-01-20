"""
Utility functions that help with the coffee analytics
"""
__author__ = "themcclure"

import datetime
import json
import os
import pygsheets
import pandas as pd


def read_tab_as_df(workbook, tab_name, col_types=None, date_cols=None, index_col=None, num_columns=None, raw=False):
    """
    Read the named tab from the given Google Sheets workbook, and return the tab as a DataFrame that has been trimmed
    to remove empty/blank cells.
    :param workbook: Google pygsheets object
    :param tab_name: name of the tab to load
    :param col_types: a dict of dtypes to apply to the DataFrame
    :param date_cols: a list of column names that are dates
    :param index_col: either a single string, or a list of column names that should be the DataFrame index
    :param num_columns: the number of columns to return
    :param raw: if set to True, then return the full tab as is
    :return: a DataFrame
    """
    df = workbook.worksheet_by_title(tab_name).get_as_df()
    if not raw and not df.empty:
        df.replace('', pd.np.nan, inplace=True)
        df.dropna(how='all', inplace=True)
        if num_columns:
            df = df.iloc[:, :num_columns]
        if col_types:
            df = df.astype(col_types)
        if date_cols:
            for col in date_cols:
                df[col] = pd.to_datetime(df[col])
        if index_col:
            df.set_index(index_col, inplace=True)
        # df.fillna('', inplace=True)

    return df


# TODO: paramterize/configize this stuff
def load_all_countries_file(filename='/Users/mcclure/Downloads/allCountries.txt', save_cache=0):
    """
    Loads the geonames.org allCounries.txt data file, processes it and saves it to a local cache file.
    allCountries.txt is TSV and has this format:
            geonameid         : integer id of record in geonames database
            name              : name of geographical point (utf8) varchar(200)
            asciiname         : name of geographical point in plain ascii characters, varchar(200)
            alternatenames    : alternatenames, comma separated varchar(5000)
            latitude          : latitude in decimal degrees (wgs84)
            longitude         : longitude in decimal degrees (wgs84)
            feature class     : see http://www.geonames.org/export/codes.html, char(1)
            feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
            country code      : ISO-3166 2-letter country code, 2 characters
            cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 60 characters
            admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
            admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80)
            admin3 code       : code for third level administrative division, varchar(20)
            admin4 code       : code for fourth level administrative division, varchar(20)
            population        : bigint (8 byte int)
            elevation         : in meters, integer
            dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
            timezone          : the timezone id (see file timeZone.txt) varchar(40)
            modification date : date of last modification in yyyy-MM-dd format
    :param filename: file location of the input
    :param save_cache: if a number, save the DataFrame to a cache file in chunks of save_cache * 1,000
    :return: a DataFrame of all the countries and features
    """
    start = datetime.datetime.now()
    last_checkpoint = datetime.datetime.now()

    column_list = ['geonameid', 'name', 'asciiname', 'alternatenames', 'latitude', 'longitude', 'feature_class', 'feature_code',
                   'country_code', 'cc2', 'admin1_code', 'admin2_code', 'admin3_code', 'admin4_code', 'population', 'elevation',
                   'dem', 'timezone', 'modification_date']
    dtype_list = {'latitude': pd.np.float, 'longitude': pd.np.float, 'population': pd.np.int, 'elevation': pd.np.float,
                  'dem': pd.np.float, 'cc2': pd.np.str, 'admin1_code': pd.np.str, 'admin2_code': pd.np.str,
                  'admin3_code': pd.np.str, 'admin4_code': pd.np.str}
    alldf = pd.read_csv(filename, '\t', names=column_list, index_col=0, dtype=dtype_list)
    # convert date column to date
    alldf['modification_date'] = pd.to_datetime(alldf['modification_date'])
    print(f"Loaded from disk after {(datetime.datetime.now() - last_checkpoint).total_seconds():.2f}s")
    last_checkpoint = datetime.datetime.now()

    if save_cache > 0:
        # alldf.to_hdf('cache.h5', 'countries', format='table')
        cache = pd.HDFStore('cache.h5')
        sizes_list = {'name': 160, 'feature_class': 3}
        index_cols = ['name', 'feature_class', 'feature_code', 'country_code']
        size = len(alldf)
        low = 0
        high = save_cache * 1000
        while high <= size:
            append = True
            if low == 0:
                append = False
            cache.put('countries', alldf.iloc[low:high], 't', append=append, data_columns=index_cols, min_itemsize=sizes_list)
            cache.flush()
            print(f"Saved from {low} to {high} after {(datetime.datetime.now() - last_checkpoint).total_seconds():.2f}s")
            last_checkpoint = datetime.datetime.now()
            low = high
            high += save_cache * 1000

    print(f"Did the whole allCountries stuff in {(datetime.datetime.now() - start).total_seconds():.2f}s")
    return alldf
