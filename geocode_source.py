"""
Takes in an export of the roast log, and from the coffee growing locations saves the geooded data, including a URL to
find the place on a map.
"""
import datetime
import os
import pandas as pd
import coffee
from coffee.config import conf
# import matplotlib as mpl
# mpl.use('TkAgg')
# import seaborn as sns


if __name__ == '__main__':
    start = datetime.datetime.now()

    # setup
    conf.configure_env("Prod", "./data")
    conf.import_keys()
    if os.getenv('REFRESH_CACHE', 0):
        conf.runtime.force_refresh = True

    geocache = coffee.geocoding.process_locations()
    last_checkpoint = datetime.datetime.now()

    # assemble the data as a df and save it to the google sheet in a new tab
    tab_rows = ['source', 'place_url', 'country', 'country_code', 'state', 'region', 'place', 'latitude', 'longitude', 'elevation_m']
    df = pd.DataFrame(columns=tab_rows)
    for key, item in geocache.items():
        df = df.append({'source': item['source'], 'place_url': item['place_url'], 'country': item['country'],
                        'country_code': item['country_code'], 'state': item['state'], 'region': item['region'],
                        'place': item['place'], 'latitude': item['latitude'], 'longitude': item['longitude'],
                        'elevation_m': item['elevation_m']}, ignore_index=True)

    # save the data into a new tab in the sheet
    client = conf.google.get_client()
    coffee_doc = client.open_by_key(conf.runtime.coffee_doc_id)
    outws = coffee_doc.worksheet_by_title('geo results')
    outws.set_dataframe(df, 'A1', fit=True)

    conf.logger.info(f'Saved {len(df)} items to Google Sheets in {(datetime.datetime.now() - last_checkpoint).total_seconds():.2f}s')
    conf.logger.info(f'For a total runtime of {(datetime.datetime.now() - start).total_seconds():.2f}s')
