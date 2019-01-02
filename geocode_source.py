"""
Takes in an export of the roast log, and from the coffee growing locations saves the geooded data, including a URL to
find the place on a map.
"""
import googlemaps
import pickle
import logging
import datetime
import json
# import gspread
import pygsheets
import pandas as pd
# import google.auth
# from google.oauth2 import service_account
from pathlib import Path


# general config items
geocache_file = Path('geocache.pkl')

# set up logging
log_format = '%(asctime)s:%(levelname)s:%(funcName)s:%(lineno)d: %(message)s'
logging.basicConfig(format=log_format, datefmt='%m/%d/%Y %H:%M:%S')
logger = logging.getLogger('geocoder')
logger.setLevel(20)

# configure google API access
keyfile = open('keys.json', 'r')
api_keys = json.load(keyfile)
google_maps_api_key = api_keys['google_map']
gmap = googlemaps.Client(key=google_maps_api_key)

# configure google docs access
# credentials = service_account.Credentials.from_service_account_file('./hero.json')
scope = ['https://spreadsheets.google.com/feeds']
client = pygsheets.authorize(service_file='./hero.json')
coffee_doc_id = '1-JCMcy_NHR8CGw9eDCllpZ1gnbSzinH4riPSWqxC_mw'
coffee_doc = client.open_by_key(coffee_doc_id)
ws = coffee_doc.worksheet_by_title('cupping dimensions')
wsdata = ws.get_as_df()
beans = wsdata[wsdata['Coffee bean'] != '']['Coffee bean']
coffee_sources = beans.tolist()
# coffee_sources = [
#     'Ethiopia Agaro Kedamai Cooperative',
#     'Costa Rica Chirripo La Fila'
# ]

if __name__ == '__main__':
    start = datetime.datetime.now()
    last_checkpoint = datetime.datetime.now()

    # load the already geocoded item cache:
    if not geocache_file.exists():
        logger.warning(f'GeoCache file not found: {geocache_file}')
        geocache = dict()
    else:
        infile = open(geocache_file, 'rb')
        geocache = pickle.load(infile)
        infile.close()

    num_geocoded = 0
    for source in coffee_sources:
        if source not in geocache:
            logger.info(f'Cache did not contain {source}, so fetching it from the API.')
            num_geocoded += 1
            places = gmap.geocode(source)
            if len(places) == 0:
                logger.warning(f'Found ZERO matches for {source}, so skipping it.')
                continue
            elif len(places) > 1:
                logger.warning(f'Found {len(places)} different matches for {source}, proceeding with the first match.')
            place = places[0]
            coded = dict()
            try:
                coded['source'] = source
                place_id = place['place_id']
                coded['place_id'] = place_id
                coded['place_url'] = f'https://www.google.com/maps/place/?q=place_id:{place_id}'
                logger.debug(f'Found {place_id}')
                coded['raw'] = place
                coded['updated'] = datetime.datetime.now()
                coded['country'] = ''
                coded['country_code'] = ''
                coded['state'] = ''
                coded['region'] = ''
                coded['place'] = ''

                country = [item['long_name'] for item in place['address_components'] if 'country' in item['types']]
                if len(country) > 0:
                    coded['country'] = country[0]
                country_code = [item['short_name'] for item in place['address_components'] if 'country' in item['types']]
                if len(country_code) > 0:
                    coded['country_code'] = country_code[0]
                state = [item['long_name'] for item in place['address_components'] if 'administrative_area_level_1' in item['types']]
                if len(state) > 0:
                    coded['state'] = state[0]
                region = [item['long_name'] for item in place['address_components'] if 'administrative_area_level_2' in item['types']]
                if len(region) > 0:
                    coded['region'] = region[0]
                if 'locality' in place['address_components'][0]:
                    coded['place'] = place['address_components'][0]['long_name']
                else:
                    coded['place'] = place['formatted_address']
            except Exception as e:
                logger.warning(f'Couldn\'t find the plus code in {source} - error was {e}, got as far as {coded} items')

            geocache[source] = coded
        else:
            logger.info(f'Found {source} in cache, so using the cached version.')

    # update the cache
    outfile = open(geocache_file, 'wb')
    pickle.dump(geocache, outfile)
    outfile.close()

    logger.info(f'Geocoded {num_geocoded} items in {(datetime.datetime.now() - last_checkpoint).total_seconds():.2f}s')
    last_checkpoint = datetime.datetime.now()

    # assemble the data as a df and save it to the google sheet in a new tab
    tab_rows = ['source', 'place_url', 'country', 'country_code', 'state', 'region', 'place']
    df = pd.DataFrame(columns=tab_rows)
    for key, item in geocache.items():
        df = df.append({'source': item['source'], 'place_url': item['place_url'], 'country': item['country'],
                        'country_code': item['country_code'], 'state': item['state'], 'region': item['region'],
                        'place': item['place']}, ignore_index=True)

    # save the data into a new tab in the sheet
    outws = coffee_doc.worksheet_by_title('geo results')
    outws.set_dataframe(df, 'A1', fit=True)

    logger.info(f'Saved {len(df)} items to Google Sheets in {(datetime.datetime.now() - last_checkpoint).total_seconds():.2f}s')
    logger.info(f'For a total runtime of {(datetime.datetime.now() - start).total_seconds():.2f}s')
