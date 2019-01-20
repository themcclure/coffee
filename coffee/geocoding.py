"""
Module for doing all the heavy lifting for geocoding the location data from the coffee data
"""
__author__ = "themcclure"

from .config import conf
from . import util
import pickle
import googlemaps
import datetime


def process_locations():
    """
    Takes the Coffee Sheet as inputs and geocodes the location of the growers of each coffee.
    Everything is saved in a cache (pickle) and reused between invocations. If an entry is missing from the cache or if
    the force_refresh flag is set, then the entries are fetched from the Google Maps API.
    The Config object is also updated with the results.
    :return: a dict of the location results
    """
    start = datetime.datetime.now()

    # load the already geocoded item cache:
    geocache_file = conf.runtime.geocache_file
    gmap = googlemaps.Client(key=conf.google.maps_api_key)

    client = conf.google.get_client()
    coffee_doc = client.open_by_key(conf.runtime.coffee_doc_id)
    wsdata = util.read_tab_as_df(coffee_doc, 'cupping dimensions', num_columns=13)
    beans = wsdata[wsdata['Coffee bean'] != ''][['Coffee bean', 'Manual Location']]

    if not geocache_file.exists():
        conf.logger.info(f'GeoCache file not found: {geocache_file}')
        geocache = dict()
    elif conf.runtime.force_refresh:
        conf.logger.warning(f'Forcing GeoCache refresh of: {geocache_file}')
        geocache = dict()
    else:
        infile = open(geocache_file, 'rb')
        geocache = pickle.load(infile)
        infile.close()

    num_geocoded = 0
    for index, sources in beans.iterrows():
        source = sources['Coffee bean']
        best_location = source
        if sources['Manual Location'] != '':
            best_location = sources['Manual Location']
        if source not in geocache:
            conf.logger.info(f'Cache did not contain {source}, so fetching it from the API.')
            num_geocoded += 1
            places = gmap.geocode(best_location)
            if len(places) == 0:
                conf.logger.warning(f'Found ZERO matches for {source}, so skipping it.')
                continue
            elif len(places) > 1:
                conf.logger.warning(f'Found {len(places)} different matches for {source}, proceeding with the first match.')
            place = places[0]
            coded = dict()
            try:
                coded['source'] = source
                place_id = place['place_id']
                coded['place_id'] = place_id
                coded['place_url'] = f'https://www.google.com/maps/place/?q=place_id:{place_id}'
                conf.logger.debug(f'Found {place_id}')
                coded['raw'] = place
                coded['updated'] = datetime.datetime.now()
                coded['country'] = ''
                coded['country_code'] = ''
                coded['state'] = ''
                coded['region'] = ''
                coded['place'] = ''
                coded['latitude'] = ''
                coded['longitude'] = ''
                coded['elevation_m'] = ''

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
                if 'location' in place['geometry']:
                    coded['latitude'] = place['geometry']['location']['lat']
                    coded['longitude'] = place['geometry']['location']['lng']
                    coded['elevation_m'] = f"{gmap.elevation((coded['latitude'], coded['longitude']))[0]['elevation']:.3f}"
            except Exception as e:
                conf.logger.warning(f'Couldn\'t find the plus code in {source} - error was {e}, got as far as {coded} items')

            geocache[source] = coded
        else:
            conf.logger.info(f'Found {source} in cache, so using the cached version.')

    # update the cache
    outfile = open(geocache_file, 'wb')
    pickle.dump(geocache, outfile)
    outfile.close()

    # save the locations in the Config object
    conf.runtime.geocache = geocache

    conf.logger.info(f'Geocoded {num_geocoded} items in {(datetime.datetime.now() - start).total_seconds():.2f}s')
    return geocache
