"""
CONFIG:
All the configuration items for the application
"""
__author__ = "themcclure"

import json
import logging
import pygsheets
from pathlib import Path
# from . import util


class Conf:
    def __init__(self):
        """
        Initialize the config object and attach its runtime version to the module (maybe better to use static class instead?)
        """
        self.google = self.Google()
        self.runtime = self.Runtime()
        self.logging = self.Logging()
        self.logger = self.logging.logger

    ##########
    # SECTION: Google API
    class Google:
        # Maps API Key
        maps_api_key = None

        # Service Account credentials
        cred_file = None

        # Authenticated connection
        client = None

        def get_client(self):
            """
            Returns the active connection to the Google Docs API, refreshing it if needed
            :return: an active
            """
            if not self.client:
                self.client = pygsheets.authorize(service_file=self.cred_file)
            return self.client

    ##########
    # SECTION: Runtime config
    class Runtime:
        coffee_doc_id = '1-JCMcy_NHR8CGw9eDCllpZ1gnbSzinH4riPSWqxC_mw'
        data_dir = None
        geocache_file = None
        geocache = None
        force_refresh = False

    ##########
    # SECTION: Logging
    class Logging:
        # set up logging
        log_format = '%(asctime)s:%(levelname)s:%(funcName)s:%(lineno)d: %(message)s'
        logging.basicConfig(format=log_format, datefmt='%m/%d/%Y %H:%M:%S')
        logger = logging.getLogger('coffee')
        logger.setLevel(20)

    ##########
    # SECTION: Helper functions
    def configure_env(self, env_name, data_dir):
        """
        At runtime, configure the Config object to match the runtime environment and initialize variables
        :param env_name: The name of an environment to configure
        :param data_dir: directory where data files will be looked for by default
        """
        self.logger.debug(f"Configuring runtime as: {env_name}")
        # runtime env specific config goes here
        geocache_fname = 'geocache.pkl'
        data_path = Path(data_dir)

        # final env configuration goes here
        self.runtime.data_dir = data_path
        self.runtime.geocache_file = data_path / geocache_fname

    def import_keys(self, api_keyfile=None, service_account=None):
        """
        Load API keys and Google credential files from the runtime environment
        :param api_keyfile: a JSON file with all the API keys needed stored in it
        :param service_account: a JSON Service Account keyfile
        """
        # load the API keys
        if not api_keyfile:
            api_keyfile = self.runtime.data_dir / 'keys.json'
        api_keyfile = open(api_keyfile, 'r')
        api_keys = json.load(api_keyfile)
        self.google.maps_api_key = api_keys['google_map']
        self.runtime.plotly_api_key = api_keys['plotly']

        # load the service account
        if not service_account:
            service_account = self.runtime.data_dir / 'service-account.json'
        self.google.cred_file = service_account


# runtime config object
conf = Conf()
