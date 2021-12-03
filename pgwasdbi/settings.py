import json
import logging
import os
from importlib import metadata
from pprint import pformat
import stat

from appdirs import user_config_dir

appname = "pgwasdbi"
appauthor = metadata.metadata(appname)['Author']
        
class Settings:
    
    def __init__(self, fpath = None):

        # Guarantee that the setting file exists and load it
        defaults = {
            "host": "localhost",
            "port": 5432,
            "database": None,
            "user": None,
            "password": None
        }
        
        self.permissions = stat.S_IRUSR | stat.S_IWUSR
        
        self.config_dir = user_config_dir(appname=appname, appauthor=appauthor)
        self.config_fname = "pgwasdbi_config.json"
        
        if fpath is None:
            self.config_fpath = os.path.join(self.config_dir, self.config_fname)
        else:
            self.config_fpath = os.path.realpath(fpath)
        logging.debug(f"{self.config_dir=}")
        logging.debug(f"{self.config_fname=}")
        logging.debug(f"{self.config_fpath=}")
        
        # Load settings otherwise initialize it and its folder structure(s)
        if not os.path.exists(self.config_dir):
            os.makedirs(self.config_dir)
        
        if not os.path.exists(self.config_fpath):
            with open(self.config_fpath, 'w') as ofp:
                json.dump(defaults, ofp, indent=4, sort_keys=True)
                os.chmod(ofp, self.permissions)
        
        with open(self.config_fpath, 'r') as ifp:
            try:
                self.__data = json.load(ifp)
            except Exception as e:
                logging.error(f"An error was encountered while attempting to load settings. Please validate your configuration file: '{self.config_fpath}'.")
                raise e
        
    def __repr__(self):
        return f"Settings('{self.config_fpath}')"

    def set(self, key, value):
        logging.debug(f"Setting '{key}' to '{value}'")
        try:
            # Load and write to find
            self.__data[key] = value
            with open(self.config_fpath, 'w') as ofp:
                json.dump(self.__data, ofp, indent=4, sort_keys=True)
                # Make sure it's private
                os.chmod(self.config_fpath, self.permissions)
        except Exception as e:
            raise e
        
        else:
            logging.info(f"Updated '{key}' to '{value}'.")
        # pass

    def get(self, key):
            return self.__data[key]


