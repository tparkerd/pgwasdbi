"""Console script for pgwasdbi."""

import argparse
from genericpath import exists
import logging
import sys

# from pgwasdbi.util.validate import validate
from pgwasdbi import __version__, log
from pgwasdbi.settings import Settings
# from pgwasdbi.core import experiment, pipeline
from pgwasdbi.util import database

# import os
# from pprint import pformat




def parse_configuration_options(*args, **kwargs):
    ## Configuration arguments
    description = "Parse configuration settings"
    supported_settings = ["database", "hostname", "password", "port", "username"]
    parser = argparse.ArgumentParser(description=description, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-v", "--verbose", action="store_true", help="Increase output verbosity")
    parser.add_argument("-V", "--version", action="version", version=f'%(prog)s {__version__}')
    parser.add_argument("--set", dest="key_value", metavar=("key", "value"), nargs=2, help=f"Set a key-value pair. E.g., '--set username lknope'. Support keys: {supported_settings}")
    parser.add_argument("config", help=argparse.SUPPRESS)
    opts = parser.parse_args()
    
    key, value = opts.key_value
    
    try:
        settings = Settings()
        settings.set(key, value)
    except:
        raise
    else:
        print(f"'{key}' set to '{value}' successfully.")
    

def parse_options():
    """Function to parse user-provided options from terminal"""
    description = """Importation script specifically for Plant GWAS database."""
    # Base arguments
    
    log.configure(module_name="pgwasdbi")
    
    # Load settings
    settings = Settings()
    
    ## Database connection test
    parser = argparse.ArgumentParser(description=description, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-v", "--verbose", action="store_true", help="Increase output verbosity")
    parser.add_argument("-V", "--version", action="version", version='%(prog)s 1.0.0')
    parser.add_argument("--test-connection", dest="test_connection", action="store_true", help="Test connection to database")
    env = ".env.qa"
    parser.add_argument(
        "--env",
        nargs="?",
        type=argparse.FileType("r"),
        default=env,
        help="Environment file",
    )
    opts = parser.parse_args()
    
    if opts.test_connection:
        database.connection_test()
        print("Do the connection!")
    
    
    
    return opts

# def run(args):
#     print("==========================\n\nDo the thing, Zhu Li!\n\n==========================")
#     try:
#         experiment.design(args)
#         pipeline.design(args)
#         experiment.collect(args)
#         pipeline.collect(args)
#     except Exception as err:
#         logging.error(f'{type(err).__name__} - {err}'.replace('\n\n', '\t'))
#         raise
#     else:
#         logging.info(f'Importation successful!')

#     logging.debug(pformat(args.conf))

def main():
    log.configure('pgwasdbi')
    
    # Settings
    if len(sys.argv) > 1 and sys.argv[1] == "config":
        opts = parse_configuration_options()
    # Normal run
    else:
        opts = parse_options()
    
def configure():
    print("Set configuration!")
    
if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
