"""Console script for pgwasdbi."""

import argparse
import logging
import os
import sys
from pprint import pformat

from pgwasdbi import log
from pgwasdbi.core import experiment, pipeline
from pgwasdbi.util.database import connect
from pgwasdbi.util.validate import validate

from pgwasdbi import __version__


def parse_options():
    """Function to parse user-provided options from terminal"""
    description = """Importation script specifically for Plant GWAS database."""
    # Superficial pass at runtime arguments
    ## Database connection test
    parser = argparse.ArgumentParser(description=description, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-v", "--verbose", action="store_true", help="Increase output verbosity")
    parser.add_argument("-V", "--version", action="version", version='%(prog)s 1.0.0')
    parser.add_argument("-n", "--dry-run", dest="dryrun", action="store_true", default=False, help="(Dry Run) Preview output via stdout. Will prevent any files from being written.")

    args = parser.parse_args()
    return args

def run(args):
    print("==========================\n\nDo the thing, Zhu Li!\n\n==========================")
    try:
        experiment.design(args)
        pipeline.design(args)
        experiment.collect(args)
        pipeline.collect(args)
    except Exception as err:
        logging.error(f'{type(err).__name__} - {err}'.replace('\n\n', '\t'))
        raise
    else:
        logging.info(f'Importation successful!')

    logging.debug(pformat(args.conf))

def safe_main():
    pass

def main():
    args = parse_options()
    
if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
