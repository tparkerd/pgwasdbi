"""Main module."""

import argparse
import logging
import os
from datetime import datetime as dt
from pprint import pformat

from pgwasdbi.core import experiment, pipeline
from pgwasdbi.util.database import connect
from pgwasdbi.util.validate import validate

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

def main(args):
    try:
        args.conn = connect(args)
    except Exception as err:
        raise err
    else:
        args.fp = os.path.realpath(args.path[0].name)
        args.cwd = os.path.dirname(os.path.abspath(args.fp))
        logging.info(f"Processing '{args.fp}'.")
        if args.validate is not None:
            try:
                validate(args)
            except Exception as err:
                logging.error(f"ARGS: {pformat(args.conf)}")
                raise err
            else:
                if args.dryrun is not True:
                    run(args)
