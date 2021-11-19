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
    print("Parsed args")
    sys.exit(0)

    
    
    parser = argparse.ArgumentParser(
        description=description, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Increase output verbosity"
    )
    parser.add_argument(
        "-V", "--version", action="version", version=f"%(prog)s {__version__}"
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        dest="dryrun",
        action="store_true",
        default=False,
        help="(Dry Run) Preview output via stdout. Will prevent any files from being written.",
    )
    parser.add_argument(
        "--validate",
        action="store",
        nargs="+",
        help="Validate input files. Available options: ['all', 'config', 'line', 'genotype', 'variant', 'kinship', 'population', 'phenotype', 'runs', 'result']",
    )
    default_environment = (
        f".env.qa"  # look in the installation/containing folder for the package
    )
    parser.add_argument(
        "--env",
        nargs="?",
        type=argparse.FileType("r"),
        default=default_environment,
        help="Environment file (Default: .env.qa)",
    )
    parser.add_argument(
        "path",
        metavar="PATH",
        type=argparse.FileType("r"),
        nargs=1,
        help="JSON Configuration file",
    )

    # parser.add_argument("--skip-genotype-validation", action="store_true", default=False, help="Errors in .012 files are infrequent, so enable this option to assume valid input.")
    # parser.add_argument("--reset-qa", dest="reset_qa", action="store_true", help="Empty the QA database")

    args = parser.parse_args()
    args.module_name = "main"
    log.configure(args)

    try:
        environment_ifp = os.path.join(os.getcwd(), args.env.name)
        logging.debug(f"Environment loaded from '{environment_ifp}'")

        if not os.path.exists(environment_ifp):
            raise FileNotFoundError(
                f"Environment file not found. File missing: {environment_ifp}"
            )
    except:
        raise

    validation_options = [
        "config",
        "line",
        "genotype",
        "variant",
        "kinship",
        "population",
        "phenotype",
        "runs",
        "result",
    ]
    # If all validation steps are selected, re-populate the list with all options
    if args.validate is not None:
        if "all" in args.validate:
            args.validate = validation_options

        for v in args.validate:
            if v in validation_options:
                continue
            else:
                logging.warning(f"Unknown validation step specified: '{v}'")
        args.validate = [v for v in args.validate if v in validation_options]

    pgwasdbi.main(args)


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

def main():
    try:
        args = parse_options()
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

if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
