"""Console script for pgwasdbi."""

import argparse
import json
import logging
import sys
from pprint import pformat

from pgwasdbi import __version__, log
from pgwasdbi.core import experiment, pipeline
from pgwasdbi.settings import Settings
from pgwasdbi.util import database
from pgwasdbi.util.validate import validate


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
    
    if opts.key_value is not None:
        key, value = opts.key_value
    else:
        logging.error("A key-value pair must be defined.")
        parser.print_help(sys.stderr)
        sys.exit(1)
    
    try:
        settings = Settings()
        settings.set(key, value)
    except:
        raise
    else:
        print(f"'{key}' set to '{value}' successfully.")
    

def test_connection(*args, **kwargs):
    """Function to parse user-provided options from terminal"""
    description = """Importation script specifically for Plant GWAS database."""
    ## Database connection test
    parser = argparse.ArgumentParser(description=description, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-v", "--verbose", action="store_true", help="Increase output verbosity")
    parser.add_argument("-V", "--version", action="version", version=f'%(prog)s {__version__}')
    parser.add_argument("test-connection", help=argparse.SUPPRESS)
    opts = parser.parse_args()
    
    try:
        settings = Settings()
        params = { key: settings.get(key) for key in ('database', 'host', 'password', 'user', 'port') }
        params_preview = dict(params)
        params_preview["password"] = "*******"
        logging.debug(f"{params_preview=}")
        if params["database"] is None or params["password"] is None or params["user"] is None:
            logging.error(f"Make sure to set values for your database, host, and user for your database. Check your configuration file: '{settings.config_fpath}'")
            sys.exit(1)
        database.connect(**params)
    except Exception as e:
        raise e
    else:
        logging.info(f"Connection to '{params['user']}'@'{params['host']}/{params['database']}' established!")
    
    return opts

def parse_options():
    """Function to parse user-provided options from terminal"""
    description = """Importation script specifically for Plant GWAS database."""
    ## Database connection test
    parser = argparse.ArgumentParser(description=description, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-v", "--verbose", action="store_true", help="Increase output verbosity")
    parser.add_argument("-V", "--version", action="version", version=f'%(prog)s {__version__}')
    
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
        metavar="DATA_FORMAT",
        help="Validate input files. Available options: ['all', 'standard', 'config', 'line', 'genotype', 'variant', 'kinship', 'population', 'phenotype', 'runs', 'result']",
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

    opts = parser.parse_args()
    path = opts.path[0]
    
    try:
        settings = Settings()
        params = { key: settings.get(key) for key in ('database', 'host', 'password', 'user', 'port') }
        params_preview = dict(params)
        params_preview["password"] = "*******"
        logging.debug(f"{params_preview=}")
    except:
        raise
    else:
        # Ready to roll!
        logging.info(f"Loaded project configuration file: '{path.name}'")
        try:
            with open(path.name, 'r') as ifp:
                project_conf = json.load(ifp)
        except Exception as e:
            raise e
        else:
            logging.info(pformat(project_conf))

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
        # # If all validation steps are selected, re-populate the list with all options
        # if args.validate is not None:
        #     if "all" in args.validate:
        #         args.validate = validation_options
        #     if "standard" in args.validate:
        #         args.validate = [
        #             "config",
        #             "line",
        #             "variant",
        #             "kinship",
        #             "population",
        #             "phenotype",
        #             "runs",
        #             "result",
        #         ]

        #     for v in args.validate:
        #         if v in validation_options:
        #             continue
        #         else:
        #             logging.warning(f"Unknown validation step specified: '{v}'")
        #     args.validate = [v for v in args.validate if v in validation_options]

        # pgwasdbi.main(opts)
        run(**opts)

def run(*args, **kwargs):
    print("==========================\n\nDo the thing, Zhu Li!\n\n==========================")
    logging.info(f"{args=}")
    logging.info(f"{kwargs=}")
    try:
        pass
        # experiment.design(args)
        # pipeline.design(args)
        # experiment.collect(args)
        # pipeline.collect(args)
    except Exception as err:
        logging.error(f'{type(err).__name__} - {err}'.replace('\n\n', '\t'))
        raise
    else:
        logging.info(f'Importation successful!')

    logging.debug(pformat(args.conf))

def main():
    verbose = False
    if '-v' in sys.argv or '--verbose' in sys.argv:
        verbose = True
    log.configure('pgwasdbi', verbose=verbose)
    # Settings
    if len(sys.argv) > 1:
        if sys.argv[1] == "config":
            opts = parse_configuration_options()
        elif sys.argv[1] == "test-connection":
            opts = test_connection()
        # Normal run
        else:
            opts = parse_options()
    else:
        logging.warning("No action specified. Aborting.")
    
if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
