"""Project module. Generates and modifies project configuration files."""

import json
import logging
import os
import re
import stat
from collections import defaultdict
from importlib import metadata
from pprint import pformat, pprint
from functools import cmp_to_key

import questionary
from appdirs import user_config_dir
from questionary import question

appname = "pgwasdbi"
appauthor = metadata.metadata(appname)['Author']

def orderByChr(a, b):
    pattern = r"chr(?P<id>\d+)_+\S+\.012"
    A = re.match(pattern, os.path.basename(a))
    B = re.match(pattern, os.path.basename(b))
    A = int(A.group('id'))
    B = int(B.group('id'))

    return A - B

def isNaturalNumber(x):
    try:
        x = int(x)
    except:
        return "Number of chromosomes must be a natural number."
    else:
        if x < 1:
            return "Number of chromosomes must be a natural number."
        return True

def isDecimalNumber(x):
    # Required, so empty string is not allowed
    if not x.strip():
        return "A numeric value is required."
    try:
        x = float(x)
    except:
        return "Must be a numeric value; typically a decimal number."
    else:
        return True

class Dataset:

    def __init__(self, fpath = None):

        self.permissions = stat.S_IRUSR | stat.S_IWUSR
        self.fpath = os.path.realpath(fpath)  # realpath to data folder
        self.readme = None  # documentation
        self.alias = None  # short-hand name of data set
        self.slug = None  # filename for dataset configuration file (e.g., WiDiv.json)

        self.__data = defaultdict()
        # Establish default values
        self.__data["species_shortname"] = ""
        self.__data["species_binomial_name"] = ""
        self.__data["species_subspecies"] = ""
        self.__data["species_variety"] = ""
        self.__data["population_name"] = ""
        self.__data["number_of_chromosomes"] = 0
        self.__data["genotype_version_assembly_name"] = ""
        self.__data["genotype_version_annotation_name"] = ""
        self.__data["reference_genome_line_name"] = ""
        self.__data["phenotype_filename"] = []
        self.__data["gwas_algorithm_name"] = ""
        self.__data["imputation_method_name"] = ""
        self.__data["kinship_algortihm_name"] = ""
        self.__data["kinship_filename"] = ""
        self.__data["population_structure_algorithm_name"] = ""
        self.__data["population_structure_filename"] = ""
        self.__data["gwas_run_filename"] = []
        self.__data["gwas_results_filename"] = []
        self.__data["missing_SNP_cutoff_value"] = 0.0
        self.__data["missing_line_cutoff_value"] = 0.0
        self.__data["minor_allele_frequency_cutoff_value"] = 0.0
        self.__data["published"] = False
        self.__data["owner"] = ""

        # Base case: No dataset folder exists or empty folder
        # Create one and initialize file structure
        # 1. README
        # 2. Input
        # 3. Src
        # 4. name.json (goes inside of the input folder)
        if not os.path.exists(self.fpath):
            logging.info(f"Dataset folder does not exist. Initializing file structure.")
            self.readme = "README.txt"

            # Ask user for name of dataset
            self.alias = os.path.basename(self.fpath)
            self.alias = questionary.text(message=f"Dataset Alias [{self.alias}]", default=self.alias).ask()

            # Ask for slug name of dataset
            self.slug = re.sub(r"\s+", "-", self.alias)
            # self.slug = input(f"Slug name: [{self.slug}]: ") or self.slug
            self.slug = questionary.text(message=f"Slug name: [{self.slug}]: ", default=self.slug).ask()

            # Create files
            src_fpath = os.path.join(self.fpath, "src")
            results_fpath = os.path.join(self.fpath, "results")
            readme_fpath = os.path.join(self.fpath, "README.txt")
            input_fpath = os.path.join(self.fpath, "input")
            metadata_fpath = os.path.join(input_fpath, f"{self.slug}.json")

            os.makedirs(src_fpath)
            os.makedirs(input_fpath)
            os.makedirs(results_fpath)

            with open(metadata_fpath, 'w') as ofp:
                json.dump(self.__data, ofp, indent=4, sort_keys=True)

        # Base case: folders exists with data
        else:
            # Get the dataset alias
            self.alias = os.path.basename(self.fpath)

            # If the folder is empty, initialize files structure
            if not os.listdir(self.fpath):
                # TODO: initialize file structure for existing data folder
                logging.info(f"Dataset folder is empty. Initializing file structure.")

            else:
                logging.info(f"Dataset folder contains data. Collecting metadata.")
                data_files = []
                for root, _, files in os.walk(self.fpath):
                    data_files.extend([ os.path.join(root, f) for f in files ])
                logging.debug(f"{data_files=}")

                # If the folder is empty, generate the slug from its name
                self.slug = re.sub(r"\s+", "-", self.alias)
                self.slug = questionary.text(message=f"Dataset filename [{self.slug}]:", default=self.slug, validate=lambda text: True if len(text) > 0 else "Filename is required.").ask()

                # Check to see if the JSON file already exists
                # TODO: check if JSON file already exists
                # ABORT IF IT EXISTS

                # Find chromosome files (e.g., .012) files
                # Use the number of files
                # genotype files
                genotype_files = [ f for f in data_files if f.endswith('.012')]
                genotype_files = sorted(genotype_files, key=cmp_to_key(orderByChr))
                logging.debug(pformat(genotype_files))

                # Species shortname (extracted from .012 files)
                shortname_pattern = r"chr(?P<id>\d+)_+(?P<shortname>[^\.]+)\.012"
                match = re.match(shortname_pattern, os.path.basename(genotype_files[0]))
                shortname = ""
                logging.debug(f"Guessed species shortname: {match=}")
                if match and "shortname" in match.groupdict():
                    shortname = match.group("shortname")
                self.__data["species_shortname"] = questionary.text(message="Species shortname:", default=shortname).ask()

                # Species binomial name
                binomial_name = ""
                self.__data["species_binomial_name"] = questionary.text(message="Species binomial name:", default=binomial_name).ask()

                # Species subspecies
                subspecies_name = ""
                self.__data["species_subspecies"] = questionary.text(message="Species subspecies name:", default=subspecies_name).ask()

                # Species variety
                variety = ""
                self.__data["species_variety"] = questionary.text(message="Species variety name:", default=variety).ask()

                # Chromosome count
                self.__data["number_of_chromosomes"] = questionary.text(message="Number of chromosomes:", default=str(len(genotype_files)), validate=isNaturalNumber).ask()
                self.__data["number_of_chromosomes"] = int(self.__data["number_of_chromosomes"])

                # Population name
                population_name = ""
                self.__data["population_name"] = questionary.text(message="Population name:", default=population_name).ask()

                # Genotype version assembly name
                genotype_version_assembly_name = ""
                self.__data["genotype_version_assembly_name"] = questionary.text(message="Genotype version assembly name:", default=genotype_version_assembly_name).ask()

                # Genotype version annotation name
                genotype_version_annotation_name = ""
                self.__data["genotype_version_annotation_name"] = questionary.text(message="Genotype version annotation name:", default=genotype_version_annotation_name).ask()

                reference_genome_line_name = ""
                self.__data["reference_genome_line_name"] = questionary.text(message="Reference genome line name:", default=reference_genome_line_name).ask()

                gwas_algorithm_name = ""
                self.__data["gwas_algorithm_name"] = questionary.text(message="GWAS algorithm name:", default=gwas_algorithm_name).ask()

                imputation_method_name = ""
                self.__data["imputation_method_name"] = questionary.text(message="Imputation method name:", default=imputation_method_name).ask()

                # Kinship Matrix
                # NOTE(tparker): there's a non-zero chance that there's more than one kinship matrix, so account for that
                kinship_filename = [""]
                kinship_files = [ f for f in data_files if "kinship" in f ]
                logging.debug(f"Guessed kinship files: {kinship_files=}")
                if kinship_files:
                    if len(kinship_files) > 1:
                        kinship_filename = questionary.select(message="Select kinship matrix file:", choices=kinship_files).ask()
                    else:
                        kinship_filename = kinship_files[0]
                else:
                    kinship_filename = kinship_filename[0]  # no kinship matrix was provided
                kinship_filename = os.path.basename(kinship_filename)  # make sure to use basename
                logging.debug(f"{kinship_filename=}")
                self.__data["kinship_filename"] = kinship_filename

                # Kinship Algorithm
                kinship_algortihm_name = ""
                if "astlebalding" in kinship_filename.lower():
                    kinship_algortihm_name = "Astle-Balding"
                elif "raden" in kinship_filename.lower() and "van" in kinship_algortihm_name.lower():
                    kinship_algortihm_name = "VanRaden"
                self.__data["kinship_algortihm_name"] = questionary.text(message="Kinship algorithm name:", default=kinship_algortihm_name).ask()

                # Population Structure
                population_structure_filename = [""]
                population_structure_files = [ f for f in data_files if "population" in f ]
                logging.debug(f"Guessed population structure files: {population_structure_files=}")
                if population_structure_files:
                    if len(population_structure_files) > 1:
                        population_structure_filename = questionary.text(message="Select population structure file:", choices=population_structure_files).ask()
                    else:
                        population_structure_filename = population_structure_files[0]
                else:
                    population_structure_filename = population_structure_filename[0]  # no population structure was provided
                population_structure_filename = os.path.basename(population_structure_filename)  # make sure to use basename

                self.__data["population_structure_filename"] = population_structure_filename

                # Population Structure Algorithm Name
                population_structure_algorithm_name = ""
                if "eigenstrat" in self.__data["population_structure_filename"].lower():
                    population_structure_algorithm_name = "Eigenstrat"
                self.__data["population_structure_algorithm_name"] = questionary.text(message="Population structure algorithm name:", default=population_structure_algorithm_name).ask()

                # Cut-off values
                self.__data["missing_SNP_cutoff_value"] = float(questionary.text(message="missing_SNP_cutoff_value:", default=str(self.__data["missing_SNP_cutoff_value"]), validate=isDecimalNumber).ask())
                self.__data["missing_line_cutoff_value"] = float(questionary.text(message="missing_line_cutoff_value:", default=str(self.__data["missing_line_cutoff_value"]), validate=isDecimalNumber).ask())
                self.__data["minor_allele_frequency_cutoff_value"] = float(questionary.text(message="minor_allele_frequency_cutoff_value:", default=str(self.__data["minor_allele_frequency_cutoff_value"]), validate=isDecimalNumber).ask())

                # Owner
                self.__data["owner"] = questionary.text(message="Owner or Point of Contact:", default="").ask()

                # Published/public
                self.__data["published"] = questionary.confirm(message="Is this dataset published/public?", default=False).ask()

                # Find results files (e.g., gwas*.csv or *results*.csv)
                results_files = [ os.path.basename(f) for f in data_files if "result" in f.lower() and os.path.basename(os.path.dirname(f)) == 'input' ]
                logging.debug(f"Guessed results/runs files: {results_files}")

            # Select result files
            if results_files:
                results_choices = [ questionary.Choice(c, checked=True) for c in results_files]  # select all files by default
                self.__data["gwas_results_filename"] = questionary.checkbox(message="Select result files:", choices=results_choices, initial_choice=None).ask()
                self.__data["gwas_run_filename"] = self.__data["gwas_results_filename"]

            # TODO(tparker): Identify phenotype/measurement files. I think this should be most files that haven't already been selected

        pprint((self.__data))



        # # Attempt to identify data files

        # # Load settings otherwise initialize it and its folder structure(s)
        # if not os.path.exists(self.config_dir):
        #     os.makedirs(self.config_dir)

        # if not os.path.exists(self.config_fpath):
        #     with open(self.config_fpath, 'w') as ofp:
        #         json.dump(defaults, ofp, indent=4, sort_keys=True)
        #         os.chmod(ofp, self.permissions)

        # with open(self.config_fpath, 'r') as ifp:
        #     try:
        #         self.__data = json.load(ifp)
        #     except Exception as e:
        #         logging.error(f"An error was encountered while attempting to load settings. Please validate your configuration file: '{self.config_fpath}'.")
        #         raise e

    def __repr__(self):
        return f"Dataset('{self.fpath}')"

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

    def show(self, key=None):
        """Deprecated"""
        # Show all
        if key is None:
            return self.list()
        else:
            return f"{key}={self.__data[key]}"

    def list(self, key=None):
        return dict(self.__data)

    def __count_chromosomes(self):
        pass
