"""Project module. Generates and modifies project configuration files."""

import json
import logging
import os
import re
import stat
from functools import cmp_to_key
from importlib import metadata
from pathlib import Path
from pprint import pformat, pprint
from textwrap import dedent

import questionary
from appdirs import user_config_dir

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

def isValidFilename(fname):
    """Determine if a file name contains any illegal characters or is a reserved word."""
    if len(fname) < 1:
        return "Field is required."

    illegal_characters = ["/", "\\", "<", ">", ":", "\"", "|", "?", ";", "*"]
    reserved_filenames_windows = ["CON", "PRN", "AUX", "NUL", "COM1", "COM2",
                                  "COM3", "COM4", "COM5", "COM6", "COM7", "COM8"
                                  "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5",
                                  "LPT6", "LPT7", "LPT8", "LPT9"]

    if fname in reserved_filenames_windows:
        return "Provided name is a reserved word in Windows and use is prohibited."

    for chr in illegal_characters:
        if chr in fname:
            return f"'{chr}' is an illegal character."

    return True

def isEmptyDirectory(fpath):
    # If it exists and is a directory...
    if os.path.exists(fpath) and os.path.isdir(fpath):
        # If the directory is completely empty, return True; otherwise, False
        return not os.listdir(fpath)
    return False

class Dataset:
    
    def __preload(self):
        """Load existing metadata from JSON file for dataset (e.g., my-dataset.json)"""
        # Base case: file does not exist, therefore cannot load
        if self.metadata_fpath is not None and not os.path.exists(self.metadata_fpath):
            logging.info(f"Project metadata file does not exists: '{self.metadata_fpath=}'")
            return
        
        logging.info(f"Loading project metadata from: '{self.metadata_fpath}'")
        with open(self.metadata_fpath, 'r') as ifp:
            metadata = json.load(ifp)
        logging.debug(pformat(metadata))
            
        # For each key-value pair in the metadata object, copy the value to its
        # corresponding key in the object's instance internal data
        for key, value in metadata.items():
            logging.debug(f"Loading '{key}': '{value}'")
            self.__data[key] = value
        
    def __init_file_structure(self):
        # Create one and initialize file structure
        # 1. README
        # 2. Input
        # 3. Src
        # 4. name.json (goes inside of the input folder)
        logging.info(f"Initializing file structure.")
        self.readme = "README.txt"

        # Ask user for name of dataset
        self.alias = os.path.basename(self.fpath)
        self.alias = questionary.text(message=f"Dataset Alias [{self.alias}]:", default=self.alias, validate=isValidFilename).ask()

        # Ask for slug name of dataset
        self.slug = re.sub(r"\s+", "-", self.alias).lower()
        # self.slug = input(f"Slug name: [{self.slug}]: ") or self.slug
        self.slug = questionary.text(message=f"Dataset filename: [{self.slug}]:", default=self.slug, validate=isValidFilename).ask()

        # Create files
        self.dname = os.path.dirname(self.fpath)
        self.fpath = os.path.join(self.dname, self.alias)
        src_fpath = os.path.join(self.fpath, "src")
        results_fpath = os.path.join(self.fpath, "results")
        readme_fpath = os.path.join(self.fpath, "README.txt")
        input_fpath = os.path.join(self.fpath, "input")
        self.metadata_fpath = os.path.join(input_fpath, f"{self.slug}.json")

        ## Create folder structure
        os.makedirs(src_fpath)
        os.makedirs(input_fpath)
        os.makedirs(results_fpath)

        ## Create readme
        with open(readme_fpath, 'w') as ofp:
            title = f"# {self.alias}"
            template = f"""
            
            Enter a description of your data set here. Provide any project-specific information needed to
            understand it: context and information relevant to interpreting results.
            
            ## File Structure & Inventory
            
            .
            ├── input
            │   └── {os.path.basename(self.metadata_fpath)}
            ├── README.txt
            └── src
            
            2 directories, 2 files
            
            ./input
                This folder contains all the pre-processed files that are consumed by pgwasdbi
                to transform and import the data into a database,
                
                This includes:
                    genotypic data (.012)
                    lines (.012.indv)
                    SNP positions (.012.pos)
                    GWAS results (.csv)
                    Measurements/phenotypes (.csv)
                    Kinship matrix (.csv)
                    Population structure matrix (.csv)
                
            ./README.txt (this file)
                Documentation for files associated with the {self.alias} data set
                
            ./src
                This folder contains copies of the original files used to create the input
                files for GWAS database. These are typically the output files from the associated
                GWAS pipeline.
            
            """
            ofp.write(title)
            ofp.write(dedent(template))

    def __run_wizard(self):
        # Get the dataset alias
        data_files = []
        for root, _, files in os.walk(self.fpath):
            data_files.extend([ os.path.join(root, f) for f in files ])
        logging.debug(f"{data_files=}")

        # If the folder is empty, generate the slug from its name
        if not self.alias:
            self.alias = os.path.basename(self.fpath)
            if self.metadata_fpath is not None:
                self.slug = os.path.splitext(os.path.basename(self.metadata_fpath))[0]
            else:
                self.slug = re.sub(r"\s+", "-", self.alias).lower()
            self.slug = questionary.text(message=f"Dataset filename [{self.slug}]:", default=self.slug, validate=isValidFilename).ask()

            # Update metadata file if changed
            if self.metadata_fpath is not None:
                self.metadata_fpath = os.path.join(os.path.dirname(self.metadata_fpath), f"{self.slug}.json")

        # Check to see if the JSON file already exists
        # If it does, load the values as defaults
        self.__preload()

        # Find chromosome files (e.g., .012) files
        # Use the number of files
        # genotype files
        genotype_files = [ f for f in data_files if f.endswith('.012')]
        genotype_files = sorted(genotype_files, key=cmp_to_key(orderByChr))
        logging.debug(pformat(genotype_files))

        # Species shortname (extracted from .012 files)
        shortname = self.__data["species_shortname"] if self.__data["species_shortname"] else ""
        logging.info(f"Preloaded value for {shortname=}")
        if not shortname and genotype_files:
            shortname_pattern = r"chr(?P<id>\d+)_+(?P<shortname>[^\.]+)\.012"
            match = re.match(shortname_pattern, os.path.basename(genotype_files[0]))
            logging.debug(f"Guessed species shortname: {match=}")
            if match and "shortname" in match.groupdict():
                shortname = match.group("shortname")
        self.__data["species_shortname"] = questionary.text(message="Species shortname:", default=shortname).ask()

        # Species binomial name
        binomial_name = self.__data["species_binomial_name"]
        match self.__data["species_shortname"].lower():
            case "maize":
                binomial_name = "Zea mays"
            case "setaria":
                binomial_name = "Setaria viridis"
            case "soybean":
                binomial_name = "Glycine max"
            case "sorghum":
                binomial_name = "Sorghum bicolor"
        self.__data["species_binomial_name"] = questionary.text(message="Species binomial name:", default=binomial_name).ask()

        # Species subspecies
        self.__data["species_subspecies"] = questionary.text(message="Species subspecies name:", default=self.__data["species_subspecies"]).ask()

        # Species variety
        self.__data["species_variety"] = questionary.text(message="Species variety name:", default=self.__data["species_variety"]).ask()

        # Chromosome count
        chromosome_count = self.__data["number_of_chromosomes"] if "number_of_chromosomes" in self.__data and self.__data["number_of_chromosomes"] else len(genotype_files)
        self.__data["number_of_chromosomes"] = questionary.text(message="Number of chromosomes:", default=str(chromosome_count), validate=isNaturalNumber).ask()
        if self.__data["number_of_chromosomes"]:
            self.__data["number_of_chromosomes"] = int(self.__data["number_of_chromosomes"])

        # Population name
        self.__data["population_name"] = questionary.text(message="Population name:", default=self.__data["population_name"]).ask()

        # Genotype version assembly name
        self.__data["genotype_version_assembly_name"] = questionary.text(message="Genotype version assembly name:", default=self.__data["genotype_version_assembly_name"]).ask()

        # Genotype version annotation name
        self.__data["genotype_version_annotation_name"] = questionary.text(message="Genotype version annotation name:", default=self.__data["genotype_version_annotation_name"]).ask()

        # Reference line name
        self.__data["reference_genome_line_name"] = questionary.text(message="Reference genome line name:", default=self.__data["reference_genome_line_name"]).ask()

        # GWAS algorithm name
        self.__data["gwas_algorithm_name"] = questionary.text(message="GWAS algorithm name:", default=self.__data["gwas_algorithm_name"]).ask()

        # Imputation method name
        self.__data["imputation_method_name"] = questionary.text(message="Imputation method name:", default=self.__data["imputation_method_name"]).ask()

        # Kinship Matrix
        # NOTE(tparker): there's a non-zero chance that there's more than one kinship matrix, so account for that
        kinship_filename = self.__data["kinship_filename"] if "kinship_filename" in self.__data and self.__data["kinship_filename"] is not None else ""
        kinship_files = [ os.path.basename(f) for f in data_files if "kinship" in f ]
        logging.debug(f"Candidate kinship matrix file(s): {kinship_files=}")
        if kinship_files:
            if len(kinship_files) > 1:
                kinship_filename = questionary.select(message="Select kinship matrix file:", choices=kinship_files, default=kinship_filename).ask()
            else:
                kinship_filename = kinship_files[0]
        
        kinship_filename = os.path.basename(kinship_filename)  # make sure to use basename
        logging.debug(f"{kinship_filename=}")
        self.__data["kinship_filename"] = kinship_filename

        # Kinship Algorithm
        kinship_algortihm_name = ""
        if "astlebalding" in kinship_filename.lower():
            kinship_algortihm_name = "Astle-Balding"
        elif "raden" in kinship_filename.lower() and "van" in kinship_algortihm_name.lower():
            kinship_algortihm_name = "VanRaden"
        self.__data["kinship_algortihm_name"] = questionary.text(message="Kinship algorithm name:", default=self.__data["kinship_algortihm_name"]).ask()

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
        self.__data["missing_SNP_cutoff_value"] = questionary.text(message="missing_SNP_cutoff_value:", default=str(self.__data["missing_SNP_cutoff_value"]), validate=isDecimalNumber).ask()
        if self.__data["missing_SNP_cutoff_value"]:
            self.__data["missing_SNP_cutoff_value"] = float(self.__data["missing_SNP_cutoff_value"])
        self.__data["missing_line_cutoff_value"] = questionary.text(message="missing_line_cutoff_value:", default=str(self.__data["missing_line_cutoff_value"]), validate=isDecimalNumber).ask()
        if self.__data["missing_line_cutoff_value"]:
            self.__data["missing_line_cutoff_value"] = float(self.__data["missing_line_cutoff_value"])
        self.__data["minor_allele_frequency_cutoff_value"] = questionary.text(message="minor_allele_frequency_cutoff_value:", default=str(self.__data["minor_allele_frequency_cutoff_value"]), validate=isDecimalNumber).ask()
        if self.__data["minor_allele_frequency_cutoff_value"]:
            self.__data["minor_allele_frequency_cutoff_value"] = float(self.__data["minor_allele_frequency_cutoff_value"])

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
            if not self.__data["gwas_results_filename"]:
                self.__data["gwas_results_filename"] = []
            self.__data["gwas_run_filename"] = self.__data["gwas_results_filename"]
            if not self.__data["gwas_run_filename"]:
                self.__data["gwas_run_filename"] = []

        # Phenotype files
        ## Used files
        input_files = [ os.path.basename(f) for f in data_files if os.path.basename(os.path.dirname(f)) == "input" ]
        used_files = []
        used_files.extend(self.__data["gwas_results_filename"])
        used_files.append(self.__data["kinship_filename"])
        used_files.append(self.__data["population_structure_filename"])
        used_files.extend([ os.path.basename(f) for f in genotype_files ])
        used_files.extend([ f for f in input_files if f.endswith(".012.pos") or f.endswith(".012.indv") ])
        used_files.extend([ f for f in input_files if f.endswith(".json") ])

        ## Ask user to confirm phenotype files
        candidate_phenotype_files = [ f for f in input_files if os.path.basename(f) not in used_files ]
        phenotype_files = []
        if candidate_phenotype_files:
            phenotype_file_choices = []
            if candidate_phenotype_files:
                for fname in [ os.path.basename(f) for f in candidate_phenotype_files ]:
                    if ("population" in fname and "struc" in fname) or "kinship" in fname:
                        phenotype_file_choices.append(questionary.Choice(fname, checked=False))
                    else:
                        phenotype_file_choices.append(questionary.Choice(fname, checked=True))

                phenotype_files = questionary.checkbox(message="Select phenotype/measurement files", choices=phenotype_file_choices).ask()

        if phenotype_files:
            self.__data["phenotype_filename"] = phenotype_files

    def __init__(self, fpath = None, *args, **kwargs):
        # Establish default values
        self.permissions = stat.S_IRUSR | stat.S_IWUSR
        self.fpath = os.path.realpath(fpath)  # realpath to data folder
        self.readme = None  #  documentation
        self.alias = None  #  short-hand name of data set
        self.slug = None  #  filename friendly name for dataset (e.g., WiDiv or wisconsin-diversity-panel)
        self.metadata_fpath = None  # filename for dataset configuration file (e.g., WiDiv.json)
        self.__data = dict()
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
        if not os.path.exists(self.fpath) or isEmptyDirectory(self.fpath):
            self.__init_file_structure()
            self.__run_wizard()

        # Base case: folders exists with data
        else:
            # If the JSON file already exists for this data set, preload it
            json_files = []
            for root, _, files in os.walk(os.path.join(self.fpath, "input")):
                for f in files:
                    if f.endswith('.json'):
                        json_files.append(os.path.join(root, f))
            logging.info(f"{json_files=}")
            if json_files:
                self.metadata_fpath = questionary.select(message="Select configuration file", choices=json_files).ask()
            self.__run_wizard()

        # Determine metadata filepath
        if not self.metadata_fpath:
            self.metadata_fpath = os.path.join(self.fpath, "input", f"{self.slug}.json")
            logging.info(f"Metadata filepath has not been defined. Assigning to {self.metadata_fpath}")

        # Check that the file already exists
        write_flag = True
        if os.path.exists(self.metadata_fpath):
            style = questionary.Style([("qmark", "fg:orange bold")])
            write_flag = questionary.confirm(message=f"Warning! The configuration file, '{os.path.basename(self.metadata_fpath)}', already exists. Do you wish to overwrite it? This *cannot* be undone.", default=False, qmark="!", style=style).ask()
        
        if write_flag:
            with open(self.metadata_fpath, 'w') as ofp:
                json.dump(self.__data, ofp, indent=4)

        # Preview form data
        logging.debug(self.__data)
        pprint((self.__data))




    def __repr__(self):
        return f"Dataset('{self.fpath}')"
