"""Validation methods for importation"""
import errno
import json
import logging
import os
import re
from pprint import pformat
from string import Template

import asyncpg
import pandas as pd
from pandas_schema import Column, Schema
from pandas_schema.validation import (CanConvertValidation,
                                      CustomSeriesValidation,
                                      IsDistinctValidation)
from tqdm import tqdm

from pgwasdbi.util.database import config, connect


def file_exists(args, filepath):
    if not os.path.isfile(filepath):
        return True
    else:
        logging.error("%s: %s", os.strerror(errno.ENOENT), filepath)
        return False


# TODO(tparker)
def location_exists(args, location):
    # Check that a growout location exists in the database
    pass


def validate_phenotype(args, filepath):
    """Validates input file for phenotype data

    This function validates that the contents of a file to contain phenotype
    data. If an error is encountered, throw an exception.

    Args:
        conn (psycopg2.extensions.connection): psycopg2 connection
        args (ArgumentParser namespace): user-defined arguments
        filepath (str): location of input file
    """
    logging.debug(f"Validating phenotype file: {filepath}")
    df = pd.read_csv(filepath)
    nrows, ncols = df.shape
    nrows += 1  # include the header in the row count

    acceptable_fields = ['genotype', 'pedigree', 'line', 'taxa']
    field_pattern = '|'.join([f"({af})" for af in acceptable_fields])
    if re.match(field_pattern, df.columns[0], re.IGNORECASE) is None:
        raise Exception(f"The name of the first column of a phenotype file should be one of the following values: {acceptable_fields}. Violating file: '{filepath}'")

    # Rename the first column of data to be the genotypes/lines
    df.rename(columns={f'{df.columns[0]}': 'genotype'}, inplace=True)

    schema_columns = [
        Column('genotype', [
            # For the WiDiv dataset, the phenotypes contained multiple entries per line
            # Therfore, I had to comment this out to allow data to pass validation
            # IsDistinctValidation()
        ])
    ]

    for n in range(1, ncols):
        schema_columns.append(
            Column(df.columns[n], [
                # NOTE(tparker): This may not always be true. If there any phenotypes that
                # are listed as categories or strings, then this would fail
                # Find out all the possible phenotype values. It may be difficult to
                # validate input data without a user-provided dtype list
                CanConvertValidation(float)
            ])
        )

    schema = Schema(schema_columns)
    err = schema.validate(df)

    if err:
        for e in err:
            logging.error(f"Error encountered while validating: {filepath}")
            raise Exception(e)


def validate_line(args, filepath):
    """Validates input file for line data

    This function validates that the contents of a file to contain line data.
    If an error is encountered, throw an exception.

    Args:
        conn (psycopg2.extensions.connection): psycopg2 connection
        args (ArgumentParser namespace): user-defined arguments
        filepath (str): location of input file
    """
    schema = Schema([
        Column('line_name', [
            IsDistinctValidation()
        ])
    ])

    df = pd.read_csv(filepath, header=None)

    if len(df.columns) != 1:
        raise Exception(f"Invalid file format. Excepted 1 column found {len(df.columns)} columns. This file should be a single column of each line. Each entry should be distinct.")
    df.columns = ['line_name']
    err = schema.validate(df)

    if err:
        for e in err:
            logging.error(f"Error encountered while validating: {filepath}")
            raise Exception(e)


def validate_genotype(args, filepath):
    """Validates input file for genotype data

    This function validates that the contents of a file to contain genotype data.
    If an error is encountered, throw an exception.

    Args:
        conn (psycopg2.extensions.connection): psycopg2 connection
        args (ArgumentParser namespace): user-defined arguments
        filepath (str): location of input file
    """
    # Allow for users to skip this validation step because it is time consuming
    if args.skip_genotype_validation is True:
        return

    schema_columns = [
        Column('row_number', [
            CanConvertValidation(int) &
            IsDistinctValidation()
        ])
    ]

    # Get the number of lines from the .pos counterpart file
    pos_filepath = '.'.join([filepath, 'pos'])
    if not os.path.exists(pos_filepath):
        raise FileNotFoundError(f"Count not locate the position counterpart file for {filepath}")
    nPositions = len(pd.read_csv(pos_filepath, header=None).index)

    for n in range(nPositions):
        schema_columns.append(
            Column(f'pos_{n}', [
                CanConvertValidation(int) &
                CustomSeriesValidation(lambda x: x.int in [-1, 0, 1, 2], 'Incorrectly coded value.')
            ])
        )

    schema = Schema(schema_columns)

    df = pd.read_csv(filepath, sep='\t', header=None)

    err = schema.validate(df)
    if err:
        for e in err:
            logging.error(f"Error encountered while validating: {filepath}")
            raise Exception(e)


def validate_variant(args, filepath):
    """Validates input file for variant data

    This function validates that the contents of a file to contain variant data.
    If an error is encountered, throw an exception.

    Args:
        conn (psycopg2.extensions.connection): psycopg2 connection
        args (ArgumentParser namespace): user-defined arguments
        filepath (str): location of input file
    """
    schema = Schema([
        Column('chr', [
            CanConvertValidation(int)
        ]),
        Column('pos', [
            CanConvertValidation(int),
            IsDistinctValidation()
        ])
    ])

    df = pd.read_csv(filepath, sep='\t', header=None)

    if len(df.columns) != 2:
        raise Exception(f"Invalid file format. Excepted 2 columns, found {len(df.columns)} columns. Columns should consist of chromsome number and SNP position. Filepath: {filepath}")

    df.columns = ['chr', 'pos']
    err = schema.validate(df)

    if err:
        for e in err:
            logging.error(f"Error encountered while validating: {filepath}")
            raise Exception(e)


def validate_kinship(args, filepath):
    """Validates input file for kinship data

    This function validates that the contents of a file to contain kinship data.
    If an error is encountered, throw an exception.

    Args:
        conn (psycopg2.extensions.connection): psycopg2 connection
        args (ArgumentParser namespace): user-defined arguments
        filepath (str): location of input file
    """
    df = pd.read_csv(filepath)
    nrows, ncols = df.shape
    # Rename the first column for easier access
    df.rename(columns={df.columns[0]: "line_name"}, inplace=True)
    nrows += 1  # include the header row in the count
    logging.debug(f"Dimensions of kinship matrix: <{nrows}, {ncols}>")

    schema_columns = [
        Column('line_name', [
            IsDistinctValidation()
        ])
    ]

    for n in range(1, ncols):
        schema_columns.append(Column(df.columns[n], [
            CanConvertValidation(float)
        ]))

    schema = Schema(schema_columns)

    err = schema.validate(df)

    if err:
        for e in err:
            logging.error(f"Error encountered while validating: {filepath}")
            raise Exception(e)


def validate_population_structure(args, filepath):
    """Validates input file for population structure data

    This function validates that the contents of a file to contain population
    structure data. If an error is encountered, throw an exception.

    Args:
        conn (psycopg2.extensions.connection): psycopg2 connection
        args (ArgumentParser namespace): user-defined arguments
        filepath (str): location of input file
    """
    df = pd.read_csv(filepath)
    nrows, ncols = df.shape
    nrows += 1  # include the header rows in the count
    logging.debug(f'Population structure columns: {df.columns}')
    logging.debug(f"Population structure dimensions: <{nrows}, {ncols}>")

    schema_columns = [
        Column('Pedigree', [
            IsDistinctValidation()
        ])
    ]

    for n in range(1, ncols):
        schema_columns.append(Column(df.columns[n], [
            CanConvertValidation(float)
        ]))

    schema = Schema(schema_columns)
    err = schema.validate(df)

    if err:
        for e in err:
            logging.error(f"Error encountered while validating: {filepath}")
            raise Exception(e)


def validate_runs(args, filepath):
    """Validates input file for GWAS run data

    This function validates that the contents of a file to contain GWAS run data.
    If an error is encountered, throw an exception.

    Args:
        conn (psycopg2.extensions.connection): psycopg2 connection
        args (ArgumentParser namespace): user-defined arguments
        filepath (str): location of input file
    """
    df = pd.read_csv(filepath)
    # For each column, add it to the schema, and then for known ones, add the
    # schema validation. Use fuzzy comparisons when possible
    schema_columns = []
    for col in df.columns:
        validators = []
        if re.match("(SNP)|(chr)|(chrom)|(pos)|(nSNPs)", col, re.IGNORECASE):
            validators.append(CanConvertValidation(int))
        # Look for any of the p-values and make sure that they can be cast as a float
        if re.match("((null)?pval(ue)?)", col, re.IGNORECASE):
            validators.append(CanConvertValidation(float))

        schema_columns.append(Column(col, validators))
    schema = Schema(schema_columns)

    err = schema.validate(df)
    if err:
        for e in err:
            logging.error(f"Error encountered while validating: {filepath}")
            raise Exception(e)


def validate_results(args, filepath):
    """Validates input file for GWAS result data

    This function validates that the contents of a file to contain GWAS result data.
    If an error is encountered, throw an exception.

    Args:
        conn (psycopg2.extensions.connection): psycopg2 connection
        args (ArgumentParser namespace): user-defined arguments
        filepath (str): location of input file
    """
    df = pd.read_csv(filepath)
    # For each column, add it to the schema, and then for known ones, add the
    # schema validation. Use fuzzy comparisons when possible
    schema_columns = []
    for col in df.columns:
        validators = []
        acceptable_fields = ['SNP', 'chr', 'chrom', 'pos', 'nSNPs']
        acceptable_fields = '|'.join([f"(^{af}$)" for af in acceptable_fields])
        if re.match(acceptable_fields, col, re.IGNORECASE):
            validators.append(CanConvertValidation(int))
        # Look for any of the p-values and make sure that they can be cast as a float
        if re.match("((null)?pval(ue)?)", col, re.IGNORECASE):
            validators.append(CanConvertValidation(float))

        schema_columns.append(Column(col, validators))
    schema = Schema(schema_columns)

    err = schema.validate(df)
    if err:
        for e in err:
            logging.error(f"Error encountered while validating: {filepath}")
            raise Exception(e)


async def validate_variant_sync(args, data):
    """Asynchronous validation of variant data"""
    cred = config(args)
    conn = await asyncpg.connect(host=cred['host'],
                                 port=cred['port'],
                                 user=cred['user'],
                                 password=cred['password'],
                                 database=cred['database'])
    # Asynchronous insertion function is actually a COPY postgresql statement
    # Therefore, you have to validate the input data does not conflict  with
    # existing data before importation
    results = []
    stmt = await conn.prepare('''SELECT variant_id FROM variant WHERE variant_species = $1 AND variant_chromosome = $2 AND variant_pos = $3''')
    for d in tqdm(data, desc='Validate variant input contents: '):
        # Expand the data tuple to be the chromosome_id, species_id, and variant position (SNP position)
        c, s, p = d
        # Check if the SNP information has already been imported into the database
        r = await stmt.fetchval(c, s, p)
        # If it has *not* been imported yet, save it for later, otherwise skip and discard it
        if r is None:
            results.append(d)

    await conn.close()

    # Send back a list of tuples to import
    return results


def validate_configuration(args, filepath):
    # Input file preprocessing and validation
    try:
        if not os.path.isfile(filepath):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), filepath)
        else:
            with open(filepath) as f:
                conf = json.load(f)  # data parameters

            # Verify that all necessary values were provided, assuming a complete dataset
            expected_fields = ["species_shortname",
                               "species_binomial_name",
                               "species_subspecies",
                               "species_variety",
                               "population_name",
                               "number_of_chromosomes",
                               "genotype_version_assembly_name",
                               "genotype_version_annotation_name",
                               "reference_genome_line_name",
                               "gwas_algorithm_name",
                               "imputation_method_name",
                               "kinship_algortihm_name",
                               "population_structure_algorithm_name",
                               "kinship_filename",
                               "population_structure_filename",
                               "gwas_run_filename",
                               "gwas_results_filename",
                               "missing_SNP_cutoff_value",
                               "missing_line_cutoff_value",
                               "minor_allele_frequency_cutoff_value",
                               "phenotype_filename"
                               ]

        missing_keys = []
        for k in expected_fields:
            if k not in conf:
                missing_keys.append(k)
        if missing_keys:
            raise KeyError(f'The following keys are required. Please include them in your JSON configuration: {missing_keys}')

        # Check for all required fields
        required_fields = ["species_shortname",
                           "species_binomial_name",
                           "population_name",
                           "number_of_chromosomes",
                           "genotype_version_assembly_name",
                           "genotype_version_annotation_name",
                           "reference_genome_line_name",
                           "gwas_algorithm_name",
                           "imputation_method_name",
                           "kinship_algortihm_name",
                           "population_structure_algorithm_name",
                           "kinship_filename",
                           "population_structure_filename",
                           "gwas_run_filename",
                           "gwas_results_filename",
                           "missing_SNP_cutoff_value",
                           "missing_line_cutoff_value",
                           "minor_allele_frequency_cutoff_value",
                           "phenotype_filename"
                           ]

        empty_fields = []
        for rf in required_fields:
            if not conf[rf]:
                empty_fields.append(rf)
        if empty_fields:
            raise KeyError(f'The following keys must be defined. Empty strings are not permitted. Please modify your JSON configuration: {empty_fields}')

        logging.info('Configuration file is valid. Verifying that all files exist.')
        logging.debug(pformat(conf))

        # Track all the files to check for existance
        locations = []

        # Convert all filepaths to absolute paths
        # Filepath templates
        filepath_template = Template('${cwd}/${filename}')
        # Lines
        lines_filename = Template('${cwd}/${chr}_${shortname}.012.indv')
        # Genotype
        genotype_filename = Template('${cwd}/${chr}_${shortname}.012')
        # Variants
        variants_filename = Template('${cwd}/${chr}_${shortname}.012.pos')

        # Convert filepaths
        # Convert genotype, variant, and lines
        conf['genotype_filename'] = []
        conf['variant_filename'] = []
        for c in range(1, conf['number_of_chromosomes'] + 1):
            chr_shortname = 'chr' + str(c)
            genotype_filepath = genotype_filename.substitute(dict(cwd=args.cwd, shortname=conf['species_shortname'], chr=chr_shortname))
            conf['genotype_filename'].append(genotype_filepath)
            variants_filepath = variants_filename.substitute(dict(cwd=args.cwd, shortname=conf['species_shortname'], chr=chr_shortname))
            conf['variant_filename'].append(variants_filepath)

            # Mark for validation if requested
            if 'genotype' in args.validate:
                locations.append(dict(cwd=args.cwd, filetype='genotype', filename=genotype_filepath))
            if 'variant' in args.validate:
                locations.append(dict(cwd=args.cwd, filetype='variant', filename=variants_filepath))

        # Lines
        if 'lines_filename' not in conf:
            # Default to the first entry of the genotype files
            lines_filepath = lines_filename.substitute(dict(cwd=args.cwd, shortname=conf['species_shortname'], chr='chr1'))
        else:
            lines_filepath = filepath_template.substitute(dict(cwd=args.cwd, filename=conf['lines_filename']))
        conf['lines_filename'] = lines_filepath

        if 'line' in args.validate:
            locations.append(dict(cwd=args.cwd, filetype='line', filename=lines_filepath))


        # Go through all the single files that are not named based off of a chromsome
        # Construct the file descriptor dictionaries, and then loop through and test each file's existance
        # phenotype_filename = Template('${cwd}/${growout}.ph.csv') # Welp, this is another instance of pheno file issue

        if 'phenotype_filename' in conf:
            # If there is just one phenotype file, convert it to a list of one
            if not isinstance(conf['phenotype_filename'], list):
                conf['phenotype_filename'] = [conf['phenotype_filename']]
            conf['phenotype_filename'] = [filepath_template.substitute(dict(cwd=args.cwd, filetype='phenotype', filename=pfp)) for pfp in conf['phenotype_filename']]
        if 'population_structure_filename' in conf:
            conf['population_structure_filename'] = filepath_template.substitute(dict(cwd=args.cwd, filetype='population_structure', filename=conf['population_structure_filename']))
        if 'gwas_results_filename' in conf:
            if not isinstance(conf['gwas_results_filename'], list):
                conf['gwas_results_filename'] = [conf['gwas_results_filename']]
            conf['gwas_results_filename'] = [filepath_template.substitute(dict(cwd=args.cwd, filetype='gwas_results_filename', filename=grfp)) for grfp in conf['gwas_results_filename']]
        if 'gwas_run_filename' in conf:
            if not isinstance(conf['gwas_run_filename'], list):
                conf['gwas_run_filename'] = [conf['gwas_run_filename']]
            conf['gwas_run_filename'] = [filepath_template.substitute(dict(cwd=args.cwd, filetype='gwas_run_filename', filename=grfp)) for grfp in conf['gwas_run_filename']]
        if 'kinship_filename' in conf:
            conf['kinship_filename'] = filepath_template.substitute(dict(cwd=args.cwd, filetype='kinship_filename', filename=conf['kinship_filename']))
        if 'kinship' in args.validate:
            locations.append(dict(cwd=args.cwd, filetype='kinship', filename=conf['kinship_filename']))
        if 'population' in args.validate:
            locations.append(dict(cwd=args.cwd, filetype='population_structure', filename=conf['population_structure_filename']))

        # Since there can be more than one file for the phenotypes, results, and run
        # For each array in the configuration file, add it to the list of paths to
        # verify as existing

        for configuration_entry in conf:
            if isinstance(conf[configuration_entry], list):
                for filename in conf[configuration_entry]:
                    locations.append(dict(cwd=args.cwd, filetype=configuration_entry, filename=filename))
            else:
                # For any of the entries that CAN be a list, add their single values to
                # the file list
                if configuration_entry in ['phenotype_filename', 'gwas_run_filename', 'gwas_results_filename', 'line']:
                    locations.append(dict(cwd=args.cwd, filetype=configuration_entry, filename=conf[configuration_entry]))

        # Remove files that will not be validated
        tmp_file_locations = list(locations)
        locations = []
        for file_location in tmp_file_locations:
            for validation_step in args.validate:
                if validation_step in file_location['filetype']:
                    locations.append(file_location)

        for location in locations:
            if not os.path.isfile(location['filename']):
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), location['filename'])

        logging.info(f'Found all files found for validation steps: {args.validate}. Validating file contents.')
        configuration = {
            'chromosome': {
                'count': conf['number_of_chromosomes']
            },
            'lines': {
                'file': conf['lines_filename']
            },
            'kinship': {
                'file': conf['kinship_filename'],
                'algorithm': conf['kinship_algortihm_name']
            },
            'genotype': {
                'file': conf['genotype_filename'],
                'version': {
                    'annotation_name': conf['genotype_version_annotation_name'],
                    'assembly_name': conf['genotype_version_assembly_name']
                }
            },
            'population': {
                'name': conf['population_name'],
                'structure': {
                    'file': conf['population_structure_filename'],
                    'algorithm': conf['population_structure_algorithm_name']
                },
                'reference_genome_line': conf['reference_genome_line_name']
            },
            'phenotype': {
                'file': conf['phenotype_filename'],
            },
            'variant': {
                'file': conf['variant_filename']
            },
            'species': {
                'shortname': conf['species_shortname'],
                'binomial': conf['species_binomial_name'],
                'subspecies': conf['species_subspecies'],
                'variety': conf['species_variety']
            },
            'gwas': {
                'algorithm': conf['gwas_algorithm_name'],
                'file': conf['gwas_results_filename'],
                'imputation_method': conf['imputation_method_name'],
                'MAF_cutoff': conf['minor_allele_frequency_cutoff_value'],
                'SNP_cutoff': conf['missing_SNP_cutoff_value'],
                'line_cutoff': conf['missing_line_cutoff_value']
            }
        }

        args.chromosome = {
            'count': conf['number_of_chromosomes']
        }
        args.lines = {
            'file': conf['lines_filename']
        }
        args.kinship = {
            'file': conf['kinship_filename'],
            'algorithm': conf['kinship_algortihm_name']
        }
        args.genotype = {
            'file': conf['genotype_filename'],
            'version': {
                'annotation_name': conf['genotype_version_annotation_name'],
                'assembly_name': conf['genotype_version_assembly_name']
            }
        }
        args.population = {
            'name': conf['population_name'],
            'structure': {
                'file': conf['population_structure_filename'],
                'algorithm': conf['population_structure_algorithm_name']
            },
            'reference_genome_line': conf['reference_genome_line_name']
        }
        args.phenotype = {
            'file': conf['phenotype_filename'],
        }
        args.variant = {
            'file': conf['variant_filename']
        }
        args.species = {
            'shortname': conf['species_shortname'],
            'binomial': conf['species_binomial_name'],
            'subspecies': conf['species_subspecies'],
            'variety': conf['species_variety']
        }
        args.gwas = {
            'algorithm': conf['gwas_algorithm_name'],
            'file': conf['gwas_results_filename'],
            'imputation_method': conf['imputation_method_name'],
            'MAF_cutoff': conf['minor_allele_frequency_cutoff_value'],
            'SNP_cutoff': conf['missing_SNP_cutoff_value'],
            'line_cutoff': conf['missing_line_cutoff_value']
        }

        # Validate the contents of each file
        for file_descriptor in locations:
            ft = file_descriptor['filetype']
            fp = file_descriptor['filename']
            if ft == 'line' and 'line' in args.validate:
                validate_line(args, fp)
            elif ft == 'variant' and 'variant' in args.validate:
                validate_variant(args, fp)
            elif ft == 'genotype' and 'genotype' in args.validate:
                validate_genotype(args, fp)
            elif ft == 'kinship' and 'kinship' in args.validate:
                validate_kinship(args, fp)
            elif ft == 'population_structure' and 'population' in args.validate:
                validate_population_structure(args, fp)
            elif ft == 'phenotype_filename' and 'phenotype' in args.validate:
                validate_phenotype(args, fp)
            elif ft == 'gwas_run_filename' and 'runs' in args.validate:
                validate_runs(args, fp)
            elif ft == 'gwas_results_filename' and 'result' in args.validate:
                validate_results(args, fp)
            else:
                logging.debug(f"Calling validation on unknown file: {fp}")
    except Exception as err:
        raise err

    logging.info(f'Input files appear to be valid. Proceeding with import.')
    # args.conf = conf


def validate(args):
    print("==========================\n\nDo the validation thing, Zhu Li!\n\n==========================")
    logging.debug(f'Validating {args.validate}')
    try:
        args.conn = connect(args)
        validate_configuration(args, args.fp)
        logging.debug(f"Initialized arguments")
        logging.debug(pformat(args))
    except Exception as err:
        raise err
