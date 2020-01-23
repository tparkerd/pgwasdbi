import errno
import os

from importation.util import find, insert
from pgwasdbi.util.models import (chromosome, genotype, genotype_version,
                                  growout, growout_type, gwas_algorithm,
                                  gwas_result, gwas_run, imputation_method,
                                  kinship, kinship_algorithm, line, location,
                                  phenotype, population, population_structure,
                                  population_structure_algorithm, species,
                                  trait, variant)


def design(args):
    # # =====================================
    # # ========== Pipeline Design ==========
    # # =====================================
    # # GWAS Algorithm: "MLMM", "EMMAx", "GAPIT", "FarmCPU"
    # # Imputation Method: "impute to major allele"
    # # Kinship Algorithm: "loiselle"
    # # Population Structure Algorithm: "Eigenstrat"

    # Set shorter variable names for frequently used values
    conf = args.conf
    conn = args.conn

    # Expected User Input
    # GWAS Algorithm
    # According to Greg's README
    gwas_algorithm_name = conf['gwas_algorithm_name']
    # Imputation Method
    # Unknown, apparently it was done by someone named Sujan
    imputation_method_name = conf['imputation_method_name']
    # Kinship Algorithm
    # Placeholder, I don't know the exact string that should be used
    kinship_algorithm_name = conf['kinship_algortihm_name']
    # Population Structure Algorithm
    # This is a guess based on filename
    population_structure_algorithm_name = conf['population_structure_algorithm_name']

    # Model Construction & Insertion
    # GWAS Algorithm
    ga = gwas_algorithm(gwas_algorithm_name)
    conf['gwas_algorithm_id'] = insert.insert_gwas_algorithm(conn, args, ga)
    # Imputation Method
    im = imputation_method(imputation_method_name)
    conf['imputation_method_id'] = insert.insert_imputation_method(conn, args, im)
    # Kinship Algorithm
    ka = kinship_algorithm(kinship_algorithm_name)
    conf['kinship_algorithm_id'] = insert.insert_kinship_algorithm(conn, args, ka)
    # Population Structure Algorithm
    psa = population_structure_algorithm(population_structure_algorithm_name)
    conf['population_structure_algorithm_id'] = insert.insert_population_structure_algorithm(
        conn, args, psa)


def collect(args):
    # =========================================
    # ========== Pipeline Collection ==========
    # =========================================
    # Kinship
    # Setaria Kinship is stored in:
    # /shares/ibaxter_share/gziegler/SetariaGWASPipeline/data/genotype/6.AstleBalding.synbreed.kinship.rda
    # Exported the file to CSV using R
    # load('6.AstleBalding.synbreed.kinship.rda')
    ### write.csv(kinship, '6.AstleBalding.synbreed.kinship.csv')
    # Population Structure

    # Set shorter variable names for frequently used values
    conf = args.conf
    conn = args.conn

    # Expected User Input
    # Kinship
    # NOTE(tparker): Currently the database just stores the filename.
    #                There is no service to upload the file to database's
    #                host, so there's no single location to find the file
    #                I would like to find out why this is the case and if
    #                it would just be better to store it in the database and
    #                allow the user to export the table themselves as a CSV.
    kinship_filepath = f'{args.cwd}/{conf["kinship_filename"]}'
    # Population Structure
    # NOTE(tparker): Same reasoning as the kinship file. There should be a way
    #                for the data to be stored in the database, not a
    population_structure_filepath = f'{args.cwd}/{conf["population_structure_filename"]}'

    kinship_algorithm_id = conf['kinship_algorithm_id']
    population_structure_algorithm_id = conf['population_structure_algorithm_id']

    # Model Construction & Insertion
    # Kinship
    try:
        if not os.path.isfile(kinship_filepath):
            raise FileNotFoundError(errno.ENOENT, os.strerror(
                errno.ENOENT), kinship_filepath)
        if not os.path.isfile(population_structure_filepath):
            raise FileNotFoundError(errno.ENOENT, os.strerror(
                errno.ENOENT), population_structure_filepath)
    except:
        raise
    k = kinship(kinship_algorithm_id, kinship_filepath)
    kinship_id = insert.insert_kinship(conn, args, k)
    # Population Structure
    ps = population_structure(
        population_structure_algorithm_id, population_structure_filepath)
    population_structure_id = insert.insert_population_structure(
        conn, args, ps)



def analysis(args):
    pass
