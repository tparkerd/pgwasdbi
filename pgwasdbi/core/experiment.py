import errno
import logging
import os
from pprint import pformat
from string import Template

import pandas as pd

from pgwasdbi.util import find, insert
from pgwasdbi.util.models import (chromosome, genotype, genotype_version,
                                  growout, growout_type, gwas_algorithm,
                                  gwas_result, gwas_run, imputation_method,
                                  kinship, kinship_algorithm, line, location,
                                  phenotype, population, population_structure,
                                  population_structure_algorithm, species,
                                  trait, variant)


def design(args):
    """
    =======================================
    ========== Experiment Design ==========
    =======================================
    What the database needs in order to create an 'experiment' is the follow
    Species: maize (Zea mays)
    Population: Maize282
    Chromosome: 10 (just the number and a way to generate its unique name)
    Line: 282set_B73 (B73) -- taken from file if possible
    Genotype Version: B73 RefGen_v4_AGPv4_Maize282 (the reference genome)
    Growout, Type, and Location:
            Location: code, city, state, country
                    "PU", "West Lafayette", "Indiana", "United States"
            Type: "field", "phenotyper", etc.
            Growout: name, population ID, location ID, year, type
                    "PU09", maize282popID, PUlocID, 2009, fieldGrowoutTypeID
    Traits (planned phenotypes/traits to measure)    
    """

    # Set shorter variable names for frequently used values
    conf = args.conf
    conn = args.conn

    # Expected User Input
    # Species
    species_shortname = conf['species_shortname']
    species_binomial = conf['species_binomial_name']
    species_subspecies = conf['species_subspecies']
    species_variety = conf['species_variety']
    # Population
    population_name = conf['population_name']
    # Chromosome
    chromosome_count = conf['number_of_chromosomes']
    # Genotype Version
    # NOTE(tparker): This is possibly just the info about the reference genome
    #                It is likely included with the VCF genotype file (.012).
    genotype_version_assembly_name = conf['genotype_version_assembly_name']
    genotype_version_annotation_name = conf['genotype_version_annotation_name']
    reference_genome_line_name = conf['reference_genome_line_name']
    # Growout, Type, and Location
    # NOTE(tparker): Unknown at this time
    # Location
    # Type
    # Growout
    #
    # # Traits
    # # Allow for more than on phenotype files
    # if isinstance(conf["phenotype_filename"], list):
    #     conf['phenotype_filenames'] = [
    #         f'{args.cwd}/{filename}' for filename in conf['phenotype_filename']]
    # else:
    #     conf['phenotype_filenames'] = [f'{args.cwd}/{conf["phenotype_filename"]}']

    # Model Construction & Insertion
    # Species
    s = species(species_shortname, species_binomial,
                species_subspecies, species_variety)
    species_id = insert.insert_species(conn, args, s)
    conf['species_id'] = species_id
    logging.debug(f'[Insert]\tSpecies ID\t{species_id}, {s}')
    # Population
    p = population(population_name, species_id)
    population_id = insert.insert_population(conn, args, p)
    conf['population_id'] = population_id
    logging.debug(f'[Insert]\tPopulation ID\t{population_id}: {p}')
    # Chromosome
    chromosome_ids = insert.insert_all_chromosomes_for_species(
        conn, args, chromosome_count, species_id)
    conf['chromosome_ids'] = chromosome_ids
    logging.debug(f'[Insert]\tChromosome IDs\t{chromosome_ids}')
    # Line
    # working_filepath = lines_filename.substitute(dict(chr="chr1", cwd=f"{args.cwd}", shortname=species_shortname))
    working_filepath = conf["lines_filename"]
    try:
        if not os.path.isfile(working_filepath):
            raise FileNotFoundError(errno.ENOENT, os.strerror(
                errno.ENOENT), working_filepath)
    except:
        raise

    # hard-coded substitue until just one file is used for lines
    line_ids = insert.insert_lines_from_file(
        conn, args, working_filepath, population_id)
    logging.debug(f'[Insert]\tLine IDs\t{line_ids}')
    # Genotype Version
    reference_genome_id = find.find_line(conn, args, reference_genome_line_name, population_id)
    if reference_genome_id is None:
        raise Exception(f"Reference genome '{reference_genome_line_name}' must exist in the given population.")
    logging.debug(
        f'[Insert]\tReference Genome ID\t{reference_genome_id}, ({reference_genome_line_name}, {population_id})')
    gv = genotype_version(genotype_version_name=genotype_version_assembly_name,
                          genotype_version=genotype_version_annotation_name,
                          reference_genome=reference_genome_id,
                          genotype_version_population=population_id)
    genotype_version_id = insert.insert_genotype_version(conn, args, gv)
    conf['genotype_version_id'] = genotype_version_id
    logging.debug(f'[Insert]\tGenome Version ID\t{genotype_version_id}')
    if genotype_version_id is None:
        raise Exception(f'Genotype version is None for parameters: {pformat(gv)}')

    # Growout, Type, and Location
    # NOTE(tparker): Unknown at this time
    # Location
    # Type
    # Growout

    # Traits
    # Go through all the phenotype files available for the dataset and insert
    # the recorded traits for each.
    for phenotype_filepath in conf['phenotype_filename']:
        try:
            if not os.path.isfile(phenotype_filepath):
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), phenotype_filepath)
        except:
            raise
        traits = list(pd.read_csv(phenotype_filepath, index_col=0))
        trait_ids = insert.insert_traits_from_traitlist(
            conn, args, traits, phenotype_filepath)
        logging.debug(
            f'[Insert]\tTrait IDs for {phenotype_filepath}\t{trait_ids}')


def collect(args):
    # ===========================================
    # ========== Experiment Collection ==========
    # ===========================================
    # Phenotype (external source?)
    #       This needs to be standardized to a .pheno filetype.
    #       For now, it is the longFormat for the Maize282 datset
    #       5.mergedWeightNorm.LM.rankAvg.longFormat.csv, but for Setaria will be
    # Genotype (VCF output)
    # Variant (VCF output)

    # Set shorter variable names for frequently used values
    conf = args.conf
    conn = args.conn

    # Expected User Input
    # Phenotype
    # NOTE(tparker): Define in earlier stage
    # Genotype
    genotype_filename = Template('${cwd}/${chr}_${shortname}.012')
    # Variants
    variants_filename = Template('${cwd}/${chr}_${shortname}.012.pos')

    chromosome_count = conf['number_of_chromosomes']
    species_id = conf['species_id']
    species_shortname = conf['species_shortname']
    genotype_version_id = conf['genotype_version_id']

    # Model Construction & Insertion
    # Phenotype
    for phenotype_filepath in conf['phenotype_filename']:
        if not os.path.isfile(phenotype_filepath):
            raise FileNotFoundError(errno.ENOENT, os.strerror(
                errno.ENOENT), phenotype_filepath)

        population_id = conf['population_id']
        phenotype_ids = insert.insert_phenotypes_from_file(
            conn, args, phenotype_filepath, population_id, phenotype_filepath)
        logging.debug(
            f'[Insert]\tPhenotype IDs for {phenotype_filepath}\t{phenotype_ids}')

    # Genotype
    line_filename = conf['lines_filename']
    for c in range(1, chromosome_count + 1):
        chromosome_shortname = 'chr' + str(c)
        chromosome_id = find.find_chromosome(
            conn, args, chromosome_shortname, species_id)
        geno_filename = genotype_filename.substitute(
            dict(chr=chromosome_shortname, cwd=f'{args.cwd}', shortname=species_shortname))

        if not os.path.isfile(geno_filename):
            logging.warning(FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), geno_filename)) 
            continue
        if not os.path.isfile(line_filename):
            logging.error(FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), line_filename))
        genotype_ids = insert.insert_genotypes_from_file(conn=conn,
                                                         args=args,
                                                         genotypeFile=geno_filename,
                                                         lineFile=line_filename,
                                                         chromosomeID=chromosome_id,
                                                         populationID=population_id,
                                                         genotype_versionID=genotype_version_id
                                                         )
        conf['genotype_ids'] = genotype_ids

    # Variants
    for c in range(1, chromosome_count + 1):
        chromosome_shortname = 'chr' + str(c)
        chromosome_id = find.find_chromosome(
            conn, args, chromosome_shortname, species_id)
        variant_filename = variants_filename.substitute(
            dict(chr=chromosome_shortname, cwd=f'{args.cwd}', shortname=species_shortname))
        if not os.path.isfile(variant_filename):
            logging.warning(FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), variant_filename))
            continue
        # insert.insert_variants_from_file(conn,
        #                                  args,
        #                                  variant_filename,
        #                                  species_id,
        #                                  chromosome_id)

        # NOTE(tparker): Changed variant insertion to the async version
        insert.insert_variants_from_file_async(conn,
                                               args,
                                               variant_filename,
                                               species_id,
                                               chromosome_id)
