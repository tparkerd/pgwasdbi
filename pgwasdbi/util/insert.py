"""Constructs and executes SQL to insert data into database"""
import asyncio
import logging
import time
from pprint import pformat

import asyncpg
import numpy as np
import pandas as pd
import psycopg2 as pg
from tqdm import tqdm

import importation.util.find as find
import importation.util.parsinghelpers as ph
from importation.util.dbconnect import config
from importation.util.models import (chromosome, genotype, genotype_version,
                                     growout, growout_type, gwas_algorithm,
                                     gwas_result, gwas_run, imputation_method,
                                     kinship, kinship_algorithm, line,
                                     location, phenotype, population,
                                     population_structure,
                                     population_structure_algorithm, species,
                                     trait, variant)
from importation.util.validate import validate_variant_sync


def exists_in_database(cur, SQL, params):
  """Checks if an object has already been inserted into the database

  Args:
    cur (psycopg2.extensions.cursor): psycopg2 cursor
    SQL (str): SQL statement to be executed

  Returns:
    int if true, None otherwise.

  """
  logging.debug(f'Existance Check: {pformat(SQL)} with parameters: {pformat(params)}')
  cur.execute(SQL, params)
  query_result = cur.fetchone()
  if query_result is not None:
    known_id = query_result[0]
    if known_id:
      return known_id
  
  return None

def insert_species(conn, args, species):
  """Inserts species into database by its shortname, binomial, subspecies, and variety

  This function inserts a species into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    species (species): :ref:`species <species_class>` object
  
  Returns:
    int: species id
  """
  cur = conn.cursor()

  # See if data has already been inserted, and if so, return it
  SQL = """SELECT species_id \
          FROM species \
          WHERE shortname = %s AND \
                binomial = %s AND \
                subspecies = %s AND \
                variety = %s"""
  args_tuple = (species.n, species.b, species.s, species.v)

  known_id = exists_in_database(cur, SQL, args_tuple)
  if known_id is not None:
    conn.commit()
    cur.close()
    return known_id

  SQL = """INSERT INTO species (shortname, binomial, subspecies, variety)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING species_id;"""
  cur.execute(SQL, args_tuple)
  row = cur.fetchone()
  if row is not None:
    newID = row[0]
    conn.commit()
    cur.close()
    return newID
  else:
    return None


def insert_population(conn, args, population):
  """Inserts population into database

  This function inserts a population into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    population (population): :ref:`population <population_class>` object

  Returns:
    int: population id
  """
  cur = conn.cursor()

  # See if data has already been inserted, and if so, return it
  SQL = """SELECT population_id \
          FROM population \
          WHERE population_name = %s AND \
                population_species = %s"""
  args_tuple = (population.n, population.s)
  
  known_id = exists_in_database(cur, SQL, args_tuple)
  if known_id is not None:
    conn.commit()
    cur.close()
    return known_id

  SQL = """INSERT INTO population (population_name, population_species)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        RETURNING population_id;"""
  cur.execute(SQL, args_tuple)
  row = cur.fetchone()
  if row is not None:
     newID = row[0]
     conn.commit()
     cur.close()
     return newID
  else:
    return None  

def insert_chromosome(conn, args, chromosome):
  """Inserts chromosome into database by its name

  This function inserts a chromosome into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    chromosome (chromosome): :ref:`chromosome <chromosome_class>` object
  
  Returns:
    int: chromosome id
  """
  cur = conn.cursor()

  # See if data has already been inserted, and if so, return it
  SQL = """SELECT chromosome_id \
          FROM chromosome \
          WHERE chromosome_name = %s AND \
                chromosome_species = %s"""
  args_tuple = (chromosome.n, chromosome.s)
  
  known_id = exists_in_database(cur, SQL, args_tuple)
  if known_id is not None:
    conn.commit()
    cur.close()
    return known_id


  SQL = """INSERT INTO chromosome (chromosome_name, chromosome_species)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        RETURNING chromosome_id;"""
  try:
    cur.execute(SQL, args_tuple)
  except pg.Error as err:
    print("%s: %s" % (err.__class__.__name__, err))
    raise
  row = cur.fetchone()
  if row is not None:
    newID = row[0]
    conn.commit()
    cur.close()
    return newID
  else:
    return None


def insert_all_chromosomes_for_species(conn, args, numChromosomes, speciesID):
  """Inserts all chromosomes for a species into database by its name

  This function inserts all chromosomes for a species into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    numChromosomes (int): upper-bound number of chromosomes to consider for a species
    species (species): :ref:`species <species_class>` object
  
  Returns:
    list of int: list of species ids
  """
  chrlist = ph.generate_chromosome_list(numChromosomes)
  insertedChromosomeIDs = []
  for chrname in chrlist:
    chrobj = chromosome(chrname, speciesID)
    insertedChromosomeID = insert_chromosome(conn, args, chrobj)
    insertedChromosomeIDs.append(insertedChromosomeID)
  return insertedChromosomeIDs


def insert_line(conn, args, line):
  """Inserts line into database

  This function inserts a line into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    line (line): :ref:`line <line_class>` object
    
  Returns:
    int: line id
  """
  cur = conn.cursor()

  # See if data has already been inserted, and if so, return it
  SQL = """SELECT line_id \
          FROM line \
          WHERE line_name = %s AND \
                line_population = %s"""
  args_tuple = (line.n, line.p)
  
  known_id = exists_in_database(cur, SQL, args_tuple)
  if known_id is not None:
    conn.commit()
    cur.close()
    return known_id

  SQL = """INSERT INTO line (line_name, line_population)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        RETURNING line_id;"""
  cur.execute(SQL, args_tuple)
  row = cur.fetchone()
  if row is not None:
    newID = row[0]
    conn.commit()
    cur.close()
    return newID
  else:
    return None


def insert_lines_from_file(conn, args, lineFile, populationID):
  """Inserts lines into database from a file

  This function inserts a lines into a database from a file


  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    lineFile (str): absolute path to input file
    populationID (int): :ref:`population <population_class>`
  
  Returns:
    list of int: list of population id
  """
  linelist = ph.parse_lines_from_file(lineFile)
  insertedLineIDs = []
  for linename in tqdm(linelist, desc="Lines"):
    lineobj = line(linename, populationID)
    insertedLineID = insert_line(conn, args, lineobj)
    insertedLineIDs.append(insertedLineID)
  return insertedLineIDs


def insert_variant(conn, args, variant):
  """Inserts variant into database

  This function inserts a variant into a database

  conn (psycopg2.extensions.connection): psycopg2 connection
  args (ArgumentParser namespace): user-defined arguments
  
  Args:
    variant (variant): :ref:`variant <variant_class>` object
  
  Returns:
    int: variant id
  """
  cur = conn.cursor()

  # See if data has already been inserted, and if so, return it
  SQL = """SELECT variant_id \
          FROM variant \
          WHERE variant_species = %s AND \
                variant_chromosome = %s AND \
                variant_pos = %s"""
  args_tuple = (variant.s, variant.c, variant.p)
  
  known_id = exists_in_database(cur, SQL, args_tuple)
  if known_id is not None:
    logging.debug(f'[Variant found] {variant}')
    conn.commit()
    cur.close()
    return known_id

  SQL = """INSERT INTO variant(variant_species, variant_chromosome, variant_pos)
        VALUES (%s,%s,%s)
        ON CONFLICT DO NOTHING
        RETURNING variant_id;"""
  cur.execute(SQL, args_tuple)
  row = cur.fetchone()
  if row is not None:
    newID = row[0]
    conn.commit()
    cur.close()
    return newID
  else:
    return None


def insert_variants_from_file(conn, args, variantPosFile, speciesID, chromosomeID):
  """Inserts chromosome into database by its name

  This function inserts a chromosome into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    variantPosFile (str): absolute path to input file
    speciesID (int): :ref:`species <species_class>`
    chromosomeID (int): :ref:`chromosome <chromosome_class>`

  Returns:
    list of int: list of variant id
  """
  variantlist = ph.parse_variants_from_file(variantPosFile)
  cVariants = len(variantlist)
  insertedVariantIDs = []
  for variantpos in tqdm(variantlist, desc="Variants from %s" % variantPosFile, total=cVariants):
    variantobj = variant(speciesID, chromosomeID, variantpos)
    insertedVariantID = insert_variant(conn, args, variantobj)
    insertedVariantIDs.append(insertedVariantID)
  return insertedVariantIDs

def insert_variants_from_file_async(conn, args, variantPosFile, speciesID, chromosomeID):
  """Insert variant position information (SNP position) using asyncpg to handle importation"""
  cred = config(args)

  # Parse input file
  # Get all of the data and pass it to the runner
  df = ph.parse_variants_from_file(variantPosFile)
  num_datapoints = len(df)
  df = [ int(d) for d in df ]
  df = list(zip(
                ([speciesID] * num_datapoints),
                ([chromosomeID] * num_datapoints),
                  df
              )
            )
  # Validation contents of input file
  # NOTE(tparker): Assumes that there is a unique SNP position for each entry
  #                Also, this means that each file can only have a SINGLE chromosome
  loop = asyncio.get_event_loop()
  validated_data = loop.run_until_complete(validate_variant_sync(args, df))
  # Perform  importation
  results = loop.run_until_complete(variant_sync_run(args, validated_data))
  logging.info(f'Imported {results} data point(s)')


def insert_genotype(conn, args, genotype):
  """Inserts genotype into database

  This function inserts a genotype into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    genotype (genotype): :ref:`genotype <genotype_class>` object

  Returns:
    int: genotype id
  """
  cur = conn.cursor()

  # See if the genotype has already been inserted, and if so, return it
  SQL = """SELECT genotype_id \
          FROM genotype \
          WHERE genotype_line = %s AND \
                genotype_chromosome = %s AND \
                genotype_genotype_version = %s"""
  params = (genotype.l, genotype.c, genotype.v)

  known_id = exists_in_database(cur, SQL, params)
  if known_id is not None:
    logging.debug(f'[Genotype found] ({genotype.l}, {genotype.c}, {genotype.v})')
    conn.commit()
    cur.close()
    return known_id

  # Otherwise, the genotype has not been inserted yet, so do so
  SQL = """INSERT INTO genotype(genotype_line,
                                genotype_chromosome,
                                genotype,
                                genotype_genotype_version)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
        RETURNING genotype_id;"""

  params = (genotype.l, genotype.c, genotype.g, genotype.v)
  try:
    cur.execute(SQL, params)
  except Exception as err:
    logging.error(f'{err} encountered while inserting genotype: {params}')
    raise
  genotype_id = cur.fetchone()[0]
  conn.commit()
  cur.close()
  return genotype_id


def insert_genotypes_from_file(conn,
                               args,
                               genotypeFile,
                               lineFile,
                               chromosomeID,
                               populationID,
                               genotype_versionID):
  """Inserts genotypes into database

  This function inserts a genotypes into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    genotypeFile (str): absolute path to input file
    lineFile (str): absolute path to input file
    chromosomeID (int): :ref:`chromosome <chromosome_class>`
    populationID (int): :ref:`population <population_class>`
  
  Returns:
    list of int: list of genotype id
  """
  genotypes = ph.parse_genotypes_from_file(genotypeFile)
  linelist = ph.parse_lines_from_file(lineFile)
  lineIDlist = ph.convert_linelist_to_lineIDlist(conn, args, linelist, populationID)
  zipped = zip(lineIDlist, genotypes)
  ziplist = list(zipped)
  insertedGenotypeIDs = []
  for zippedpair in tqdm(ziplist, desc="Genotypes from %s" % genotypeFile):
    genotypeObj = genotype(zippedpair[0], chromosomeID, zippedpair[1], genotype_versionID)
    insertedGenotypeID = insert_genotype(conn, args, genotypeObj)
    insertedGenotypeIDs.append(insertedGenotypeID)


  return insertedGenotypeIDs

def insert_growout(conn, args, growout):
  """Inserts growout into database

  This function inserts a growout into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    growout (growout): :ref:`growout <genotype_class>` object
    
  Returns:
    int: growout id
  """
  cur = conn.cursor()
  SQL = """INSERT INTO growout(growout_name, growout_population, growout_location, year, growout_growout_type)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING growout_id;"""
  args_tuple = (growout.n, growout.p, growout.l, growout.y, growout.t)
  try:
    cur.execute(SQL, args_tuple)
  except pg.Error as err:
    print("%s: %s" % (err.__class__.__name__, err))
    raise
  row = cur.fetchone()
  if row is not None:
    newID = row[0]
    conn.commit()
    cur.close()
    return newID
  else:
    return None

def insert_location(conn, args, location):
  """Inserts location into database

  This function inserts a location into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    location (location): :ref:`location <location_class>` object
  
  Returns:
    int: location id
  """
  cur = conn.cursor()
  SQL = """INSERT INTO location(country, state, city, code)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING location_id;"""
  args_tuple = (location.c, location.s, location.i, location.o)
  cur.execute(SQL, args_tuple)
  row = cur.fetchone()
  if row is not None:
    newID = row[0]
    conn.commit()
    cur.close()
    return newID
  else:
    return None  

def insert_phenotype(conn, args, phenotype):
  """Inserts phenotype into database

  This function inserts a phenotype into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    phenotype (phenotype): :ref:`phenotype <phenotype_class>` object
    
  Returns:
    int: phenotype id
  """
  cur = conn.cursor()

  # See if data has already been inserted, and if so, return it
  SQL = """SELECT phenotype_id \
          FROM phenotype \
          WHERE phenotype_line = %s AND \
                phenotype_trait = %s AND \
                LOWER(phenotype_value) = %s""" # Lower needed to deal with SQL's representation of NaN ('NaN') and Python's ('nan')
  args_tuple = (phenotype.l, phenotype.t, phenotype.v)
  
  known_id = exists_in_database(cur, SQL, args_tuple)
  if known_id is not None:
    logging.debug(f'[Phenotype found] {phenotype}')
    conn.commit()
    cur.close()
    return known_id

  SQL = """INSERT INTO phenotype(phenotype_line, phenotype_trait, phenotype_value)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING phenotype_id;"""
  try:
    cur.execute(SQL, args_tuple)
  except pg.Error as err:
    print("%s: %s" % (err.__class__.__name__, err))
    raise
  row = cur.fetchone()
  if row is not None:
    newID = row[0]
    conn.commit()
    cur.close()
    return newID
  else:
    return None

def insert_phenotypes_from_file(conn, args, phenotype_filename, population_id, filename):
  """Inserts phenotypes into database

  This function inserts phenotypes from a file into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    phenotype_filename (str): absolute path to input file
    population_id (int): :ref:`population_id <population_class>`

  Returns:
    list of int: list of phenotype id
  """
  # Read through just the first column of the CSV, ignoring any column
  df = pd.read_csv(phenotype_filename, index_col=0)
  phenotype_ids = []
  pbar = tqdm(total = (df.shape[0] * df.shape[1]), desc=f"Phenotypes from {filename}") 
  for key, value in df.iteritems():
    trait_id = find.find_trait(conn, args, key)
    for line_name, traitval in value.iteritems():
      line_id = find.find_line(conn, args, line_name, population_id)
      if line_id is None:
        l = line(line_name, population_id)
        line_id = insert_line(conn, args, l)
      ph = phenotype(line_id, trait_id, traitval)
      phenotype_id = insert_phenotype(conn, args, ph)
      phenotype_ids.append(phenotype_id)
      pbar.update(1)
  pbar.close()
  return phenotype_ids


def insert_trait(conn, args, trait):
  """Inserts trait into database

  This function inserts a trait into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    trait (trait): :ref:`trait <trait_class>` object

  Returns:
    int: trait id
  """
  cur = conn.cursor()

  # See if data has already been inserted, and if so, return it
  SQL = """SELECT trait_id \
          FROM trait \
          WHERE trait_name = %s"""
  arg = (trait.n,)
  
  known_id = exists_in_database(cur, SQL, arg)
  if known_id is not None:
    logging.debug(f'[Trait found] {trait}')
    conn.commit()
    cur.close()
    return known_id

  SQL = """INSERT INTO trait(trait_name)
        VALUES (%s)
        ON CONFLICT DO NOTHING
        RETURNING trait_id;"""
  cur.execute(SQL, arg)
  row = cur.fetchone()
  if row is not None:
    newID = row[0]
    conn.commit()
    cur.close()
    return newID
  else:
    return None


def insert_traits_from_traitlist(conn, args, traitlist, filename):
  """Inserts traits from list into database

  This function inserts a traitlist into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    traitlist (list of str): list of trait names
  
  Returns:
    list of int: list of trait id
  """
  traitIDs = []
  for traitname in tqdm(traitlist, desc=f"Traits from {filename}"):
    traitObj = trait(traitname, None, None, None)
    insertedTraitID = insert_trait(conn, args, traitObj)
    traitIDs.append(insertedTraitID)
  return traitIDs


def insert_growout_type(conn, args, growout_type):
  """Inserts growout type into database

  This function inserts a growout type into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    growout_type (growout_type): :ref:`growout_type <growout_type_class>` object

  Returns:
    int: growout type id
  """
  cur = conn.cursor()
  SQL = """INSERT INTO growout_type(growout_type)
        VALUES (%s)
        ON CONFLICT DO NOTHING
        RETURNING growout_type_id;"""
  arg = (growout_type.t,)
  cur.execute(SQL, arg)
  row = cur.fetchone()
  if row is not None:
    newID = row[0]
    conn.commit()
    cur.close()
    return newID
  else:
    return None


def insert_gwas_algorithm(conn, args, gwas_algorithm):
  """Inserts GWAS algorithm into database

  This function inserts a GWAS algorithm into a database


  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    gwas_algorithm (gwas_algorithm): :ref:`gwas_algorithm <gwas_algorithm_class>` object
  
  Returns:
    int: gwas algorithm id
  """
  cur = conn.cursor()

  # See if data has already been inserted, and if so, return it
  SQL = """SELECT gwas_algorithm_id \
          FROM gwas_algorithm \
          WHERE gwas_algorithm = %s"""
  args = (gwas_algorithm.a,)
  
  known_id = exists_in_database(cur, SQL, args)
  if known_id is not None:
    logging.debug(f'[Genotype algorithm found] {gwas_algorithm}')
    conn.commit()
    cur.close()
    return known_id

  SQL = """INSERT INTO gwas_algorithm(gwas_algorithm)
        VALUES (%s)
        ON CONFLICT DO NOTHING
        RETURNING gwas_algorithm_id;"""
  cur.execute(SQL, args)
  row = cur.fetchone()
  if row is not None:
    newID = row[0]
    conn.commit()
    cur.close()
    return newID
  else:
    return None


def insert_genotype_version(conn, args, genotype_version):
  """Inserts genotype version into database

  This function inserts a genotype version into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    genotype_version (genotype_version): :ref:`genotype_version <genotype_version_class>` object

  Returns:
    int: genotype version id
  """
  cur = conn.cursor()

  # See if data has already been inserted, and if so, return it
  SQL = """SELECT genotype_version_id \
          FROM genotype_version \
          WHERE genotype_version_assembly_name = %s AND \
                genotype_version_annotation_name = %s AND \
                reference_genome = %s AND \
                genotype_version_population = %s"""
  params = (genotype_version.n, genotype_version.v, genotype_version.r, genotype_version.p)
  
  known_id = exists_in_database(cur, SQL, params)
  if known_id is not None:
    conn.commit()
    cur.close()
    return known_id

  SQL = """INSERT INTO genotype_version(genotype_version_assembly_name,
                                        genotype_version_annotation_name,
                                        reference_genome,
                                        genotype_version_population)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
        RETURNING genotype_version_id;"""
  
  logging.debug(f'Inserting genotype version\n==========================\nSQL:\t{pformat(SQL)}\nParameters:\t{pformat(params)}')
  cur.execute(SQL, params)
  row = cur.fetchone()
  if row is not None:
    newID = row[0]
    conn.commit()
    cur.close()
    return newID
  else:
    logging.error(f'Failed to insert genotype version: {params}')
    return None


def insert_imputation_method(conn, args, imputation_method):
  """Inserts imputation method into database

  This function inserts a imputation method into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    imputation_method (imputation_method): :ref:`imputation_method <imputation_method_class>` object
  
  Returns:
    int: imputation method id
  """
  cur = conn.cursor()

  # See if data has already been inserted, and if so, return it
  SQL = """SELECT imputation_method_id \
          FROM imputation_method \
          WHERE imputation_method = %s"""
  args = (imputation_method.m,)
  
  known_id = exists_in_database(cur, SQL, args)
  if known_id is not None:
    logging.debug(f'[Imputation method found] {imputation_method}')
    conn.commit()
    cur.close()
    return known_id

  SQL = """INSERT INTO imputation_method(imputation_method)
        VALUES (%s)
        ON CONFLICT DO NOTHING
        RETURNING imputation_method_id;"""
  cur.execute(SQL, args)
  row = cur.fetchone()
  if row is not None:
    newID = row[0]
    conn.commit()
    cur.close()
    return newID
  else:
    return None


def insert_kinship_algorithm(conn, args, kinship_algorithm):
  """Inserts kinship_algorithm into database

  This function inserts a kinship_algorithm into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    kinship_algorithm (kinship_algorithm): :ref:`kinship_algorithm <kinship_algorithm_class>` object

  Returns:
    int: kinship algorithm id
  """
  cur = conn.cursor()

  # See if data has already been inserted, and if so, return it
  SQL = """SELECT kinship_algorithm_id \
          FROM kinship_algorithm \
          WHERE kinship_algorithm = %s"""
  args = (kinship_algorithm.a,)
  
  known_id = exists_in_database(cur, SQL, args)
  if known_id is not None:
    logging.debug(f'[Kinship algorithm found] {kinship_algorithm}')
    conn.commit()
    cur.close()
    return known_id

  SQL = """INSERT INTO kinship_algorithm(kinship_algorithm)
        VALUES (%s)
        ON CONFLICT DO NOTHING
        RETURNING kinship_algorithm_id;"""
  cur.execute(SQL, args)
  row = cur.fetchone()
  if row is not None:
    newID = row[0]
    conn.commit()
    cur.close()
    return newID
  else:
    return None


def insert_kinship(conn, args, kinship):
  """Inserts kinship into database

  This function inserts a kinship into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    kinship (kinship): :ref:`kinship <kinship_class>` object
    
  Returns:
    int: kinship id
  """
  cur = conn.cursor()

  # See if data has already been inserted, and if so, return it
  SQL = """SELECT kinship_id \
          FROM kinship \
          WHERE kinship_algorithm = %s AND \
                kinship_file_path = %s"""
  args = (kinship.a, kinship.p)
  
  known_id = exists_in_database(cur, SQL, args)
  if known_id is not None:
    logging.debug(f'[Kinship found] {kinship}')
    conn.commit()
    cur.close()
    return known_id

  SQL = """INSERT INTO kinship(kinship_algorithm, kinship_file_path)
        VALUES (%s,%s)
        ON CONFLICT DO NOTHING
        RETURNING kinship_id;"""
  cur.execute(SQL, args)
  row = cur.fetchone()
  if row is not None:
    newID = row[0]
    conn.commit()
    cur.close()
    return newID
  else:
    return None


def insert_population_structure_algorithm(conn, args, population_structure_algorithm):
  """Inserts population_structure_algorithm into database

  This function inserts a population_structure_algorithm into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    population_structure_algorithm (population_structure_algorithm): :ref:`population_structure_algorithm <population_structure_algorithm_class>` object
  
  Returns:
    int: population structure algorithm id
  """
  cur = conn.cursor()
  # See if data has already been inserted, and if so, return it
  SQL = """SELECT population_structure_algorithm_id \
          FROM population_structure_algorithm \
          WHERE population_structure_algorithm = %s"""
  args = (population_structure_algorithm.a,)
  
  known_id = exists_in_database(cur, SQL, args)
  if known_id is not None:
    logging.debug(f'[Population structure algorithm found] {population_structure_algorithm}')
    conn.commit()
    cur.close()
    return known_id
  
  SQL = """INSERT INTO population_structure_algorithm(population_structure_algorithm)
        VALUES (%s)
        ON CONFLICT DO NOTHING
        RETURNING population_structure_algorithm_id;"""
  cur.execute(SQL, args)
  row = cur.fetchone()
  if row is not None:
    newID = row[0]
    conn.commit()
    cur.close()
    return newID
  else:
    return None


def insert_population_structure(conn, args, population_structure):
  """Inserts population into database

  This function inserts a population into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    population (population): :ref:`population <population_class>` object
  
  Returns:
    int: population id
  """
  cur = conn.cursor()

  # See if data has already been inserted, and if so, return it
  SQL = """SELECT population_structure_id \
          FROM population_structure \
          WHERE population_structure_algorithm = %s AND \
                population_structure_file_path = %s"""
  args = (population_structure.a, population_structure.p)
  
  known_id = exists_in_database(cur, SQL, args)
  if known_id is not None:
    logging.debug(f'[Population structure found] {population_structure}')
    conn.commit()
    cur.close()
    return known_id

  SQL = """INSERT INTO population_structure(population_structure_algorithm, population_structure_file_path)
        VALUES (%s,%s)
        ON CONFLICT DO NOTHING
        RETURNING population_structure_id;"""
  cur.execute(SQL, args)
  row = cur.fetchone()
  if row is not None:
    newID = row[0]
    conn.commit()
    cur.close()
    return newID
  else:
    return None


def insert_gwas_run(conn, args, gwas_run):
  """Inserts gwas_run into database

  This function inserts a gwas_run into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    gwas_run (gwas_run): :ref:`gwas_run <gwas_run_class>` object
  
  Returns:
    int: gwas run id
  """
  cur = conn.cursor()

  # See if data has already been inserted, and if so, return it
  SQL = """SELECT gwas_run_id \
          FROM gwas_run \
          WHERE gwas_run_trait = %s AND \
                nsnps = %s AND \
                nlines = %s AND \
                gwas_run_gwas_algorithm = %s AND \
                gwas_run_genotype_version = %s AND \
                missing_snp_cutoff_value = %s AND \
                missing_line_cutoff_value = %s AND \
                minor_allele_frequency_cutoff_value = %s AND \
                gwas_run_imputation_method = %s AND \
                gwas_run_kinship = %s AND \
                gwas_run_population_structure = %s"""
  params = (gwas_run.t,
            gwas_run.s,
            gwas_run.l,
            gwas_run.a,
            gwas_run.v,
            gwas_run.m,
            gwas_run.i,
            gwas_run.n,
            gwas_run.p,
            gwas_run.k,
            gwas_run.o)
  logging.debug('GWAS Run Insertion Parameters')
  logging.debug(params)
  
  known_id = exists_in_database(cur, SQL, params)
  if known_id is not None:
    logging.debug(f'[GWAS run found] {gwas_run}')
    conn.commit()
    cur.close()
    return known_id

  SQL = """INSERT INTO gwas_run(gwas_run_trait, \
                                nsnps, \
                                nlines, \
                                gwas_run_gwas_algorithm, \
                                gwas_run_genotype_version, \
                                missing_snp_cutoff_value, \
                                missing_line_cutoff_value, \
                                minor_allele_frequency_cutoff_value, \
                                gwas_run_imputation_method, \
                                gwas_run_kinship, \
                                gwas_run_population_structure)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING gwas_run_id;"""
  cur.execute(SQL, params)
  row = cur.fetchone()
  if row is not None:
    newID = row[0]
    conn.commit()
    cur.close()
    return newID
  else:
    return None


def insert_gwas_runs_from_gwas_results_file(conn, args, gwas_results_file, gwasRunAlgorithmID, gwasRunGenotypeVersionID, missing_snp_cutoff_value, missing_line_cutoff_value, minor_allele_frequency_cutoff_value, gwasRunImputationMethodID, gwasRunKinshipID, gwasRunPopulationStructureID):
  """Inserts a collection of GWAS runs from an input file into database

  This function inserts a a collection of GWAS runs from an input file into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    gwas_results_file (str): absolute path to input file
    gwasRunAlgorithmID (int): :ref:`gwas_algorithm_id <gwas_algorithm_class>`
    gwasRunGenotypeVersionID (int): :ref:`genotype_version_id <genotype_version_class>`
    missing_snp_cutoff_value (numeric): 
    missing_line_cutoff_value (numeric):
    minor_allele_frequency_cutoff_value (numeric):
    gwasRunImputationMethodID (int): :ref:`imputation_method_id <imputation_method_class>`
    gwasRunKinshipID (int): :ref:`kinship_id <kinship_class>`
    gwasRunPopulationStructureID (int): :ref:`population_structure_id <population_structure_class>`
  Returns:
    list of int: GWAS Run IDs
  """
  gwas_run_list = ph.parse_unique_runs_from_gwas_results_file(gwas_results_file)
  insertedGwasRunIDs = []
  for gwas_run_item in tqdm(gwas_run_list, desc="GWAS Runs"):
    traitID = find.find_trait(conn, args, gwas_run_item['trait'])
    gwas_run_obj = gwas_run(traitID,
                            gwas_run_item['nSNPs'],
                            gwas_run_item['nLines'],
                            gwasRunAlgorithmID,
                            gwasRunGenotypeVersionID,
                            missing_snp_cutoff_value,
                            missing_line_cutoff_value,
                            minor_allele_frequency_cutoff_value,
                            gwasRunImputationMethodID,
                            gwasRunKinshipID,
                            gwasRunPopulationStructureID)
    insertedGwasRunID = insert_gwas_run(conn, args, gwas_run_obj)
    insertedGwasRunIDs.append(insertedGwasRunID)
  return insertedGwasRunIDs


def insert_gwas_result(conn, args, gwas_result):
  """Inserts gwas_result into database

  This function inserts a gwas_result into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    gwas_result (gwas_result): :ref:`gwas_result <gwas_result_class>` object
  
  Returns:
    int: GWAS result id
  """
  cur = conn.cursor()
  SQL = """INSERT INTO gwas_result(gwas_result_chromosome, \
                                   basepair, \
                                   gwas_result_gwas_run, \
                                   pval, \
                                   cofactor, \
                                   _order, \
                                   null_pval, \
                                   model_added_pval, \
                                   model, \
                                   pcs)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING gwas_result_id;"""
  params = (gwas_result.c,
          gwas_result.b,
          gwas_result.r,
          gwas_result.p,
          gwas_result.o,
          gwas_result.d,
          gwas_result.n,
          gwas_result.a,
          gwas_result.m,
          gwas_result.s)
  try:
    cur.execute(SQL, params)
  except Exception as err:
    logging.error(f"{err} for GWAS result insertion for {pformat(params)}")
    raise
  row = cur.fetchone()
  if row is not None:
    newID = row[0]
    conn.commit()
    cur.close()
    return newID
  else:
    return None


def insert_gwas_results_from_file(conn,
                                  args,
                                  speciesID,
                                  gwas_results_file,
                                  gwas_algorithm_ID,
                                  missing_snp_cutoff_value,
                                  missing_line_cutoff_value,
                                  imputationMethodID,
                                  genotypeVersionID,
                                  kinshipID,
                                  populationStructureID,
                                  minor_allele_frequency_cutoff_value):
  """Inserts a collection of GWAS results from a file into database

  This function inserts a collection of GWAS results from a file into a database

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    args (ArgumentParser namespace): user-defined arguments
    speciesID (int): :ref:`species_id <species_class>`
    gwas_results_file (str): absolute path to input file
    gwas_algorithm_ID (int): :ref:`gwas_algorithm_id <gwas_algorithm_class>`
    missing_snp_cutoff_value (numeric):
    missing_line_cutoff_value (numeric):`
    imputationMethodID (int): :ref:`imputation_method_id <imputation_method_class>`
    genotypeVersionID (int): :ref:`genotype_version_id <genotype_version_class>`
    kinshipID (int): :ref:`kinship_id <kinship_class>`
    populationStructureID (int): :ref:`population_structure_id <population_structure_class>`
    minor_allele_frequency_cutoff_value (numeric):

  Returns:
    list of int: GWAS Result IDs
  """
  new_gwas_result_IDs = []
  df = pd.read_csv(gwas_results_file)
  for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="GWAS Results"):
    trait = row['trait']
    # In case either nSNPs or nLines are not defined, make them none
    nSNPs = None
    nLines = None
    if 'nSNPs' in df.columns:
      nSNPs = row['nSNPs']
    if 'nLines' in df.columns:
      nLines = row['nLines']

    traitID = find.find_trait(conn, args, trait)
    gwas_run_ID = find.find_gwas_run(conn = conn, 
                                     args = args,
                                     gwas_algorithm = gwas_algorithm_ID,
                                     missing_snp_cutoff_value = missing_snp_cutoff_value,
                                     missing_line_cutoff_value = missing_line_cutoff_value,
                                     gwas_run_imputation_method = imputationMethodID,
                                     gwas_run_trait = traitID,
                                     nsnps = nSNPs,
                                     nlines = nLines,
                                     gwas_run_genotype_version = genotypeVersionID,
                                     gwas_run_kinship = kinshipID,
                                     gwas_run_population_structure = populationStructureID,
                                     minor_allele_frequency_cutoff_value = minor_allele_frequency_cutoff_value)

    logging.debug("Found run ID: %s", gwas_run_ID)

    if 'chr' in df.columns:
      chromosome = row['chr']
    elif 'chrom' in df.columns:
      chromosome = row['chrom']
    chromosome = "chr"+str(chromosome)
    chromosomeID = find.find_chromosome(conn, args, chromosome, speciesID)
    
    # Set Position
    snp = None
    if 'SNP' in df.columns:
      snp = row['SNP']
    if 'bp' in df.columns:
      basepair = row['bp']
    elif 'basepair' in df.columns:
      basepair = row['basepair']
    elif 'pos' in df.columns:
      basepair = row['pos']
    elif 'SNP' in df.columns:
      basepair = row['SNP']
    else:
      raise Exception(f'Cannot identify basepair column in GWAS result file: {gwas_results_file}')
    
    if 'PCs' in df.columns:
      pcs = row['PCs']
      if type(pcs) == str:
        pcs_list = pcs.split(":")
        pcs_list = [int(x) for x in pcs_list]
      elif np.isnan(pcs):
        pcs_list = None
    else:
      pcs_list = None

    # Assume the data producer did not provide the data
    # NOTE(tparker): I want to replace this with more flexible code that does
    # not restrict the user's spelling of the column names
    model = None
    modelAddedPval = None
    pval = None
    nullPval = None
    cofactor = None
    order = None

    if 'model' in df.columns:
      model = row['model']
    if 'modelAddedPval' in df.columns:
      modelAddedPval = row['modelAddedPval']
    if 'pval' in df.columns:
      pval = row['pval']
    if 'nullPval' in df.columns:
      nullPval = row['nullPval']
    if 'cofactor' in df.columns:
      cofactor = row['cofactor']
    if 'order' in df.columns:
      order = row['order']

    new_gwas_result = gwas_result(gwas_result_chromosome = chromosomeID,
                                  basepair = basepair,
                                  gwas_result_gwas_run = gwas_run_ID,
                                  pval = pval,
                                  cofactor = cofactor,
                                  _order = order,
                                  null_pval = nullPval,
                                  model_added_pval = modelAddedPval,
                                  model = model,
                                  pcs = pcs_list)

    new_gwas_result_ID = insert_gwas_result(conn, args, new_gwas_result)
    new_gwas_result_IDs.append(new_gwas_result_ID)
  return new_gwas_result_IDs

# Asynchronous variants insertion

async def variant_sync_run(args, data):
  cred = config(args)
  conn = await asyncpg.connect(host=cred['host'],
                               port=cred['port'],
                               user=cred['user'],
                               password=cred['password'],
                               database=cred['database'])
  # Copy all of the validated SNP postion data into the variant table
  # This does not perform any validation against the contents of the
  # database, so it will fail if any constraints are violated or duplicate
  # information is attempted to be imported.
  # Any data that arrives to this point must be preprocessed and validated
  # by other means.
  result = await conn.copy_records_to_table(
    'variant', columns=['variant_species', 'variant_chromosome', 'variant_pos'], records=data
  )

  await conn.close()

  # Return the number of data points that were imported
  # This should match the number of unique entries in the input file (line count)
  return int(result.split(' ')[1])
