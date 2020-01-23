"""Looks up IDs of various elements in the database"""
import csv
import logging

import numpy as np
import pandas as pd
import psycopg2

import importation.util.insert
from importation.util.dbconnect import config, connect
from importation.util.models import (chromosome, genotype, genotype_version,
                                     growout, growout_type, gwas_algorithm,
                                     gwas_result, gwas_run, imputation_method,
                                     kinship, kinship_algorithm, line,
                                     location, phenotype, population,
                                     population_structure,
                                     population_structure_algorithm, species,
                                     trait, variant)


def find_species(conn, args, speciesShortname):
  """Finds species by shortname 

  This function finds species_id for a species by its shortname
  
  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    speciesShortname (str): human-readable shortname of species
  
  Returns:
    int: species id
  """
  logging.debug(f'Finding species: {speciesShortname}')
  cur = conn.cursor()
  cur.execute("SELECT species_id FROM species WHERE shortname = %s;", [speciesShortname])
  row = cur.fetchone()
  if row is not None:
    speciesID = row[0]  
    cur.close()
    return speciesID
  else:
    return None

def find_population(conn, args, populationName):
  """Finds species by population name  

  This function finds the population_id for a population by its name

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    populationName (str): human-readable name of population
  
  Returns:
    int: population id
  """
  logging.debug(f'Finding population: {populationName}')
  cur = conn.cursor()
  cur.execute("SELECT population_id FROM population WHERE population_name = %s;", [populationName])
  row = cur.fetchone()
  if row is not None:
    populationID = row[0]
    cur.close()
    return populationID
  else:
    return None

def find_chromosome(conn, args, chromosome_name, chromosome_species):
  """Finds chromosome by name and species id

    This function finds the chromosome_id for a chromosome by its name and species id

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    chromosome_name (str): abbreviation of choromosome name
    chromosome_species (int): :ref:`species id <species_class>`
  
  Returns:
    int: chromosome id
  """
  logging.debug(f'Finding chromosome: {chromosome_name}, {chromosome_species}')
  cur = conn.cursor()
  # not sure if next line is correct...
  # TODO(timp): Check if this line meets functional requirements
  sql = "SELECT chromosome_id \
               FROM chromosome \
               WHERE chromosome_name = %s \
               AND chromosome_species = %s;"
  params = (chromosome_name, chromosome_species)
  cur.execute(sql, params)
  row = cur.fetchone()
  if row is not None:
    chromosomeID = row[0]
    cur.close()
    return chromosomeID
  else:
    return None

def find_line(conn, args, line_name, line_population):
  """Finds line by its name and population name 

    This function finds the line_id for a line by its name and population id

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    line_name (str): human-readable name of line
    line_population (int): :ref:`population id <population_class>`
  
  Returns:
   int: line id
  """
  logging.debug(f'Finding line: {line_name}, {line_population}')
  cur = conn.cursor()
  cur.execute("SELECT line_id FROM line WHERE line_name = %s AND line_population = %s;", (line_name, line_population))
  row = cur.fetchone()
  if row is not None:
    lineID = row[0]
    cur.close()
    return lineID
  else:
    return None

def find_growout_type(conn, args, growout_type):
  """Finds growout type by its name  

    This function finds the growout_id for a growout type by its type

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    growout_type (str): human-readable name of growout type

  Returns:
    int: growout type id
  """
  logging.debug(f'Finding growout type: {growout_type}')
  cur = conn.cursor()
  cur.execute("SELECT growout_type_id FROM growout_type WHERE growout_type = %s;", [growout_type])
  row = cur.fetchone()
  if row is not None:
    growout_type_ID = row[0]
    cur.close()
    return growout_type_ID
  else:
    return None

def find_growout(conn, args, growout_name):
  """Finds growout ID by its name
  
  This function finds the growout_id for a growout by its name

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    growout_name (str): human-readable name of growout type
  
  Returns:
      int: growout id
  """
  logging.debug(f'Finding growout: {growout_name}')
  cur = conn.cursor()
  cur.execute(
      "SELECT growout_id FROM growout WHERE growout_name = %s;" , [growout_name])
  row = cur.fetchone()
  if row is not None:
    growout_id = row[0]
    cur.close()
    return growout_id
  else:
    return None
   
def find_location(conn, args, code):
  """Finds location by its code 

  This function finds the location_id for a location by its code

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    code (str): human-readable code assigned to a location
  
  Returns:
    int: location id
  """
  logging.debug(f'Finding location: {code}')
  cur = conn.cursor()
  cur.execute("SELECT location_id FROM location WHERE code = %s;", [code])
  row = cur.fetchone()
  if row is not None:
    location_ID = row[0]
    cur.close()
    return location_ID
  else:
    return None

def find_kinship_algorithm(conn, args, algorithm):
  """Finds kinship algorithm by algorithm name 

  This function finds the kinship_algorithm_id by its name

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    algorithm (str): human-readable name for algorithm

  Returns:
    int: kinsihp algorithm id
  """
  logging.debug(f'Finding kinship algorithm: {algorithm}')
  cur = conn.cursor()
  cur.execute("SELECT kinship_algorithm_id FROM kinship_algorithm WHERE kinship_algorithm = %s;", [algorithm])
  row = cur.fetchone()
  if row is not None:
    kinship_algorithm_ID = row[0]
    cur.close()
    return kinship_algorithm_ID
  else:
    return None

def find_population_structure_algorithm(conn, args, algorithm):
  """Finds population structure algorithm by algorithm name

  This function finds the population_structure_id by the name of its algorithm

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    algorithm (str): human-readable name for algorithm
  
  Returns:
    int: population structure algorithm id
  """
  logging.debug(f'Finding population structure algorithm: {algorithm}')
  cur = conn.cursor()
  cur.execute("SELECT population_structure_algorithm_id FROM population_structure_algorithm WHERE population_structure_algorithm = %s;", [algorithm])
  row = cur.fetchone()
  if row is not None:
    population_structure_algorithm_ID = row[0]
    cur.close()
    return population_structure_algorithm_ID
  else:
    return None

def find_gwas_algorithm(conn, args, gwas_algorithm):
  """Finds algorithm used for genome-wide association study by algorithm name

  This function finds the gwas_algorithm_id by the name of the algorithm used in a genome-wide association study

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    gwas_algorithm (str): human-readable name of a GWAS algorithm
  
  Returns:
    int: GWAS algorithm id
  """
  logging.debug(f'Finding GWAS algorithm: {gwas_algorithm}')
  cur = conn.cursor()
  cur.execute("SELECT gwas_algorithm_id FROM gwas_algorithm WHERE gwas_algorithm = %s;", [gwas_algorithm])
  row = cur.fetchone()
  if row is not None:
    gwas_algorithm_ID = row[0]
    cur.close()
    return gwas_algorithm_ID
  else:
    return None

def find_genotype_version(conn, args, genotype_version_name):
  """Finds version of genotype by name 

  This function finds the genotype_version_id of a genotype by its name

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    genotype_version_name (str): human-readable name of genotype version
  
  Returns:
    int: genotype version id
  """
  logging.debug(f'Finding genotype version: {genotype_version_name}')
  cur = conn.cursor()
  cur.execute("SELECT genotype_version_id FROM genotype_version WHERE genotype_version_name = %s;", [genotype_version_name])
  row = cur.fetchone()
  if row is not None:
    genotype_version_ID = row[0]
    cur.close()
    return genotype_version_ID
  else:
    return None

def find_imputation_method(conn, args, imputation_method):
  """Finds imputation methodo by name

  This function finds the imputation_method_id by its name
  
  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    imputation_method (str): human-readable name of method
  
  Returns:
    int: imputation method id
  """
  logging.debug(f'Finding imputation method: {imputation_method}')
  cur = conn.cursor()
  cur.execute("SELECT imputation_method_id FROM imputation_method WHERE imputation_method = %s;", [imputation_method])
  row = cur.fetchone()
  if row is not None:
    imputation_method_ID = row[0]
    cur.close()
    return imputation_method_ID
  else:
    return None

def find_kinship(conn, args, kinship_file_path):
  """Finds kinship by its location on a file system 

  This function finds the kinship_id by its location within a file system

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    kinship_file_path (str): path to kinship file
    :example path: ``/opt/BaxDB/file_storage/kinship_files/4.AstleBalding.synbreed.kinship.csv``
  
  Returns:
    int: kinship id
  """
  logging.debug(f'Finding kinship: {kinship_file_path}')
  cur = conn.cursor()
  cur.execute("SELECT kinship_id FROM kinship WHERE kinship_file_path = %s;", [kinship_file_path])
  row = cur.fetchone()
  if row is not None:
    kinship_ID = row[0]
    cur.close()
    return kinship_ID
  else:
    return None

def find_population_structure(conn, args, population_structure_file_path):
  """Finds population_structure by its location within a file system 

  This function placeholder

  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    population_structure_file_path (str): path to kinship file
    :example path: ``/opt/BaxDB/file_storage/population_structure_files/4.Eigenstrat.population.structure.10PCs.csv``
  
  Returns:
    int: population structure id
  """
  logging.debug(f'Finding population structure: {population_structure_file_path}')
  cur = conn.cursor()
  cur.execute("SELECT population_structure_id FROM population_structure WHERE population_structure_file_path = %s;", [population_structure_file_path])
  row = cur.fetchone()
  if row is not None:
    population_structure_ID = row[0]
    cur.close()
    return population_structure_ID
  else:
    return None

def find_trait(conn, args, trait_name):
  """Finds trait by its name 

  This function finds the traid_id for a trait by its name
  
  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    traitName (str): human-readable name of trait
  
  Returns:
    int: trait id
  """
  logging.debug(f'Finding trait: {trait_name}')
  cur = conn.cursor()
  cur.execute("SELECT trait_id FROM trait WHERE trait_name = %s;", [trait_name])
  row = cur.fetchone()
  if row is not None:
    trait_ID = row[0]
    cur.close()
    return trait_ID
  else:
    return None

def find_gwas_run(conn,
                  args,
                  gwas_algorithm,
                  missing_snp_cutoff_value,
                  missing_line_cutoff_value,
                  gwas_run_imputation_method,
                  gwas_run_trait,
                  nsnps,
                  nlines,
                  gwas_run_genotype_version,
                  gwas_run_kinship,
                  gwas_run_population_structure,
                  minor_allele_frequency_cutoff_value):
  """Finds GWAS run by its parameters

  This function finds the gwas_run_id by its parameters
  Args:
    conn (psycopg2.extensions.connection): psycopg2 connection
    gwas_algorithm (int): :ref:`gwas_algorithm_id <gwas_algorithm_class>`
    missing_snp_cutoff_value (numeric): ``todo``
    missing_line_cutoff_value (numeric): ``todo``
    gwas_run_imputation_method (int): :ref:`imputation_method_id <imputation_method_class>`
    gwas_run_trait (int): :ref:`traid_id <trait_class>`
    nsnps (int): ``todo``
    nlines (int): ``todo``
    gwas_run_genotype_version (int): :ref:`genotype_version_id <genotype_version_class>`
    gwas_run_kinship (int): kinship id
    gwas_run_population_structure (int): :ref:`population_structure_id <population_structure_class>`
    minor_allele_frequency_cutoff_value (numeric): ``todo``
  
  Returns:
    int: GWAS run id

  .. note::
    Needs additional information on the
      - missing_snp_cutoff_value
      - missing_line_cutoff_value
      - nsnps
      - nlines
      - minor_allele_frequency_cutoff_value

  """
  cur = conn.cursor()
  sql = """SELECT gwas_run_id
           FROM gwas_run
           WHERE gwas_run_gwas_algorithm = %s
           AND missing_snp_cutoff_value = %s
           AND missing_line_cutoff_value = %s
           AND gwas_run_imputation_method = %s
           AND gwas_run_trait = %s
           AND (nsnps = %s or nsnps is null)
           AND (nlines = %s or nlines is null)
           AND gwas_run_genotype_version = %s
           AND gwas_run_kinship = %s
           AND gwas_run_population_structure = %s
           AND minor_allele_frequency_cutoff_value = %s;"""
  params = (gwas_algorithm,
           missing_snp_cutoff_value,
           missing_line_cutoff_value,
           gwas_run_imputation_method,
           gwas_run_trait,
           nsnps,
           nlines,
           gwas_run_genotype_version,
           gwas_run_kinship,
           gwas_run_population_structure,
           minor_allele_frequency_cutoff_value)

  logging.debug(f'Finding GWAS run: {params}')

  cur.execute(sql, params)
                                                     
  row = cur.fetchone()
  if row is not None:
    gwas_run_ID = row[0]
    cur.close()
    return gwas_run_ID
  else:
    return None
