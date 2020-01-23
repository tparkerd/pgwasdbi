"""Truncate the QA database"""

import logging
import time

import numpy as np
import pandas as pd
import psycopg2 as pg
from tqdm import tqdm

from importation.util.dbconnect import connect


def truncate(args):
  """This function resets by truncating all tables in the QA server
  
  Tables affected:
    growout_type
    line
    location
    growout
    variant
    genotype_version
    genotype
    kinship_algorithm
    phenotype
    trait
    population_structure_algorithm
    gwas_algorithm
    kinship
    population_structure
    gwas_run
    gwas_result
    tissue
    chromosome
    species
    population
    imputation_method
  
  """
  def exec(conn, args, stmt):
    """Remove all entries in a table with truncation

    This function removes all data in a table

    Args:
      conn (psycopg2.extensions.connection): psycopg2 connection
      args (ArgumentParser namespace): user-defined arguments
      SQL statement (string): truncation command
    """
    cur = conn.cursor()
    try:
      cur.execute(stmt)
    except:
      raise
    finally:
      conn.commit()
      cur.close()

  conn = connect(args)
  
  tables = [ 'growout_type',
             'line',
             'location',
             'growout',
             'variant',
             'genotype_version',
             'genotype',
             'kinship_algorithm',
             'phenotype',
             'trait',
             'population_structure_algorithm',
             'gwas_algorithm',
             'kinship',
             'population_structure',
             'gwas_run',
             'gwas_result',
             'tissue',
             'chromosome',
             'species',
             'population',
             'imputation_method' ]

  # Since table names cannot be passed as parameters to a prepared statement,
  # I had to construct each truncate statement by hardcoding them
  sql_stmts = [ f"TRUNCATE TABLE {t} CASCADE" for t in tables ]
  logging.debug(sql_stmts)

  for stmt in sql_stmts:
    logging.info(f"Executing: {stmt}")
    exec(conn, args, stmt)