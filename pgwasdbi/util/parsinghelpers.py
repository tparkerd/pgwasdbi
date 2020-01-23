import csv
import logging

import pandas as pd

import importation.util.find as find


def generate_chromosome_list(numChromosomes):
  """Generates list of chromosomes from numeric list of chromsomes

    Create a list of chromosome names in the format "Chr#" for specified number of chromosomes

    :param numChromosomes: list of chromosomes as integers
    :type numChromosomes: list of integer
    :return: list of chromsome numbers prepended with ``"chr"``. Example ``10`` -> ``"chr10"``
    :rtype: list of string

  """
  chrlist = []
  for count in range(1,numChromosomes+1):
    chrname = 'chr'+str(count)
    chrlist.append(chrname)
  return chrlist

def parse_lines_from_file(lineFile):
  """Parses each line from a file

  Removes trailing whitespace from each line

  :param lineFile: input file
  :type lineFile: string
  :return: list of names of lines
  :rtype: list of string

  Expected input file format is a newline-delimited file
    .. code-block:: bash
      
      282set_33-16
      282set_38-11Goodman-Buckler
      282set_4226
      282set_4722
      282set_A188
      282set_A214NGoodman-Buckler
      282set_A239
      282set_A441-5
      282set_A554
      282set_A556
  """
  linelist = []
  with open(lineFile) as f:
    rawlines = f.readlines()
    for linename in rawlines:
      linename = linename.rstrip()
      linelist.append(linename)
  return linelist

def convert_linelist_to_lineIDlist(conn, args, linelist, populationID):
  """Converts list of named lines to list of line IDs

  :param linelist:
  :type linelist: 
  :param populationID:
  :type populationID: integer
  :return: list of line IDs
  :rtype: list

  """
  lineIDlist = []
  for linename in linelist:
    lineID = find.find_line(conn, args, linename, populationID)
    lineIDlist.append(lineID)
  return lineIDlist

def parse_variants_from_file(variantPosFile):
  """Converts a newline-delimtied list of variant positions into a list, ignoring the first column of data

  :param variantPosFile: file path for variant positions within a chromosome
  :type variantPosFile: string
  :return: list of variant positions (integers)
  :rtype: list

  .. note::
    It seems like the first column is the chromosome number that the variant is found on, and the second column is that actual position on the chromosome

  """
  with open(variantPosFile) as f:
    variantReader = csv.reader(f, delimiter='\t')
    variantlist = []
    for variant in variantReader:
      variantlist.append(variant[1])
  return variantlist

def parse_genotypes_from_file(genotypeFile):
  """Converts a newline-delimited file of genotypes (for a given chromosome) to a list of allele calls

  :param genotypeFile: file path for genotype information on allele calls, ignoring the first column since it is an index term
  :type genotypeFile: string
  :return: list of allele calls (integers)
  :rtype: list

  """
  rawGenos = []
  # print("Parsing genotypes from file")
  with open(genotypeFile) as f:
    genoReader = csv.reader(f, delimiter='\t')
    for item in genoReader:
      # Remove first item, which is an index term
      item.pop(0)
      # Convert all genotype values to integers
      for i in range(len(item)):
        item[i] = int(item[i])
      rawGenos.append(item)

  return rawGenos

def parse_unique_runs_from_gwas_results_file(filepath):
  """Convert :abbr:`GWAS(Genome-wide association studies)` run from file to 
  
  :param genotypeFile: file path for GWAS run
  :type genotypeFile: string
  :return: list of unique gwas runs (list)
  :rtype: list

  .. note::
    I'm still not sure what the difference is between the gwas run and the gwas result. I assume that the run is before the result, but which one is the what Greg generates?


  Expected input file format is a newline-delimited file, containing comma-delimited entries
    .. code-block:: bash
      
      "SNP","pval","cofactor","order","nullPval","modelAddedPval","model","trait","nSNPs","nLines","PCs"
      "4_217880534",0.0000000034623340525349,1,1,0.000000283021468328469,0.000000283021468328469,"MaxCof","As75_lmResid_PU09",31,229,NA
      "7_148696418",0.000000705269070262516,1,2,0.0000203360216625424,0.000000705269070262506,"MaxCof","As75_lmResid_PU09",31,229,NA
      "4_217880534",0.0000000034623340525349,1,1,0.000000283021468328469,0.000000283021468328469,"Mbonf","As75_lmResid_PU09",31,229,NA
      "7_148696418",0.000000705269070262516,1,2,0.0000203360216625424,0.000000705269070262506,"Mbonf","As75_lmResid_PU09",31,229,NA
      "4_217880534",0.0000000034623340525349,1,1,0.000000283021468328469,0.000000283021468328469,"ExtBIC","As75_lmResid_PU09",31,229,NA
      "7_148696418",0.000000705269070262516,1,2,0.0000203360216625424,0.000000705269070262506,"ExtBIC","As75_lmResid_PU09",31,229,NA
      "2_42047577",0.000000368637213827897,1,1,0.000000368637213827897,0.000000368637213827897,"MaxCof","Co59_lmResid_NY06",15,115,NA
      "2_42047577",0.000000368637213827897,1,1,0.000000368637213827897,0.000000368637213827897,"Mbonf","Co59_lmResid_NY06",15,115,NA
      "2_42047577",0.000000368637213827897,1,1,0.000000368637213827897,0.000000368637213827897,"ExtBIC","Co59_lmResid_NY06",15,115,NA

  """
  gwas_runs = []
  df = pd.read_csv(filepath)
  for index, row in df.iterrows():
    # Ignore duplicate entries based on trait, number of SNPs, and number of lines.
    
    # NOTE(tparker): Because the nSNPs & nLines are not required fields for a GWAS run/result
    #                This code is to deal with attempting to extract values that do not exist
    #                in the results/run file (.csv)
    keys_of_interest = [ 'trait', 'nSNPs', 'nLines' ]
    gwas_run = dict.fromkeys(keys_of_interest)
    for k in keys_of_interest:
      if k in df.columns:
        gwas_run[k] = row[k]
    
    if gwas_run not in gwas_runs:
      gwas_runs.append(gwas_run)
  return gwas_runs
