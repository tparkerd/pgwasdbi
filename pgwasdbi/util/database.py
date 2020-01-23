"""This module helps connect to a PostgreSQL database"""
import os
import sys

import psycopg2
from dotenv import load_dotenv
import logging


# Use the parameters in database.ini to configure the database connection
def config(args):
  """Collect credentials for connecting to database using environment variables 

    This function parsers out the PostgreSQL credentials from environment

    :return: database connection credentials
    :rtype: dict
    :raises: :exc:`FileNotFound`

    :example .env:
      .. code-block:: env
        database=database_name
        user=database_owner
        password=password
        host=localhost
        port=5432
  """
  try:
    load_dotenv(args.env.name)
  except:
    raise 
  # get section, default to postgresql
  db = {}
  required_sections = [ 'database', 'user', 'password', 'host', 'port' ]
  for section in required_sections:
    try:
      db[section] = os.getenv(section)
    except:
      raise Exception(f"Section '{section}' not found. Please define it as an environment variable.")

  return db

# Return a connection to the database
def connect(args):
  """Creates connection object to database 

    This function creates a connection object to database

    Args:
      args (ArgumentParser namespace): user-defined arguments

    Returns:
      psycopg2.extensions.connection: PostgreSQL connection object
  """

  conn = None
  try:
    # read connection parameters
    params = config(args)

    # connect to the PostgreSQL server
    logging.debug('Connecting to the PostgreSQL database.')
    conn = psycopg2.connect(**params)
    if (conn):
      logging.info("Successfully connected to PostgreSQL database.")

    # create a cursor
    cur = conn.cursor()

    logging.debug('PostgreSQL database version:')
    cur.execute('SELECT version()')

    db_version = cur.fetchone()
    logging.debug(db_version)

    # Close the connection
    cur.close()
  except (Exception, psycopg2.DatabaseError) as error:
    logging.error('Unable to connect!\n%s', error)
    raise
  
  return conn
