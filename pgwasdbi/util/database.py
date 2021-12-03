"""This module helps connect to a PostgreSQL database"""
import os
import sys

import psycopg2
from dotenv import load_dotenv
import logging

# Return a connection to the database
def connect(*args, **kwargs):
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
    params = kwargs

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


def connection_test(hostname="localhost",
                    password=None,
                    port=5432,
                    username=None,
                    database="pgwasdb",
                    *args,
                    **kwargs):
  connect()
  pass