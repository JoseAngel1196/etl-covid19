import os
import psycopg2
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)

param_dic = {
    "host": os.environ['HOST'],
    "database": os.environ['DATABASE'],
    "user": os.environ['USER'],
    "password": os.environ['PASSWORD']
}

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        logger.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**param_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
    logger.info('Connection succesfully')
    return conn

def create_table(conn):
    """ Create records table if not exist """
    cursor = conn.cursor()
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {os.environ['TABLE']} (date date PRIMARY KEY, cases integer, deaths integer, recovered numeric)")

def get_total_records(conn):
    """ Get the total records """
    cursor = conn.cursor()
    cursor.execute(f"SELECT CASE WHEN EXISTS (SELECT * FROM public.{os.environ['TABLE']} LIMIT 1) THEN 1 ELSE 0 END")
    return cursor.fetchone()

def count_records(cursor):
    """ Count how many records are in the db """
    cursor.execute(f"SELECT * from public.{os.environ['TABLE']}")
    return cursor.rowcount

def get_last_record(cursor):
    """ Get last record from records """
    cursor.execute(f"SELECT max(date) from public.{os.environ['TABLE']}")
    return cursor.fetchone()

def create_temporary_table(cursor):
    """ Create temporary table """
    cursor.execute(f"CREATE TEMPORARY TABLE {os.environ['TEMPORARY_TABLE']} as (SELECT * FROM public.{os.environ['TABLE']} limit 0)")

def insert_records(cursor):
    """ INSERT RECORDS FROM TEMPORARY TABLE TO RECORDS """
    cursor.execute(f"INSERT INTO public.{os.environ['TABLE']} SELECT {os.environ['TEMPORARY_TABLE']}.* FROM {os.environ['TEMPORARY_TABLE']} LEFT JOIN {os.environ['TABLE']} USING (date) WHERE {os.environ['TABLE']}.date IS NULL")
    cursor.execute(f"DROP TABLE {os.environ['TEMPORARY_TABLE']}")