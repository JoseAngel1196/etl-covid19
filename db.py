import psycopg2
import logging

from decouple import config

logger = logging.getLogger()

param_dic = {
    "host": config('HOST'),
    "database": config('DATABASE'),
    "user": config('USER'),
    "password": config('PASSWORD')
}

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        logger.info('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**param_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
    logger.info('Connection succesful')
    return conn

def create_table(conn):
    """ Create records table if not exist """
    cursor = conn.cursor()
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {config('TABLE')} (date date PRIMARY KEY, cases integer, deaths integer, recovered integer)")

def get_total_records(conn):
    """ Get the total records """
    cursor = conn.cursor()
    cursor.execute(f"SELECT CASE WHEN EXISTS (SELECT * FROM public.{config('TABLE')} LIMIT 1) THEN 1 ELSE 0 END")
    return cursor.fetchone()

def count_records(conn):
    """ Count how many records are in the db """
    cursor = conn.cursor()
    cursor.execute(f"SELECT * from public.{config('TABLE')}")
    return cursor.fetchall()

def get_last_record(conn):
    """ Get last record from records """
    cursor = conn.cursor()
    cursor.execute(f"SELECT max(date) from public.{config('TABLE')}")
    return cursor.fetchone()

def create_temporary_table(conn):
    """ Create temporary table """
    cursor = conn.cursor()
    cursor.execute(f"CREATE TEMPORARY TABLE {config('TEMPORARY_TABLE')} as (SELECT * FROM {config('TABLE')} limit 0)")

def insert_records(conn):
    """ INSERT RECORDS FROM TEMPORARY TABLE TO RECORDS """
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO {config('TABLE')} (SELECT * FROM {config('TEMPORARY_TABLE')} 
                    LEFT JOIN {config('TABLE')} USING (date) WHERE {config('TABLE')}.date IS NULL)")
    cursor.execute(f"DROP TABLE {config('TEMPORARY_TABLE')}")