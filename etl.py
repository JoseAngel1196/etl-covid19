import os
import pandas as pd
import logging

from utils import to_datetime
from db import get_total_records, get_last_record, create_temporary_table, insert_records, create_table, count_records

import notify as n

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)

def extract():
    return {
        'nyc_times': pd.read_csv(os.environ['NYC_TIMES_URL']),
        'johns_hopkins': pd.read_csv(os.environ['JOHNS_HOPKINS_URL'])
    }

def transform(data):
    covid_df = None
    try:
        logger.info('Starting transformation')

        nyc_times_df = data['nyc_times']
        johns_hopkins_df = data['johns_hopkins']

        nyc_times_df['date'] = to_datetime('date', nyc_times_df)
        johns_hopkins_df['Date'] = to_datetime('Date', johns_hopkins_df)

        johns_hopkins_df = johns_hopkins_df[(johns_hopkins_df['Country/Region'] == 'US')]
        johns_hopkins_df = johns_hopkins_df[['Date', 'Recovered']]

        johns_hopkins_df.columns = [column.lower() for column in johns_hopkins_df.columns]

        covid_df = pd.merge(nyc_times_df, johns_hopkins_df, on='date')
    except (Exception) as err:
        n.notify('Error in the transformation')
        logger.error('Error in the transformation', err)
    logger.info('Transformation completed')
    return covid_df

def load(conn, covid_df):
    try:
        logger.info('Create table if not exist')
        create_table(conn)

        total_records = get_total_records(conn)
        logger.info('Total records')
        logger.info(total_records)

        if total_records[0] == 0:
            logger.info('Bulk')
            bulk(conn, covid_df)
        else:
            logger.info('Insert')
            insert(conn, covid_df)
    except (Exception) as err:
        n.notify('Error in the load process')
        logger.error('Error in the load process', err)
        cursor.close()
    logger.info('Load succesfully')

def bulk(conn, covid_df):
    tmp_df = '/tmp/tmp_dataframe.csv'
    try:
        logger.info(covid_df)
        covid_df.to_csv(tmp_df, index=False, header=False)
        f = open(tmp_df, 'r')
        cursor = conn.cursor()
        cursor.copy_from(f, os.environ['TABLE'], sep=",")
    except (Exception) as error:
        n.notify('Error in the bulk process')
        logger.error('Error in the bulk process')
        logger.error(error)
        conn.rollback()
    conn.commit()

def insert(conn, covid_df):
    cursor = conn.cursor()
    last_record = get_last_record(cursor)
    last_date_record = last_record[0]
    last_date_df = max(covid_df['date']).date()
    diff =  last_date_df - last_date_record
    if diff.days > 0:
        try:
            logger.info('Creating temporary table')
            create_temporary_table(cursor)
            tmp_csv = '/tmp/table_tmp.csv'
            covid_df.to_csv(tmp_csv, index=False, header=False)
            f = open(tmp_csv, 'r')
            cursor.copy_from(f, os.environ['TEMPORARY_TABLE'], sep=",")
            logger.info('Inserting records')
            insert_records(cursor)
            total_rows = count_records(cursor)
            n.notify(f"Number of rows inserted to the database: {diff.days}")
        except (Exception) as err:
            logger.error('Error in the insert process')
            logger.error(err)
        else:
            logger.info('Records inserted succesfully')
    else:
        n.notify('Your database is up to date')
        logger.info('Data is up to date')
    cursor.close()

