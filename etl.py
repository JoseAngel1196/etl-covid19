import os
import pandas as pd
import psycopg2
import logging

from decouple import config

from utils import toDatetime
from db import get_total_records, get_last_record, create_temporary_table, insert_records, create_table

logger = logging.getLogger()

def extract():
    return {
        'nyc_times': pd.read_csv(config('NYC_TIMES_URL')),
        'johns_hopkins': pd.read_csv(config('JOHNS_HOPKINS_URL'))
    }

def transform(data):
    nyc_times_df = data['nyc_times']
    johns_hopkins_df = data['johns_hopkins']

    nyc_times_df['date'] = toDatetime('date', nyc_times_df)
    johns_hopkins_df['Date'] = toDatetime('Date', johns_hopkins_df)

    johns_hopkins_df = johns_hopkins_df[(johns_hopkins_df['Country/Region'] == 'US')]
    johns_hopkins_df = johns_hopkins_df[['Date', 'Recovered']]

    johns_hopkins_df.columns = [column.lower() for column in johns_hopkins_df.columns]

    covid_df = pd.merge(nyc_times_df, johns_hopkins_df, on='date')
    return covid_df

def load(conn, covid_df):
    logger.info('Create table if not exist')
    create_table(conn)
    
    total_records = get_total_records(conn)

    if total_records[0] == 0:
        bulk(conn, covid_df)
    else:
        insert(conn, covid_df)
    cursor.close()

def bulk(conn, covid_df):
    tmp_df = 'tmp/tmp_dataframe.csv'
    covid_df.to_csv(tmp_df)
    f = open(tmp_df, 'r')
    cursor = conn.cursor()
    try:
        cursor.copy_from(f, table, sep=",")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error, exc_info=True)
        os.remove(tmp_df)
        conn.rollback()
        cursor.close()
    conn.commit()
    os.remove(tmp_df)

def insert(conn, covid_df):
    last_record = get_last_record(conn)
    diff = max(covid_df['date']).date() - last_record[0]
    if diff.days > 0:
        try:
        create_temporary_table(conn)
        tmp_csv = '/tmp/table_tmp.csv'
        covid_df.to_csv(tmp_csv, index=False, header=False)
        f = open(tmp_csv, 'r')
        cursor.copy_from(f, config('TEMPORARY_TABLE'), sep=",")
        insert_records(conn)
        except:
            logger.error(error, exc_info=True)
    else:
        logging.info('Data is up to date')

