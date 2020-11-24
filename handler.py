import json
import logging

from etl import extract, transform, load
from db import connect

logger = logging.getLogger()

def app(event, context):
    response = None
    try:
        logger.info('Extract')
        data = extract()

        logger.info('Transform')  
        covid_df = transform(data)

        conn = connect()

        logger.info('Load')
        load(conn, covid_df)
        
        response = {
        "statusCode": 200,
       }
    except (Exception) as err:
        logger.error(err)
        response = {
            "statusCode": 500,
        }
    return response
