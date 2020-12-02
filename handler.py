import logging

from etl import extract, transform, load
from db import connect

import notify as n

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)

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
        n.notify("Error in the handler")
        logger.error(err)
        response = {
            "statusCode": 500,
        }
    return response
