import os
import boto3
import logging

sns = boto3.client('sns')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)

def notify(text):
    try:
        logger.info('Sending notification')
        sns.publish(
            TopicArn=os.environ['SNS_TOPIC'],
            Subject='Notify_etl_covid19',
            Message=text)
    except (Exception) as err:
        logger.error("Failed to send notification")
        logger.error(err)
    logger.info('Notification sent!')