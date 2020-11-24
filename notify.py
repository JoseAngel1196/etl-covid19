import boto3
import logging

sns = boto3.client('sns')

logger = logging.getLogger()

def notify(text):
    try:
        sns.publish(
            TopicArn='SNS',
            Subject='Notify_etl_covid19',
            Message=text)
    except:
        logger.error("Failed to send notification")
    logger.info('Notification sent!')