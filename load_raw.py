import datetime
import logging
import json
import os
import time

from aws_secretsmanager_caching import SecretCache, SecretCacheConfig
import boto3
from botocore.exceptions import ClientError

import pandas as pd

# Environment Variables (Configured in Lambda)
ENVIRONMENT = os.getenv('ENVIRONMENT')
TEST = os.getenv('TEST')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY= os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DB_HOST = os.getenv('AWS_DB_HOST')
AWS_DB_CREDENTIALS = os.getenv('AWS_DB_CREDENTIALS')
AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')
AWS_DYNAMODB_TABLE_NAME = os.getenv('AWS_DYNAMODB_TABLE_NAME')
AWS_S3_FOLDER_PATH = os.getenv('AWS_S3_FOLDER_PATH')
AWS_REGION_NAME = os.getenv('AWS_REGION_NAME')
AWS_DB_NAME = os.getenv('AWS_DB_NAME') 
AWS_RDS_DATA = os.getenv('AWS_RDS_DATA')
AWS_S3 = os.getenv('AWS_S3')
AWS_SECRETS_MANAGER = os.getenv('AWS_SECRETS_MANAGER')
AWS_DYNAMODB = os.getenv('AWS_DYNAMODB')
FILE_STRUCTURES = json.load(os.getenv('FILE_STRUCTURES'))

def has_invalid_structure(file_name, df):
    valid = False
    for prefix in FILE_STRUCTURES.keys():
        if file_name.startswith(prefix):
            required_columns = FILE_STRUCTURES[prefix]
            valid = all(col in df.columns for col in required_columns)
    
    return valid

def get_test_event():
    if TEST == 'true':
        with open('./load_raw_test.json', 'r') as file:
            data = file.read()
        return json.loads(data)
    
    return None

def get_service_client(service_name):
    session = boto3.session.Session()    
        
    if ENVIRONMENT == 'development':
        client = session.client(
            service_name=service_name,
            region_name=AWS_REGION_NAME,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    else:
        client = session.client(
            service_name=service_name,
            region_name=AWS_REGION_NAME
        )
    
    return client

def load_raw(event, context):
    try:
        if TEST == 'true':
            event = get_test_event()
        
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']

        file_name = f'{object_key}.csv'

        s3_client = get_service_client(AWS_S3)
        with open(file_name, 'wb') as data:
            s3_client.download_fileobj(bucket_name, object_key, data)

        df = pd.read_csv(file_name)

        if df.empty or has_invalid_structure(object_key, df):
            # Send file to error bucket
            return {
                'statusCode': 200,
                'body': json.dumps("No data to process or invalid structure")
            }

        # Fill all NaN values with empty strings
        df.fillna('', inplace=True)

        # Delete duplicate rows
        df.drop_duplicates(inplace=True)

        # Delete duplicate rows
        df.drop_duplicates(inplace=True)

        # Delete the local file after processing
        if os.path.exists(file_name):
            os.remove(file_name)

        return {
            'statusCode': 200,
            'body': json.dumps("Lambda execution completed successfully")
        }

    except Exception as e:
        logging.error(f"Error during Lambda execution: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Lambda execution failed: {e}")
        }