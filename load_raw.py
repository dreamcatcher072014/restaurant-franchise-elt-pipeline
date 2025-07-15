import logging
import json
import os

import boto3

import numpy as np
import pandas as pd

# Environment Variables (Configured in Lambda)
ENVIRONMENT = os.getenv('ENVIRONMENT')

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY= os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION_NAME = os.getenv('AWS_REGION_NAME')
AWS_DB_NAME = os.getenv('AWS_DB_NAME')

AWS_DB_HOST = os.getenv('AWS_DB_HOST')
AWS_DB_CREDENTIALS = os.getenv('AWS_DB_CREDENTIALS')

AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')
AWS_S3_FOLDER_PATH = os.getenv('AWS_S3_FOLDER_PATH')
AWS_S3_ERROR_BUCKET_NAME = os.getenv('AWS_S3_ERROR_BUCKET_NAME')

AWS_DYNAMODB_TABLE_NAME = os.getenv('AWS_DYNAMODB_TABLE_NAME')

AWS_REDSHIFT_CREDENTIALS = os.getenv('AWS_REDSHIFT_CREDENTIALS')

AWS_SECRETS_MANAGER = os.getenv('AWS_SECRETS_MANAGER')
AWS_DYNAMODB = os.getenv('AWS_DYNAMODB')
AWS_REDSHIFT = os.getenv('AWS_REDSHIFT')
AWS_RDS_DATA = os.getenv('AWS_RDS_DATA')
AWS_S3 = os.getenv('AWS_S3')

FILE_STRUCTURES = json.load(os.getenv('FILE_STRUCTURES'))

def has_invalid_structure(file_name, df):
    valid = False
    for prefix in FILE_STRUCTURES.keys():
        if file_name.startswith(prefix):
            required_columns = FILE_STRUCTURES[prefix]['required_columns']
            valid = all(col in df.columns for col in required_columns)
    
    return valid

def get_test_event():
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

def lambda_handler(event, context):
    try:
        if ENVIRONMENT == 'development':
            event = get_test_event()
        
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']

        file_name = f'{object_key}.csv'

        s3_client = get_service_client(AWS_S3)
        with open(file_name, 'wb') as data:
            s3_client.download_fileobj(bucket_name, object_key, data)

        df = pd.read_csv(file_name)

        if df.empty or has_invalid_structure(object_key, df):
            s3_client.upload_file(f'./{file_name}', AWS_S3_ERROR_BUCKET_NAME, f'{file_name}_empty_or_invalid_stricture.csv')
            
            # Send file to error bucket
            return {
                'statusCode': 200,
                'body': json.dumps("No data to process or invalid structure")
            }

        # Replace blank values with nan
        df.replace(r'^\s*$', np.nan, regex=True, inplace=False)

        # Fill all NaN values with the string EMPTY
        df.fillna('EMPTY', inplace=True)

        # Delete duplicate rows
        df.drop_duplicates(inplace=True)

        redshift_client = boto3.client('redshift-data')

        response = redshift_client.execute_statement(
            ClientToken='string',
            ClusterIdentifier='string',
            Database='string',
            DbUser='string',
            Parameters=[
                {
                    'name': 'string',
                    'value': 'string'
                },
            ],
            ResultFormat='JSON'|'CSV',
            SecretArn='string',
            SessionId='string',
            SessionKeepAliveSeconds=123,
            Sql='string',
            StatementName='string',
            WithEvent=True|False,
            WorkgroupName='string'
        )
    
        # Delete the local file after processing
        if os.path.exists(file_name):
            os.remove(file_name)

        # Upload the processed file to the raw table in Redshift
        print(f'Loading data from {object_key} to Redshift...')

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
    
if ENVIRONMENT == 'development':
    lambda_handler(None, None)    
