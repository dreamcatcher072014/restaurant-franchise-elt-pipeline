import logging
import json
import os
import re
import time

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
AWS_REDSHIFT_ROLE_ARN = os.getenv('AWS_REDSHIFT_ROLE_ARN')
AWS_SECRETS_MANAGER = os.getenv('AWS_SECRETS_MANAGER')
AWS_DYNAMODB = os.getenv('AWS_DYNAMODB')
AWS_REDSHIFT = os.getenv('AWS_REDSHIFT')
AWS_RDS_DATA = os.getenv('AWS_RDS_DATA')
AWS_REDSHIFT_DB='db'
AWS_REDSHIFT_WORKGROUP_ARN='arn:aws:redshift-serverless:us-east-2:925401940064:workgroup/56bee133-c0c8-45dd-b41b-5039b5aadf4e'
AWS_S3 = os.getenv('AWS_S3')
FILE_STRUCTURES = json.loads(os.getenv('FILE_STRUCTURES'))

def get_test_event():
    with open('./test/load_raw_test.json', 'r') as file:
        data = file.read()
        return json.loads(data)
    
    return None

def replace_non_printable(text):
    if isinstance(text, str):
        # Remove non-printable ASCII characters (0-31 and 127)
        return re.sub(r'[\x00-\x1F\x7F]', ' ', text)
    return text

def has_invalid_structure(file_name, df):
    valid = False
    for prefix in FILE_STRUCTURES.keys():
        if file_name.startswith(prefix):
            required_columns = FILE_STRUCTURES[prefix]['required_columns']
            valid = all(col in df.columns for col in required_columns)
    
    return valid

def get_copy_query(file_name):
    for prefix in FILE_STRUCTURES.keys():
        if file_name.startswith(prefix):
            query = FILE_STRUCTURES[prefix]['copy_query']
            s3uri = f's3://{AWS_S3_BUCKET_NAME}/{AWS_S3_FOLDER_PATH}/{file_name}'
            query = query.replace('s3uri', f'\'{s3uri}\'').replace('iamrole', f'\'{AWS_REDSHIFT_ROLE_ARN}\'')
            return query
            
    return None

def clean_data(df):
    # Replace non printable characters with spaces
    df = df.map(replace_non_printable)

    # Replace empty cells with NOVALUE
    df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    df.fillna(-1, inplace=True)        

    # Convert 'created_at' to datetime
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', format='%Y-%m-%d_%H:%M:%S')

    # Drop any duplicate rows
    df.drop_duplicates(inplace=True)

    return df

    # Peform additional cleaning steps here

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
        if ENVIRONMENT =='development':
            event = get_test_event()
    
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        file_name = object_key.split('/')[1]

        # Get the s3 file
        s3_client = get_service_client(AWS_S3)
        s3_client.download_file(bucket_name, object_key, file_name)

        df = pd.read_csv(file_name)
        
        if df.empty or has_invalid_structure(object_key, df):
            s3_client.upload_file(file_name, AWS_S3_ERROR_BUCKET_NAME, f'{file_name}_empty_or_invalid_stricture.csv')
            
            # Send file to error bucket
            return {
                'statusCode': 200,
                'body': json.dumps("No data to process or invalid structure")
            }

        # Clean the data file        
        df = clean_data(df)

        # Output the cleaned data file
        cleaned_file_name = f'{file_name}'.replace('.','_cleaned.')
        cleaned_file_path = f'./{cleaned_file_name}'
        df.to_csv(cleaned_file_path, index=False)

        s3_client.upload_file(cleaned_file_path, AWS_S3_BUCKET_NAME, AWS_S3_FOLDER_PATH + '/' + cleaned_file_name)
        
        copy_sql_query = get_copy_query(cleaned_file_name)

        redshift_client = boto3.client('redshift-data')
        response = redshift_client.execute_statement(
            Database=AWS_REDSHIFT_DB,
            Sql=copy_sql_query,
            WorkgroupName=AWS_REDSHIFT_WORKGROUP_ARN
        )

        # Delete the local file after processing
        if os.path.exists(file_name):
            os.remove(file_name)
        if os.path.exists(cleaned_file_path):
            os.remove(cleaned_file_path)

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
    # Start script timer        
    start_time = time.time()

    # Transfer raw data from source to S3
    load_raw(None, None)

    # Print script execution time
    print("--- %s minutes ---" % str((time.time() - start_time)/60))