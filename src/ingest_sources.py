import datetime
import json
import os
import re
import time

import pandas as pd
import numpy as np

from aws_secretsmanager_caching import SecretCache, SecretCacheConfig
import boto3
from botocore.exceptions import ClientError
import sqlalchemy as sa

print("NumPy location:", os.__file__)

# Generate Timestamp for File Naming
timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def has_valid_structure(file_name, df):
    file_structures = json.loads(os.getenv('FILE_STRUCTURES'))

    valid = True
    for prefix in file_structures.keys():
        if file_name.startswith(prefix):
            required_columns = file_structures[prefix]['required_columns']
            valid = all(col in df.columns for col in required_columns)
    
    print(f"File {file_name} has valid structure: {valid}")

    return valid

def replace_non_printable(text):
    if isinstance(text, str):
        # Remove non-printable ASCII characters (0-31 and 127)
        return re.sub(r'[\x00-\x1F\x7F]', ' ', text)
    return text


def clean_data(df):
    if df.empty:
        print("DataFrame is empty, skipping cleaning.")
        return df
    
    # Replace non printable characters with spaces
    df = df.map(replace_non_printable)

    # Replace empty cells with NOVALUE
    df.replace(r'^\s*$', None, regex=True, inplace=True)
    df.fillna(-1, inplace=True)        

    # Convert 'created_at' to datetime
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', format='%Y-%m-%d_%H:%M:%S')

    # Drop any duplicate rows
    df.drop_duplicates(inplace=True)

    return df

    # Peform additional cleaning steps here

def get_service_client(service_name):
    if not service_name:
        raise ValueError("Environment variable for service name is not set.")

    session = boto3.session.Session()    
        
    if os.getenv('ENVIRONMENT') == 'development':
        client = session.client(
            service_name=service_name,
            region_name=os.getenv('AWS_REGION_NAME'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
    else:
        client = session.client(
            service_name=service_name,
            region_name=os.getenv('AWS_REGION_NAME')
        )
    
    return client

def get_secret(secret_name):
    try:
        client = get_service_client(os.getenv('AWS_SECRETS_MANAGER'))
        cache_config = SecretCacheConfig()
        cache = SecretCache(config=cache_config, client=client)
        secret = cache.get_secret_string(secret_id=secret_name)
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        print(f'Could not connect to client: {e}')
        raise e
    except Exception as e:
        print(f'An unexpected error occurred: {e}')
        raise e 
    
    secret = get_secret_value_response['SecretString']

    return secret

def get_secret_dict(secret_name):
    try:
        secret = get_secret(secret_name)
        return json.loads(secret)  # Convert JSON string to dictionary
    except Exception as e:
        print(f"Error retrieving secret {secret_name}: {e}")
        raise e

def get_last_processed_date(table_name, client):
    print(f"Retrieving last processed date for table: {table_name}")
    
    response = client.get_item(
        TableName=os.getenv('AWS_DYNAMODB_TABLE_NAME'),
        ConsistentRead=True,
        ProjectionExpression='processed_date',
        Key={'table_name': {'S': table_name}}
    )

    if 'Item' not in response:
        print(f"No processing date found for table: {table_name}")
        return None
    else:
        processing_date = response['Item']['processed_date']['S']

        print(processing_date)

        return processing_date

def mark_last_processed_date(table_name, timestamp, client):
    client.put_item(
        TableName=os.getenv('AWS_DYNAMODB_TABLE_NAME'),
        Item={'table_name': {'S': table_name}, 'processed_date': {'S': timestamp}}
    )

def create_query(table_name, client):
    latest_processed_date = get_last_processed_date(table_name, client)

    # Query to fetch data from the source table
    if latest_processed_date:
        query = f"SELECT * FROM {table_name} WHERE created_at > '{latest_processed_date}'::timestamp ORDER BY created_at DESC;"
    else:           
        query = f"SELECT * FROM {table_name} ORDER BY created_at DESC;"

    print(f"Query for {table_name}: {query}")

    return query

def get_db_connection(secret):
    return sa.create_engine(f"postgresql+psycopg2://{secret['username']}:{secret['password']}@{secret['host']}/{secret['dbname']}")

def upload_to_s3(conn):
    file_structures = json.loads(os.getenv('FILE_STRUCTURES'))

    print("File structures:", file_structures.keys())

    dynamo_db_client = get_service_client(os.getenv('AWS_DYNAMODB'))

    s3_client = get_service_client(os.getenv('AWS_S3'))

    for table_name in file_structures.keys():        
        file_name = f"{table_name}_{timestamp}.csv"

        print(f"Processing table: {table_name}")    

        query = create_query(table_name, dynamo_db_client)        
        
        rows = conn.execute(query)

        # Create data frame from query results
        df = pd.DataFrame(data=rows)

        # Clean the data file        
        df = clean_data(df)

        csv_file_path =  f"/tmp/{file_name}"

        # Save DataFrame to CSV
        df.to_csv(csv_file_path, index=False)

        if df.empty or not has_valid_structure(table_name, df):
            print(f"No data to process for {table_name} or invalid structure")
            s3_client.upload_file(csv_file_path, os.getenv('AWS_S3_ERROR_BUCKET_NAME'), f'{file_name}_empty_or_invalid_stricture.csv')
            continue
                    
        last_processed_date = df['created_at'].max() if not df.empty else None
        last_processed_date = str(last_processed_date) if last_processed_date else None
        print(f"Last processed date for {table_name}: {last_processed_date}")

        print(f"Data for {table_name} saved to {csv_file_path}")

        # Upload to S3
        repsonse = s3_client.upload_file(csv_file_path, os.getenv('AWS_S3_BUCKET_NAME'), os.getenv('AWS_S3_FOLDER_PATH') + '/' + file_name)
        
        print(f"Uploaded {table_name} data to S3 at {file_name}")
        print(repsonse)

        mark_last_processed_date(table_name, last_processed_date, dynamo_db_client)

def lambda_handler(event, context):
    try:
        secret = get_secret(os.getenv('AWS_SOURCE_DB_CREDENTIALS'))
        secret = json.loads(secret)  # Convert JSON string to dictionary

        conn = get_db_connection(secret)
        
        upload_to_s3(conn)

    except Exception as e:
        print(f"Error in handler: {e}")
        raise e
    
    return {
        'statusCode': 200,
        'body': json.dumps("Data ingestion completed successfully")
    }