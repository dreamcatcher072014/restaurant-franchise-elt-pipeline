import datetime
import json
import os
import time

from aws_secretsmanager_caching import SecretCache, SecretCacheConfig
import boto3
from botocore.exceptions import ClientError
import sqlalchemy as sa
import pandas as pd

# Environment Variables (Configured in Lambda)
ENVIRONMENT = os.getenv('ENVIRONMENT')
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

# Generate Timestamp for File Naming
timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

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

def get_secret(secret_name):
    try:
        client = get_service_client(AWS_SECRETS_MANAGER)
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
        TableName=AWS_DYNAMODB_TABLE_NAME,
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
        TableName=AWS_DYNAMODB_TABLE_NAME,
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
    username = secret['username']
    password = secret['password']
    host = AWS_DB_HOST  
    db = AWS_DB_NAME

    engine = sa.create_engine(f'postgresql+psycopg2://{username}:{password}@{host}/{db}')

    return engine.connect()

def upload_to_s3(conn):
    source_tables = [
        'order_items',
        'order_item_options',
        'date_dim',
    ]

    dynamo_db_client = get_service_client(AWS_DYNAMODB)

    for table_name in source_tables:
        print(f"Processing table: {table_name}")    

        query = create_query(table_name, dynamo_db_client)        
        print(f"Executing query: {query}")
        
        rows = conn.execute(query)

        df = pd.DataFrame(data=rows)

        if df.empty:
            print(f"No new data found for table: {table_name}")
            continue
        
        last_processed_date = pd.to_datetime(df['created_at'].max()) if not df.empty else None
        last_processed_date = str(last_processed_date) if last_processed_date else None
        print(f"Last processed date for {table_name}: {last_processed_date}")

        # Save DataFrame to CSV
        file_name = f"{table_name}_{timestamp}.csv"
        file_path = f"./{file_name}"
        df.to_csv(file_path, index=False)

        print(f"Data for {table_name} saved to {file_path}")

        # Upload to S3
        s3_client = get_service_client(AWS_S3)
        file_name = f"{table_name}_{timestamp}.json"
        
        repsonse = s3_client.upload_file(file_path, AWS_S3_BUCKET_NAME, AWS_S3_FOLDER_PATH + '/' + file_name)
        
        print(f"Uploaded {table_name} data to S3 at {file_name}")
        print(repsonse)

        mark_last_processed_date(table_name, last_processed_date, dynamo_db_client)

def ingest_sources():
    try:
        secret = get_secret(AWS_DB_CREDENTIALS)
        secret = json.loads(secret)  # Convert JSON string to dictionary

        conn = get_db_connection(secret)
        
        upload_to_s3(conn)

    except Exception as e:
        print(f"Error in handler: {e}")
        raise e

# Start script timer        
start_time = time.time()

# Transfer raw data from source to S3
ingest_sources()

# Print script execution time
print("--- %s minutes ---" % str((time.time() - start_time)/60))