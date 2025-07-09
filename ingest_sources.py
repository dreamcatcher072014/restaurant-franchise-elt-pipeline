import csv
import datetime
import json
import logging
import os

from aws_secretsmanager_caching import SecretCache, SecretCacheConfig
import boto3
from botocore.exceptions import ClientError
import pymssql
 
import pandas as pd

# Constants
AWS_RDS_DATA = 'rds-data'
AWS_S3 = 's3'
AWS_SECRETS_MANAGER = 'secretsmanager'

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment Variables (Configured in Lambda)
ENVIRONMENT = os.getenv('ENVIRONMENT')  # e.g., 'development', 'staging', 'production'
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')  # e.g.,
AWS_SECRET_ACCESS_KEY= os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DB_CREDENTIALS = os.getenv('AWS_DB_CREDENTIALS')  # e.g.,
AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')
AWS_S3_FOLDER_PATH = os.getenv('AWS_S3_FOLDER_PATH')
AWS_REGION_NAME = os.getenv('AWS_REGION_NAME')

# Generate Timestamp for File Naming
timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def get_service_client(service_name):
    if ENVIRONMENT == 'production':
        client = session.client(
            service_name=service_name,
            region_name=AWS_REGION_NAME
        )
    else:
        session = boto3.session.Session()    
        client = session.client(
            service_name=service_name,
            region_name=AWS_REGION_NAME,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
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

def handler():
    try:
        secret = get_secret(AWS_DB_CREDENTIALS)
        secret = json.loads(secret)  # Convert JSON string to dictionary

        conn = pymssql.connect('resource-franchise-data-source-2.c5ie4m22aqrp.us-east-2.rds.amazonaws.com', secret['username'], secret['password'], "restaurant_franchise_source_dev")
        cursor = conn.cursor()
        
        source_tables = [
            'order_items',
            'order_item_options',
            'date_dim',
        ]
    
        for table_name in source_tables:
            logger.info(f"Processing table: {table_name}")
            
            cursor.execute(f"SELECT TOP 10 * FROM {table_name};")
            rows = cursor.fetchall()
            
            # Go through the results row-by-row and write the output to a CSV file
            # (QUOTE_NONNUMERIC applies quotes to non-numeric data; change this to
            # QUOTE_NONE for no quotes.  See https://docs.python.org/2/library/csv.html
            # for other settings options)
            with open(f"{table_name}.csv", "w") as outfile:
                writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
                for row in cursor:
                    writer.writerow(row)

# Close the cursor and the database connection
cursor.close()
conn.close()

            df = pd.DataFrame(rows)

            for col in df.columns:
                print(col, df[col].dtype)
            

            
            # Upload to S3
            # s3_client = get_service_client(AWS_S3)
            # file_name = f"{AWS_S3_FOLDER_PATH}{table_name}_{timestamp}.json"
            # s3_client.put_object(
            #     Bucket=AWS_S3_BUCKET_NAME,
            #     Key=file_name,
            #     Body=json.dumps(records)
            # )
            # logger.info(f"Uploaded {table_name} data to S3 at {file_name}")

    except Exception as e:
        logger.error(f"Error in handler: {e}")
        raise e
    
handler()