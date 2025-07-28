import logging
import json
import os

import boto3
import sqlalchemy as sa
    
def get_service_client(service_name):
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

def get_db_connection(secret):
    return sa.create_engine(f"postgresql+psycopg2://{secret['username']}:{secret['password']}@{secret['host']}/{secret['dbname']}")

def build_refined(redshift_client):
    queries = json.loads(os.getenv('REFINED_QUERIES'))

    print("File structures:", queries.keys())

    dynamo_db_client = get_service_client(os.getenv('AWS_DYNAMODB'))
    
    for table_name in queries.keys():
        latest_processed_date = get_last_processed_date(table_name, dynamo_db_client)

        if latest_processed_date:
            query = queries[table_name]['merge_query'].replace('$date$', latest_processed_date)
        else:
            query = queries[table_name]['merge_query'].replace('$date$', '0001-01-01')
          
        print(f"Query for {table_name}: {query}")
    
        response = redshift_client.execute_statement(
            WorkgroupName=os.getenv("AWS_REDSHIFT_WORKGROUP_NAME"),  # Redshift Serverless Workgroup ARN
            SecretArn=os.getenv("AWS_REDSHIFT_SECRET_ARN"),      # Secrets Manager ARN
            Sql=query,                                  # Your COPY or INSERT SQL
            Database=os.getenv("AWS_REDSHIFT_DB")                # DB name inside Redshift
        )

        print(f"Response for {table_name}: {response}")

    return

def build_transformed(redshift_client):
    query = json.loads(os.getenv('TRANSFORMED_QUERIES'))

    response = redshift_client.execute_statement(
        WorkgroupName=os.getenv("AWS_REDSHIFT_WORKGROUP_NAME"),  # Redshift Serverless Workgroup ARN
        SecretArn=os.getenv("AWS_REDSHIFT_SECRET_ARN"),      # Secrets Manager ARN
        Sql=query,                                  # Your COPY or INSERT SQL
        Database=os.getenv("AWS_REDSHIFT_DB")                # DB name inside Redshift
    )

    print(f"Response for: {response}")

    return

def lambda_handler(event, context):
    try:
        redshift_client = get_service_client(os.getenv('AWS_REDSHIFT_DATA_API'))

        build_refined(redshift_client)

        build_transformed(redshift_client)

        return {
            'statusCode': 200,
            'body': json.dumps("Lambda execution completed successfully")
        }

    except Exception as e:
        logging.error(f"Error during Lambda execution: {e}")

        print(traceback.format_exc())

        return {
            'statusCode': 500,
            'body': json.dumps(f"Lambda execution failed: {e}")
        }