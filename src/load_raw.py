import logging
import json
import os

import boto3

import traceback

def get_test_event():
    with open('./load_raw_test.json', 'r') as file:
        data = file.read()
        return json.loads(data)
    
    return None

def get_copy_query(file_name):
    file_structures = json.loads(os.getenv('FILE_STRUCTURES'))

    for prefix in file_structures.keys():
        if file_name.startswith(prefix):
            query = file_structures[prefix]['copy_query']
            s3uri = f"'s3://{os.getenv('AWS_S3_BUCKET_NAME')}/{os.getenv('AWS_S3_FOLDER_PATH')}/{file_name}'"
            
            query = query.replace('s3uri', s3uri)
            query = query.replace('iamrole', f"'{os.getenv('AWS_REDSHIFT_ROLE_ARN')}'")

            return query
            
    return None

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

def lambda_handler(event, context):
    try:
        if os.getenv('ENVIRONMENT') =='development':
            event = get_test_event()
    
        object_key = event['Records'][0]['s3']['object']['key']
        file_name = object_key.split('/')[1]

        copy_sql_query = get_copy_query(file_name)

        print(f'Executing Redshift copy command for file: {copy_sql_query}')

        redshift_client = get_service_client(os.getenv('AWS_REDSHIFT_DATA_API'))
        response = redshift_client.execute_statement(
            WorkgroupName=os.getenv("AWS_REDSHIFT_WORKGROUP_NAME"),  # Redshift Serverless Workgroup ARN
            SecretArn=os.getenv("AWS_REDSHIFT_SECRET_ARN"),      # Secrets Manager ARN
            Sql=copy_sql_query,                                  # Your COPY or INSERT SQL
            Database=os.getenv("AWS_REDSHIFT_DB")                # DB name inside Redshift
        )

        if (response['ResponseMetadata']['HTTPStatusCode'] != 200):
            raise Exception(f"Failed to execute Redshift copy command: {response}")

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

        print(traceback.format_exc())

        return {
            'statusCode': 500,
            'body': json.dumps(f"Lambda execution failed: {e}")
        }