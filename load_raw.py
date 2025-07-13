import os
import json
import boto3
import pandas as pd
import requests
import datetime
from io import StringIO
import logging

def load_raw(event, context):
    try:
        api_key = get_calendly_api_key()
        print(api_key)

        # Fetch Calendly Data
        calendly_df = fetch_calendly_scheduled_calls(api_key)

        # Upload Raw Data to S3
        upload_to_s3(calendly_df, S3_CALENDLY_PATH)

        # Calculate and Upload Metrics
        metrics_df = calculate_metrics(calendly_df)
        upload_to_s3(metrics_df, S3_METRICS_PATH)

        logger.info("Lambda execution completed successfully")

        return {
            'statusCode': 200,
            'body': json.dumps("Lambda execution completed successfully")
        }

    except Exception as e:
        logger.error(f"Error during Lambda execution: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Lambda execution failed: {e}")
        }