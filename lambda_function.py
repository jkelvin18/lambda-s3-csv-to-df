import boto3
import pandas as pd
import datetime
import logging
from src.manage_data import get_latest_matching_object, load_data

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Lambda function to read the most recent S3 object that matches a given
    prefix and contains a given substring in the name, read its contents into
    a Pandas dataframe, and save it to S3 as a Parquet file.
    """
    # Get input parameters from event
    # bucket = event['bucket']
    # substr = event['substr']
    # schema = event['schema']
    # pathoutput = event['pathoutput']
    # table = event['table']
    # database = event['database']
    # partitions_cols = event['partitions_cols'
    bucket = 'your-bucket-name'
    yesterday = (datetime.datetime.today() - datetime.timedelta(days=1)
                 ).strftime('%Y%m%d')
    prefix = f'{yesterday}/'
    substr = 'your-substring'
    schema = {
        "column1": "string",
        "column2": "int",
        "column3": "float",
        "column4": "date",
        "column5": "datetime"
    }
    pathoutput = "s3://bucket-name/tb-name"
    table = "tb_name"
    database = "db_name"
    partitions_cols = "anomesdia"

    # S3 client
    s3_client = boto3.client('s3')

    try:
        # Get the most recent matching object
        logger.info(f"Getting the latest matching object from \
                    {bucket}/{prefix}")
        latest_obj_key = get_latest_matching_object(s3_client,
                                                    bucket,
                                                    prefix,
                                                    substr)

        # Read the object contents into a Pandas dataframe with schema
        obj = s3_client.get_object(Bucket=bucket, Key=latest_obj_key)
        df = pd.read_csv(obj['Body'],
                         names=schema,
                         delimiter=";",
                         skiprows=1)

        # Load data to S3 and Glue catalog
        logger.info("Loading data to S3 and Glue catalog")
        load_data(df, pathoutput, table, database, partitions_cols)

        logger.info("Lambda function execution completed successfully")
        return "Success"

    except Exception as e:
        # Log the error and return a message to the caller
        logger.error(f"An error occurred: {e}")
        return {"errorMessage": str(e)}
