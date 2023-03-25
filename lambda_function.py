import logging
import boto3
import pandas as pd
import datetime
import zipfile
from io import BytesIO
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
    # Get yesterday's date as a string in YYYYMMDD format to use in S3 prefix
    yesterday = (
        datetime.datetime.today() - datetime.timedelta(days=1)
    ).strftime("%Y%m%d")

    data_ref = event.get("data_ref", yesterday)

    # Get input parameters from event
    bucket_dstn = event["bucket_dstn"]
    bucket_src = event["bucket_src"]
    substr = event["substr"]
    schema = event["schema"]
    pathoutput = event["pathoutput"]
    table = event["table"]
    database = event["database"]
    partitions_cols = event["partitions_cols"]
    bucket_prefix = event["prefix"]

    prefix = f"{bucket_prefix}/{data_ref}"

    # S3 client
    s3_client = boto3.client("s3")
    boto3_session = boto3.Session()
    s3 = boto3.resource("s3")

    try:
        # Get the most recent matching object
        latest_obj_key = get_latest_matching_object(
            s3_client, bucket_src, prefix, substr
        )

        # Download the file from S3 using boto3
        s3_object = s3.Object(bucket_src, latest_obj_key)
        zip_content = s3_object.get()["Body"].read()

        # Load the zip file content into memory using BytesIO
        zip_file = BytesIO(zip_content)

        # Extract the zip file content in memory using zipfile
        with zipfile.ZipFile(zip_file) as z:
            file_name = z.namelist()[0]
            with z.open(file_name) as f:
                df = pd.read_csv(
                    f,
                    delimiter=";",
                    encoding="windows-1252",
                    names=schema,
                    skiprows=1,
                )

        # Load data to S3 and Glue catalog
        logger.info("Loading data to S3 and Glue catalog")
        load_data(
            df,
            pathoutput,
            table,
            database,
            data_ref,
            partitions_cols,
            bucket_dstn,
            s3_client,
            boto3_session,
        )

        logger.info("Lambda function execution completed successfully")
        return "Success"

    except Exception as e:
        # Log the error and return a message to the caller
        logger.error(f"An error occurred: {e}")
        return {"errorMessage": str(e)}
