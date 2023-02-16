import boto3
import pandas as pd
import datetime


def lambda_handler(event, context):
    # S3 bucket and object key
    bucket = 'your-bucket-name'
    yesterday = (datetime.datetime.today() - datetime.timedelta(days=1))\
        .strftime('%Y%m%d')
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
        from src.manage_data import get_latest_matching_object
        latest_obj_key = get_latest_matching_object
        (s3_client, bucket, prefix, substr)

        # Read the object contents into a Pandas dataframe with schema
        obj = s3_client.get_object(Bucket=bucket, Key=latest_obj_key)
        df = pd.read_csv(obj['Body'], names=schema)

        from src.manage_data import load_data
        load_data(df, pathoutput, table, database, partitions_cols)

        return "Success"

    except Exception as e:
        # Log the error and return a message to the caller
        print(f"An error occurred: {e}")
        return {"errorMessage": str(e)}
