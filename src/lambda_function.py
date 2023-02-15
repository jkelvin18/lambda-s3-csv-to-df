import boto3
import pandas as pd
import datetime


def get_latest_matching_object(s3_client, bucket, prefix, substr):
    """
    Gets the S3 object that matches the given prefix
    and contains the given substring in the name.
    Returns the object key (name) of the most recent object
    that matches the prefix and substring.
    """
    objects = s3_client.\
        list_objects_v2(Bucket=bucket, Prefix=prefix)['Contents']
    filtered_objects = [obj for obj in objects if substr in obj['Key']]
    if not filtered_objects:
        raise ValueError(f"No objects found with prefix {prefix} \
            and substring {substr}")
    latest_obj = max(filtered_objects, key=lambda obj: obj['LastModified'])
    return latest_obj['Key']


def lambda_handler(event, context):
    # S3 bucket and object key
    bucket = 'your-bucket-name'
    yesterday = (datetime.datetime.today() - datetime.timedelta(days=1)).\
        strftime('%Y%m%d')
    prefix = f'{yesterday}/'
    substr = 'your-substring'
    schema = {
        "column1": "string",
        "column2": "int",
        "column3": "float",
        "column4": "date",
        "column5": "datetime"
    }

    # S3 client
    s3_client = boto3.client('s3')

    try:
        # Get the most recent matching object
        latest_obj_key = get_latest_matching_object
        (s3_client, bucket, prefix, substr)

        # Read the object contents into a Pandas dataframe with schema
        obj = s3_client.get_object(Bucket=bucket, Key=latest_obj_key)
        df = pd.read_csv(obj['Body'], names=schema)

        return df.to_dict()

    except Exception as e:
        # Log the error and return a message to the caller
        print(f"An error occurred: {e}")
        return {"errorMessage": str(e)}
