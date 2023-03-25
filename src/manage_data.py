import boto3
from awswrangler import s3
import logging
from botocore.exceptions import ClientError

# Set up logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_latest_matching_object(s3_client,
                               bucket_src,
                               prefix,
                               substr):
    """
    Gets the S3 object that matches the given prefix and contains the given
    substring in the name. Returns the object key (name) of the most recent
    object that matches the prefix and substring.
    Args:
        s3_client (boto3.client): S3 client
        bucket_src (str): The name of the source bucket
        prefix (str): S3 object prefix
        substr (str): substring to search for in object names
    Returns:
        str: name of the most recent object that matches the prefix and
        substring
    Raises:
        ValueError: if there is an error listing objects or if no objects
        are found that match the prefix and substring
    """
    try:
        objects = s3_client.list_objects_v2(Bucket=bucket_src,
                                            Prefix=prefix)['Contents']
    except Exception as e:
        # Log error and raise exception
        logger.error(f"Error occurred while listing objects in\
                          {bucket_src}: {e}")
        raise ValueError(f"Error occurred while listing objects in\
                          {bucket_src}: {e}")

    filtered_objects = [obj for obj in objects if substr in obj['Key']]
    if not filtered_objects:
        # Log warning and raise exception
        logger.warning(f"No objects found with prefix {prefix} \
                         and substring {substr}")
        raise ValueError(f"No objects found with prefix {prefix} \
                         and substring {substr}")

    # Get most recent object
    latest_obj = max(filtered_objects, key=lambda obj: obj['LastModified'])
    # Log info message
    logger.info(f"Latest matching object: {latest_obj['Key']}")
    return latest_obj['Key']


def load_data(df,
              pathoutput,
              table,
              database,
              data_ref,
              partitions_cols,
              bucket_dstn,
              s3_client,
              boto3_session):
    """
    Save the extracted data to S3 as a Parquet file
    Args:
        df (pd.DataFrame): The extracted data
        pathoutput (str): The path to save the data in S3
        table (str): table name
        database (str): database name
        data_ref (str): The partition that will be recorded
        partitions_cols(str): partition column name
        bucket_dstn (str): The name of the destin bucket
    Returns:
        None
    Raises:
        ValueError: if there is an error saving data
    """

    # Get S3 client
    s3_client = boto3.client('s3')

    # Remove existing partition if exists
    partition_path = f"{pathoutput}/{partitions_cols}={data_ref}"
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_dstn,
                                             Prefix=partition_path)
        if response['KeyCount'] > 0:
            objects = response['Contents']
            delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects]}
            s3_client.delete_objects(Bucket=bucket_dstn, Delete=delete_keys)
    except ClientError as e:
        logger.error(f"Error occurred while removing existing partition \
                      {partition_path}: {e}")
        raise ValueError(f"Error occurred while removing existing partition \
                          {partition_path}: {e}")

    # Save the new partition
    try:
        s3.to_parquet(
            df=df,
            path=pathoutput,
            table=table,
            database=database,
            compression="snappy",
            dataset=True,
            partition_cols=[partitions_cols],
            boto3_session=boto3_session,
            mode="overwrite_partitions"
        )
    except Exception as e:
        # Log error and raise exception
        logger.error(f"Error occurred while saving data to\
                     {pathoutput}: {e}")
        raise ValueError(f"Error occurred while saving data to\
                         {pathoutput}: {e}")
