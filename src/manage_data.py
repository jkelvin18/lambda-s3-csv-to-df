import logging
import awswrangler as wr

# Set up logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_latest_matching_object(s3_client, bucket, prefix, substr):
    """
    Gets the S3 object that matches the given prefix and contains the given
    substring in the name. Returns the object key (name) of the most recent
    object that matches the prefix and substring.

    Args:
        s3_client (boto3.client): S3 client
        bucket (str): S3 bucket name
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
        objects = s3_client.list_objects_v2(Bucket=bucket,
                                            Prefix=prefix)['Contents']
    except Exception as e:
        # Log error and raise exception
        logger.error(f"Error occurred while listing objects in\
                      {bucket}: {e}")
        raise ValueError(f"Error occurred while listing objects in\
                          {bucket}: {e}")

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


def load_data(df, pathoutput, table, database, partitions_cols):
    """
    Save the extracted data to S3 as a Parquet file

    Args:
        df (pd.DataFrame): The extracted data
        pathoutput (str): The path to save the data in S3
        table (str): table name
        database (str): database name
        partitions_cols(str): partition column name

    Returns:
        None

    Raises:
        ValueError: if there is an error saving data
    """
    try:
        wr.s3.to_parquet(
            df=df,
            path=pathoutput,
            table=table,
            database=database,
            compression="snappy",
            dataset=True,
            partition_cols=[partitions_cols],
        )
    except Exception as e:
        # Log error and raise exception
        logger.error(f"Error occurred while saving data to \
                     {pathoutput}: {e}")
        raise ValueError(f"Error occurred while saving data to \
                         {pathoutput}: {e}")
