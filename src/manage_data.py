import awswrangler as wr


def get_latest_matching_object(s3_client, bucket, prefix, substr):
    """
    Gets the S3 object that matches the given prefix
    and contains the given substring in the name.
    Returns the object key (name) of the most recent object
    that matches the prefix and substring.
    """
    try:
        objects = s3_client.list_objects_v2(Bucket=bucket,
                                            Prefix=prefix)['Contents']
    except Exception as e:
        raise ValueError(f"Error occurred while listing objects in\
                          {bucket}: {e}")

    filtered_objects = [obj for obj in objects if substr in obj['Key']]
    if not filtered_objects:
        raise ValueError(f"No objects found with prefix {prefix} \
                         and substring {substr}")

    latest_obj = max(filtered_objects, key=lambda obj: obj['LastModified'])
    return latest_obj['Key']


def load_data(df, pathoutput, table, database, partitions_cols):
    """
    Save the extracted data to S3 as a Parquet file
    Parameters:
    df (pd.DataFrame): The extracted data
    pathoutput (str): The path to save the data in S3
    Returns:
    None
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
        raise ValueError(f"Error occurred while \
                         saving data to {pathoutput}: {e}")
