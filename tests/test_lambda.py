import boto3
import mock
import pandas as pd
import pytest
from moto import mock_s3


@pytest.fixture
def s3():
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')


@pytest.fixture
def bucket(s3):
    bucket = 'test-bucket'
    s3.create_bucket(Bucket=bucket)
    yield bucket


@pytest.fixture
def object_key(s3, bucket):
    object_key = '20220210/test_file.csv'
    s3.put_object(Bucket=bucket, Key=object_key, Body=b'column1,value1\n')
    return object_key


def test_get_latest_matching_object(s3, bucket, object_key):
    from src.manage_data import get_latest_matching_object
    result = get_latest_matching_object(s3, bucket, '20220210', 'test')
    assert result == object_key


def test_load_data():
    """Test the load_data function."""
    # Define test data and inputs
    df = pd.DataFrame(
        {
            "anomesdia": ["20220101"],
            "codigo": [1],
            "tipo": ["A"],
            "nome": ["Test"],
            "valor": [100],
        }
    )
    pathoutput = "s3://bucket-name/tb-name"
    table = "tb_name"
    database = "db_name"
    partitions_cols = "anomesdia"

    # Mock awswrangler.s3.to_parquet to verify function call
    with mock.patch("awswrangler.s3.to_parquet") as mock_to_parquet:
        from src.manage_data import load_data
        # Call load_data function
        load_data(df, pathoutput, table, database, partitions_cols)

        # Verify the expected call to awswrangler.s3.to_parquet is made
        mock_to_parquet.assert_called_once_with(
            df=df,
            path=pathoutput,
            table=table,
            database=database,
            compression="snappy",
            dataset=True,
            partition_cols=[partitions_cols],
        )
