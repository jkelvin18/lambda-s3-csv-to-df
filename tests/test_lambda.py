import boto3
import pandas as pd
import datetime
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
    from src.lambda_function import get_latest_matching_object
    result = get_latest_matching_object(s3, bucket, '20220210', 'test')
    assert result == object_key
