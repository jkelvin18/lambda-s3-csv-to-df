import boto3
import pytest
import datetime
from moto import mock_s3


@pytest.fixture
def s3():
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def bucket(s3):
    bucket = "dblocalsotpagamentos"
    s3.create_bucket(Bucket=bucket)
    yield bucket


@pytest.fixture
def object_key(s3, bucket):
    yesterday = (
        datetime.datetime.today() - datetime.timedelta(days=1)
    ).strftime("%Y%m%d")
    object_key = f"input_debentures/{yesterday}/debentures_v_2023.csv"
    s3.put_object(Bucket=bucket, Key=object_key, Body=b"column1,value1\n")
    return object_key


def test_get_latest_matching_object(s3, bucket, object_key):
    from src.manage_data import get_latest_matching_object
    yesterday = (
        datetime.datetime.today() - datetime.timedelta(days=1)
    ).strftime("%Y%m%d")
    prefix = f"input_debentures/{yesterday}"

    result = get_latest_matching_object(s3, bucket, prefix, "debentures_v")
    assert result == object_key
    