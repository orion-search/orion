import pytest

from unittest.mock import patch
from orion.packages.utils.s3_utils import store_on_s3
from orion.packages.utils.s3_utils import create_s3_bucket


@patch("orion.packages.utils.s3_utils.boto3")
def test_store_on_s3(boto3):
    """Data is archived, uploaded, and the floor is swept"""
    store_on_s3("test_data", "bucket", "prefix")

    boto3.resource.assert_called_with("s3")
    boto3.resource().Object.assert_called_with("bucket", "prefix.pickle")


@patch("orion.packages.utils.s3_utils.boto3")
def test_create_s3_bucket(boto3):
    create_s3_bucket("test_bucket")

    boto3.resource.assert_called_with("s3")
