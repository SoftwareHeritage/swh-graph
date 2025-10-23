# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

import boto3
from moto import mock_aws
import pytest

import swh


def add_example_dataset_to_s3_bucket(datasets_path, bucket, prefix, name):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=bucket)
    for root, _, files in os.walk(datasets_path):
        for f in files:
            path = os.path.join(root, f)
            relative_path = path.replace(datasets_path + "/", "")
            key = os.path.join(prefix, name, "compressed", relative_path)
            s3.upload_file(
                Filename=path,
                Bucket=bucket,
                Key=key,
                ExtraArgs={
                    "ACL": "public-read",
                },
            )
    s3.put_object(
        ACL="public-read",
        Body=b"{}",
        Bucket=bucket,
        Key=os.path.join(prefix, name, "compressed/meta/compression.json"),
    )


@pytest.fixture
def s3_bucket_name():
    return "softwareheritage"


@pytest.fixture
def s3_graph_dataset_path_prefix():
    return "graph"


@pytest.fixture
def s3_graph_dataset_name():
    return "example"


@pytest.fixture
def s3_graph_dataset_url(
    s3_bucket_name, s3_graph_dataset_path_prefix, s3_graph_dataset_name
):
    return (
        f"s3://{s3_bucket_name}/{s3_graph_dataset_path_prefix}/"
        f"{s3_graph_dataset_name}/compressed/"
    )


@pytest.fixture
def graph_example_dataset_path():
    return os.path.join(
        os.path.dirname(swh.graph.__file__), "example_dataset", "compressed"
    )


@pytest.fixture
def mocked_aws(
    graph_example_dataset_path,
    s3_bucket_name,
    s3_graph_dataset_path_prefix,
    s3_graph_dataset_name,
):
    with mock_aws():
        add_example_dataset_to_s3_bucket(
            graph_example_dataset_path,
            s3_bucket_name,
            s3_graph_dataset_path_prefix,
            s3_graph_dataset_name,
        )
        yield
