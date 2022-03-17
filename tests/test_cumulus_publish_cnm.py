import boto3
import json

import pytest

from cumulus_publish_cnm import __version__
from cumulus_publish_cnm.cumulus_publish_cnm import lambda_handler
from moto import mock_s3, mock_sns

lambda_input = {
    "cma": {
        "event": {
            "cumulus_meta": {
                "cumulus_version": "9.9.0",
                "message_source": "sfn",
                "system_bucket": "dummy_bucket"
            },
            "replace": {
                "Bucket": "dummy_bucket",
                "Key": "events/dummy_aws_s3_object.json",
                "TargetPath": "$"
            }
        },
        "task_config": {
            "sns_endpoint": "{$.meta.provider}"
        }
    }
}

bad_lambda_input = {
    "cma": {
        "event": {
            "cumulus_meta": {
                "cumulus_version": "9.9.0",
                "message_source": "sfn",
                "system_bucket": "dummy_bucket"
            },
            "replace": {
                "Bucket": "dummy_bucket",
                "Key": "events/dummy_aws_s3_object.json",
                "TargetPath": "$"
            }
        }
    }
}

s3_file_content = {
    "cumulus_meta": {
        "cumulus_version": "9.9.0",
        "message_source": "sfn",
        "system_bucket": "dummy_bucket"
    },
    "exception": "None",
    "meta": {
    },
    "payload": {
        "cnm_list": [
            {
                "version": "1.5.1",
                "provider": "PODAAC",
                "collection": "VIIRS_NPP-NAVO-L2P-v3.0",
                "submissionTime": "2022-03-17T17:21:12.539731",
                "identifier": "20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "product": {
                    "name": "20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                    "files": [
                        {
                            "type": "data",
                            "uri": "sftp://ops-metis.jpl.nasa.gov/cumulus-test/gds2/NAVO/20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                            "size": 18167706,
                            "name": "20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc"
                        },
                        {
                            "type": "metadata",
                            "uri": "sftp://ops-metis.jpl.nasa.gov/cumulus-test/gds2/NAVO/20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                            "size": 97,
                            "name": "20220111135009-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5"
                        }
                    ],
                    "dataVersion": "3.0"
                }
            },
            {
                "version": "1.5.1",
                "provider": "PODAAC",
                "collection": "VIIRS_NPP-NAVO-L2P-v3.0",
                "submissionTime": "2022-03-17T17:21:12.539764",
                "identifier": "20220111135133-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "product": {
                    "name": "20220111135133-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                    "files": [
                        {
                            "type": "data",
                            "uri": "sftp://ops-metis.jpl.nasa.gov/cumulus-test/gds2/NAVO/20220111135133-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                            "size": 18294159,
                            "name": "20220111135133-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc"
                        },
                        {
                            "type": "metadata",
                            "uri": "sftp://ops-metis.jpl.nasa.gov/cumulus-test/gds2/NAVO/20220111135133-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                            "size": 97,
                            "name": "20220111135133-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5"
                        }
                    ],
                    "dataVersion": "3.0"
                }
            },
            {
                "version": "1.5.1",
                "provider": "PODAAC",
                "collection": "VIIRS_NPP-NAVO-L2P-v3.0",
                "submissionTime": "2022-03-17T17:21:12.539789",
                "identifier": "20220111135258-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "product": {
                    "name": "20220111135258-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                    "files": [
                        {
                            "type": "data",
                            "uri": "sftp://ops-metis.jpl.nasa.gov/cumulus-test/gds2/NAVO/20220111135258-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                            "size": 17146221,
                            "name": "20220111135258-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc"
                        },
                        {
                            "type": "metadata",
                            "uri": "sftp://ops-metis.jpl.nasa.gov/cumulus-test/gds2/NAVO/20220111135258-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                            "size": 97,
                            "name": "20220111135258-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5"
                        }
                    ],
                    "dataVersion": "3.0"
                }
            },
            {
                "version": "1.5.1",
                "provider": "PODAAC",
                "collection": "VIIRS_NPP-NAVO-L2P-v3.0",
                "submissionTime": "2022-03-17T17:21:12.539813",
                "identifier": "20220111135423-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "product": {
                    "name": "20220111135423-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                    "files": [
                        {
                            "type": "data",
                            "uri": "sftp://ops-metis.jpl.nasa.gov/cumulus-test/gds2/NAVO/20220111135423-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                            "size": 18654568,
                            "name": "20220111135423-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc"
                        },
                        {
                            "type": "metadata",
                            "uri": "sftp://ops-metis.jpl.nasa.gov/cumulus-test/gds2/NAVO/20220111135423-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                            "size": 97,
                            "name": "20220111135423-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5"
                        }
                    ],
                    "dataVersion": "3.0"
                }
            },
            {
                "version": "1.5.1",
                "provider": "PODAAC",
                "collection": "VIIRS_NPP-NAVO-L2P-v3.0",
                "submissionTime": "2022-03-17T17:21:12.539837",
                "identifier": "20220111135549-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "product": {
                    "name": "20220111135549-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                    "files": [
                        {
                            "type": "data",
                            "uri": "sftp://ops-metis.jpl.nasa.gov/cumulus-test/gds2/NAVO/20220111135549-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                            "size": 17730761,
                            "name": "20220111135549-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc"
                        },
                        {
                            "type": "metadata",
                            "uri": "sftp://ops-metis.jpl.nasa.gov/cumulus-test/gds2/NAVO/20220111135549-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                            "size": 97,
                            "name": "20220111135549-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5"
                        }
                    ],
                    "dataVersion": "3.0"
                }
            },
            {
                "version": "1.5.1",
                "provider": "PODAAC",
                "collection": "VIIRS_NPP-NAVO-L2P-v3.0",
                "submissionTime": "2022-03-17T17:21:12.539860",
                "identifier": "20220111135714-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "product": {
                    "name": "20220111135714-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                    "files": [
                        {
                            "type": "data",
                            "uri": "sftp://ops-metis.jpl.nasa.gov/cumulus-test/gds2/NAVO/20220111135714-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                            "size": 16983663,
                            "name": "20220111135714-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc"
                        },
                        {
                            "type": "metadata",
                            "uri": "sftp://ops-metis.jpl.nasa.gov/cumulus-test/gds2/NAVO/20220111135714-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                            "size": 97,
                            "name": "20220111135714-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5"
                        }
                    ],
                    "dataVersion": "3.0"
                }
            },
            {
                "version": "1.5.1",
                "provider": "PODAAC",
                "collection": "VIIRS_NPP-NAVO-L2P-v3.0",
                "submissionTime": "2022-03-17T17:21:12.539883",
                "identifier": "20220111135840-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                "product": {
                    "name": "20220111135840-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0",
                    "files": [
                        {
                            "type": "data",
                            "uri": "sftp://ops-metis.jpl.nasa.gov/cumulus-test/gds2/NAVO/20220111135840-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc",
                            "size": 16316733,
                            "name": "20220111135840-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc"
                        },
                        {
                            "type": "metadata",
                            "uri": "sftp://ops-metis.jpl.nasa.gov/cumulus-test/gds2/NAVO/20220111135840-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5",
                            "size": 97,
                            "name": "20220111135840-NAVO-L2P_GHRSST-SST1m-VIIRS_NPP-v02.0-fv03.0.nc.md5"
                        }
                    ],
                    "dataVersion": "3.0"
                }
            }
        ]
    }
}


def test_version():
    assert __version__ == '0.2.0'


@mock_s3
@mock_sns
def test_lambda_handler(monkeypatch):  # monkeypatch enables environment variable setup
    """Publish Data and assure response"""
    # S3 client setup
    s3_client = boto3.client('s3', region_name='us-east-1')  # s3 doesn't like us-west-2...
    test_bucket_name = 'dummy_bucket'
    test_bucket_key = 'events/dummy_aws_s3_object.json'
    s3_client.create_bucket(Bucket=test_bucket_name)
    s3_client.put_object(Body=json.dumps(s3_file_content), Bucket=test_bucket_name, Key=test_bucket_key)

    # SNS client setup
    sns_client = boto3.client('sns', region_name='us-west-2')
    sns_client.create_topic(Name='dummy_aws_sns_topic')
    topics_json = sns_client.list_topics()
    topic_arn = topics_json["Topics"][0]["TopicArn"]

    response = {}

    lambda_input['cma']['task_config']['sns_endpoint'] = topic_arn

    try:
        response = lambda_handler(lambda_input, {})
    except Exception as e:
        print(e)

    assert response['payload'][0]['ResponseMetadata']['HTTPStatusCode'] is 200

@mock_s3
@mock_sns
def test_lambda_missing_environment_variables():
    """Expected error raised due to missing environment variable (sns_endpoint and list_key)"""

    """Publish Data and assure response"""
    # S3 client setup
    s3_client = boto3.client('s3', region_name='us-east-1')  # s3 doesn't like us-west-2...
    test_bucket_name = 'dummy_bucket'
    test_bucket_key = 'events/dummy_aws_s3_object.json'
    s3_client.create_bucket(Bucket=test_bucket_name)
    s3_client.put_object(Body=json.dumps(s3_file_content), Bucket=test_bucket_name, Key=test_bucket_key)

    # SNS client setup
    sns_client = boto3.client('sns', region_name='us-west-2')
    sns_client.create_topic(Name='dummy_aws_sns_topic')
    topics_json = sns_client.list_topics()
    topic_arn = topics_json["Topics"][0]["TopicArn"]

    response = {}

    with pytest.raises(Exception) as exc_info:
        response = lambda_handler(bad_lambda_input, {})

    assert "config key is missing" in str(exc_info.value)

