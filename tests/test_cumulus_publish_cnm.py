import boto3
import json

import pytest

from cumulus_publish_cnm import __version__
from cumulus_publish_cnm.cumulus_publish_cnm import lambda_handler
from moto import mock_s3, mock_sns

lambda_input = {
  "cumulus_meta": {
    "cumulus_version": "9.9.0",
    "message_source": "sfn",
    "queueExecutionLimits": {
      "https://sqs.us-west-2.amazonaws.com/065089468788/hryeung-ia-podaac-background-job-queue": 200,
      "https://sqs.us-west-2.amazonaws.com/065089468788/hryeung-ia-podaac-backgroundProcessing": 5,
      "https://sqs.us-west-2.amazonaws.com/065089468788/hryeung-ia-podaac-big-background-job-queue": 20,
      "https://sqs.us-west-2.amazonaws.com/065089468788/hryeung-ia-podaac-forge-background-job-queue": 200,
      "https://sqs.us-west-2.amazonaws.com/065089468788/hryeung-ia-podaac-tig-background-job-queue": 200
    },
    "state_machine": "arn:aws:states:us-west-2:065089468788:stateMachine:hryeung-ia-podaac-DiscoverWorkflow",
    "system_bucket": "hryeung-ia-podaac-internal",
    "queueUrl": "arn:aws:sqs:us-west-2:065089468788:hryeung-ia-podaac-startSF"
  },
  "replace": {
    "Bucket": "hryeung-ia-podaac-internal",
    "Key": "events/hryeung-ia-podaac-testingFunction-INPUT.json",
    "TargetPath": "$"
  }
}

s3_file_content = {
    "payload": {
        "granules": [
            {
                "version": "1.5",
                "provider": "PODAAC",
                "submissionTime": "2020-11-10T18:27:13.988143",
                "collection": "MODIS_A-JPL-L2P-v2019.0",
                "identifier": "5abb6308-2382-11eb-9c5b-acde48001122",
                "trace": "NCMODIS_A-JPL-L2P-v2019.01",
                "product": {
                    "name": "20200101232501-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0",
                    "dataVersion": "2019.0",
                    "files": [
                        {
                            "type": "data",
                            "uri": "s3://podaac-dev-cumulus-test-input-v2/MODIS_A-JPL-L2P-v2019.0/2020/001/20200101232501-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc",
                            "name": "20200101232501-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc",
                            "checksumType": "md5",
                            "size": 22015385
                        },
                        {
                            "type": "metadata",
                            "uri": "s3://podaac-dev-cumulus-test-input-v2/MODIS_A-JPL-L2P-v2019.0/2020/001/20200101232501-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc.md5",
                            "name": "20200101232501-JPL-L2P_GHRSST-SSTskin-MODIS_A-D-v02.0-fv01.0.nc.md5",
                            "size": 98
                        }
                    ]
                }
            }
        ]
    }
}


def test_version():
    assert __version__ == '0.1.1'


@mock_s3
@mock_sns
def test_lambda_handler(monkeypatch):  # monkeypatch enables environment variable setup
    """Publish Data and assure response"""
    # S3 client setup
    s3_client = boto3.client('s3', region_name='us-east-1')  # s3 doesn't like us-west-2...
    test_bucket_name = 'hryeung-ia-podaac-internal'
    test_bucket_key = 'events/hryeung-ia-podaac-testingFunction-INPUT.json'
    s3_client.create_bucket(Bucket=test_bucket_name)
    s3_client.put_object(Body=json.dumps(s3_file_content), Bucket=test_bucket_name, Key=test_bucket_key)

    # SNS client setup
    sns_client = boto3.client('sns', region_name='us-west-2')
    sns_client.create_topic(Name='some_topic')
    topics_json = sns_client.list_topics()
    topic_arn = topics_json["Topics"][0]["TopicArn"]

    response = {}
    try:
        monkeypatch.setenv('sns_endpoint', topic_arn)
        monkeypatch.setenv('list_key', 'granules')
        response = lambda_handler(lambda_input, {})
    except Exception as e:
        print(e)

    assert response[0]['ResponseMetadata']['HTTPStatusCode'] is 200


def test_lambda_missing_environment_variables():
    """Expected error raised due to missing environment variable (sns_endpoint and list_key)"""
    with pytest.raises(NameError) as exc_info:
        response = lambda_handler(lambda_input, {})

