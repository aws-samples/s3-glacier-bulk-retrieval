# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import os
import uuid
import datetime
import time
import logging
import boto3
from arnparse import arnparse

logger = logging.getLogger()
logger.setLevel(logging.INFO)

"""
{
    "bucket": "BUCKET_NAME",
    "key": "KEY",
    "tier": "STANDARD",
    "expiration_days": 1,
    "client_tag": "CLIENT_TAG"
}
"""

REPORT_BUCKET = os.environ.get("ReportBucket")
JOBS_TABLE = os.environ.get("JobsTable")
FILES_TABLE = os.environ.get("FilesTable")
ROLE_ARN = os.environ.get("RoleArn")
SNS_TOPIC = os.environ.get("SNSTopic")
SQS_QUEUE = os.environ.get("SQSQueue")
SQS_TOPIC_QUEUE = os.environ.get("SQSTopicQueue")

s3_client = boto3.client("s3")
s3_control_client = boto3.client("s3control")
sts_client = boto3.client("sts")
dynamodb = boto3.resource("dynamodb")
identity = sts_client.get_caller_identity()
account_id = identity.get("Account")


def create_job(event, body, job_id):
    bucket_name = body["bucket"]
    object_key = body["key"]
    client_tag = body["client_tag"]
    tier = body["tier"]
    expiration_days = body["expiration_days"]

    s3_result = s3_client.head_object(Bucket=bucket_name, Key=object_key)
    object_etag = s3_result["ETag"]

    response = s3_control_client.create_job(
        AccountId=account_id,
        ConfirmationRequired=False,
        ClientRequestToken=job_id,
        Manifest={
            "Spec": {
                "Format": "S3BatchOperations_CSV_20180820",
                "Fields": ["Bucket", "Key"]
            },
            "Location": {
                "ObjectArn": f"arn:aws:s3:::{bucket_name}/{object_key}",
                "ETag": object_etag
            }
        },
        Operation={
            "S3InitiateRestoreObject": {
                "ExpirationInDays": expiration_days,
                "GlacierJobTier": tier
            }
        },
        Report={
            "Bucket": REPORT_BUCKET,
            "Format": 'Report_CSV_20180820',
            "Enabled": True,
            "ReportScope": "AllTasks"
        },
        Priority=10,
        RoleArn=ROLE_ARN,
        Tags=[{"Key": "client_tag", "Value": client_tag}]
    )

    return response


def get_item_mapper(job_id):
    def get_item(line):
        [bucket, key] = line.split(",")

        week = datetime.datetime.today() + datetime.timedelta(days=7)
        expiryDateTime = int(time.mktime(week.timetuple()))

        return {
            "BucketName": bucket,
            "ObjectKey": key,
            "JobId": job_id,
            "RestoreStatus": False,
            "TimeToLive": expiryDateTime
        }

    return get_item


def dynamodb_insert(job_id, data, batch_job):
    num_files = len(data)

    files_table = dynamodb.Table(FILES_TABLE)
    jobs_table = dynamodb.Table(JOBS_TABLE)
    items = map(get_item_mapper(job_id), data)

    with files_table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)

    jobs_table.put_item(
        Item={
            "JobId": job_id,
            "TotalFiles": num_files,
            "BatchJobId": batch_job["JobId"],
            "Timestamp": datetime.datetime.utcnow().isoformat()
        })

    return num_files


def check_bucket_subscription(data):
    buckets = set()

    for line in data:
        bucket_name = line.split(",")[0]
        buckets.add(bucket_name)

    for bucket in buckets:
        bucket_notifications_configuration = {
            "QueueConfigurations": [{
                "Events": ["s3:ObjectRestore:Completed"],
                "Id": "Notifications",
                "QueueArn": SQS_QUEUE
            }]
        }

        s3_client.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration=bucket_notifications_configuration
        )


def lambda_handler(event, context):
    try:
        body = json.loads(event["body"])
        job_id = str(uuid.uuid4())

        logger.info(body)
        logger.info(job_id)

        bucket_name = body["bucket"]
        object_key = body["key"]
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        data = response["Body"].read().decode("utf-8").splitlines()

        check_bucket_subscription(data)
        batch_job = create_job(event, body, job_id)
        num_files = dynamodb_insert(job_id, data, batch_job)
    except Exception as e:
        logger.error(e)

        return {
            "statusCode": 500,
            "body": json.dumps({"Error": str(e)}),
        }

    bucket_arn = arnparse(REPORT_BUCKET)
    report_bucket = bucket_arn.resource

    return {
        "statusCode": 200,
        "body": json.dumps({
            "SNSTopic": SNS_TOPIC,
            "SQSQueue": SQS_TOPIC_QUEUE,
            "ReportBucket": report_bucket,
            "BulkJobId": job_id,
            "TotalFilesRequested": num_files,
            "BatchOperation": batch_job
        }),
    }
