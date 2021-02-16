# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import boto3
import os

JOBS_TABLE = os.environ.get("JobsTable")
FILES_TABLE = os.environ.get("FilesTable")


def lambda_handler(event, context):
    
    dynamodb = boto3.resource('dynamodb')
    table_name = os.environ.get('FilesTable')
    table = dynamodb.Table(table_name)

    for record in event['Records']:
        obj = json.loads(record['body'])
        
        #print(obj)
        
        if 'Records' in obj:
            eventName = obj['Records'][0]['eventName']
            if eventName=='ObjectRestore:Completed':
                bucketName = obj['Records'][0]['s3']['bucket']['name']
                keyFile = obj['Records'][0]['s3']['object']['key']
    
                print("bucket:"+bucketName +"  file:"+ keyFile)
    
                if (table_name and bucketName and keyFile):
                    UpdateItem(bucketName, keyFile, table)
                else:
                    return {
                        'statusCode': 400,
                        'body': 'BucketName or table_name or keyFile is empty'
                    }

    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }


def UpdateItem(bucket, key, dynamoTable):
    try:
        response = dynamoTable.update_item(
            Key={
                'BucketName': bucket,
                'ObjectKey': key
            },
            UpdateExpression='SET RestoreStatus = :val1',

            ExpressionAttributeValues={
                ':val1': True
            }
        )
        print(response)
        
    except ClientError as e:
        print(e.response['Error']['Message'])
