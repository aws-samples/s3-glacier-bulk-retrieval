# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import boto3
import os
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import datetime

dynamodb = boto3.resource('dynamodb')
s3_control_client = boto3.client("s3control")

sts_client = boto3.client("sts")
identity = sts_client.get_caller_identity()
account_id = identity.get("Account")

topic_name = os.environ.get('SNSTopic')
file_table_name = os.environ.get('FilesTable')
jobs_table_name = os.environ.get('JobsTable')
retentionJobTime = int(os.environ.get('RetentionJobTimeDays'))
    
finished_BatchOps_states = {
        "Failed",
        "Complete",
        "Canceled"
    }

def lambda_handler(event, context):

    #Get all Jobs
    jobs = getAllJobIds(jobs_table_name)
    
    finished_jobs =""
    batch_job_status=""
    failedFiles =0

    #Iterate all jobs 
    for job in jobs:
        # Getting and updating status of jobs
        # if there is no status from s3 batch ops then get it and update table
        
        if 'JobStatus' not in job:
           
            #only getting finished statuses here bypassing all intermediate
            batch_job_status, failedFiles = getJobStatusAndUpdateTable(jobs_table_name, job['BatchJobId'], job['JobId'])
            job_status = batch_job_status
        else:
            #batch_job_status = job['BatchOpsStatus']
            job_status = job['JobStatus']
            failedFiles = job['BatchOpsFailedFiles']
        
        # if the s3 batch ops completed then count tasks and send SNS event
        if job_status == "Restoring":

            #Get all tasks/files per job
            filesInProgress = getFilesPerJob(file_table_name, job['JobId'], False)
            filesFinished = getFilesPerJob(file_table_name, job['JobId'], True)

            
            # Handle blank jobs without files
            if len(filesInProgress) == 0 and len(filesFinished) == 0:
                
                #delete job from the table of jobs, files have ttl for 5 days and will automatically cleaned
                cleanDynamoDBtables(jobs_table_name, job['JobId'] )

                #publish event of finishing the proccess
                PublishEvent(job['JobId'], job['BatchJobId'], 'Cancelled', 0, failedFiles)
        
            # if all tasks are finished then send SNS event and delete job from table
            if len(filesInProgress) == 0 and len(filesFinished) > 0:
                finished_jobs = str(job['JobId']) + ", " + finished_jobs
                print("JobID that finished:"+job['JobId'])

                #delete job from the table of jobs, files have ttl for 5 days and will automatically cleaned
                cleanDynamoDBtables(jobs_table_name, job['JobId'] )

                #publish event of finishing the proccess
                PublishEvent(job['JobId'], job['BatchJobId'], 'Complete', 100, failedFiles)
        
            # When we have files in progress lets calculate them and update progress
            if len(filesInProgress) > 0: 
                percentProgress =0
                if job['TotalFiles'] != 0:
                    percentProgress = round(len(filesInProgress) / int(job['TotalFiles']))

                # if current status is the same from DB we will not send anything
                if 'RestoreProgressPercent' in job:
                    if percentProgress == job['RestoreProgressPercent']:
                        print('Status and Progress not changed for job:', job['JobId'])
                    else:

                        #Update table with progress
                        updateJobProgress(jobs_table_name, job['JobId'], percentProgress)

                        #Publish event with progress
                        PublishEvent(job['JobId'], job['BatchJobId'], job_status , percentProgress, failedFiles)
                else:

                    #Update table with progress
                    updateJobProgress(jobs_table_name, job['JobId'], percentProgress)
        
        # If the job is old delete it and send timeout event
        deadlinetime = datetime.datetime.fromisoformat(job['Timestamp']) + datetime.timedelta(days=5)
        #print(deadlinetime)
        if datetime.datetime.now() > deadlinetime:
            #delete job from the table of jobs, files have ttl for 5 days and will automatically cleaned
            cleanDynamoDBtables(jobs_table_name, job['JobId'] )

            #publish event of finishing the proccess
            PublishEvent(job['JobId'], job['BatchJobId'], "Timeout")

            
        # if batch ops status bad send SNS Event and Delete it
        if job_status == "Failed" or job_status == "Cancelled":

            #delete job from the table of jobs, files have ttl for 5 days and will automatically cleaned
            cleanDynamoDBtables(jobs_table_name, job['JobId'] )

            #publish event of finishing the proccess
            PublishEvent(job['JobId'], job['BatchJobId'], job_status)
           
    print('Executed and found these Jobs that are finished: ' + finished_jobs)

    return {
        'statusCode': 200,
        'body': json.dumps('Executed and found these Jobs that are finished: ' + finished_jobs)
    }


def PublishEvent(jobId, batchJobId, status, progress=0, failedBatchFiles=0):

    #print(jobId, batchJobId, status, progress, failedBatchFiles) 

    publishToSNSevent(topic_name, {
        'JobId': jobId,
        'BatchJobId': batchJobId,
        'Status': status,
        'ProgressPercent':str(progress),
        'FailedBatchFiles': str(failedBatchFiles)
        })

def updateJobProgress(table_name, jobid, progress):
    table = dynamodb.Table(table_name)

    try:
        response = table.update_item(
            Key={
                'JobId': jobid
            },
            UpdateExpression='SET RestoreProgressPercent = :val1 , JobStatus = :val2',

            ExpressionAttributeValues={
                ':val1': progress,
                ':val2': 'Restoring'
            }
        )
        #print(response)

    except ClientError as e:
        print(e.response['Error']['Message'])


def getJobStatusAndUpdateTable(table_name, batchJobid, jobid):

    failedFiles=0
    table = dynamodb.Table(table_name)
    
    try:
        response = s3_control_client.describe_job(
            AccountId=account_id ,
            JobId=batchJobid
        )
        if response:
            if 'Job' in response:
                batchStatus = response['Job']['Status']

                if batchStatus == 'Complete':
                    status = 'Restoring'
                else:
                    status = batchStatus
                
                if 'ProgressSummary' in response['Job']:
                    failedFiles = response['Job']['ProgressSummary']['NumberOfTasksFailed']


                if batchStatus in finished_BatchOps_states:
                    
                    #update
                    UpdateJobItemBatchStatus(table_name, jobid, status, batchStatus , failedFiles)
                
    except ClientError as e:
        print(e.response['Error']['Message'])
    
    return status, failedFiles

# Method that get all files for the job from DynamoDB table
def getFilesPerJob(table_name, jobid, status):
    
    table = dynamodb.Table(table_name)
    try:
        response = table.query(
            KeyConditionExpression=Key('JobId').eq(jobid),
            FilterExpression=Attr('RestoreStatus').eq(status),
            IndexName='JobIndex'
        )
    except ClientError as e:
        print(e.response['Error']['Message'])

    return response['Items']


# Method that get all Jobs from DynamoDB table
def getAllJobIds(table_name):
  
    table = dynamodb.Table(table_name)
    #first request
    response = table.scan()
    items = response['Items']
    
    try:
        #contiunue requesting until pages exists
        while 'LastEvaluatedKey' in response:
            #print(response['LastEvaluatedKey'])
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
    except ClientError as e:
        print(e.response['Error']['Message'])

    return items

def UpdateJobItemBatchStatus(dynamoTable, jobid, status, batchStatus, batchFailedFiles):

    table = dynamodb.Table(dynamoTable)

    try:
        response = table.update_item(
            Key={
                'JobId': jobid
            },
            UpdateExpression='SET BatchOpsStatus = :val1, BatchOpsFailedFiles = :val2, JobStatus = :val3, RestoreProgressPercent = :val4',
            ExpressionAttributeValues={
                ':val1': batchStatus,
                ':val2': batchFailedFiles,
                ':val3': status,
                ':val4': 0
            }
        )
        #print(response)
        
    except ClientError as e:
        print(e.response['Error']['Message'])


def print_jobs(jobs):
        for job in jobs:
            print(f"\n{job['JobId']} : {job['TotalFiles']}")
            

def print_files(files):
        for file in files:
            print(f"\n{file['BucketName']} : {file['ObjectKey']}")
            

def cleanDynamoDBtables(table_name, jobid):
    table = dynamodb.Table(table_name)

    try:
        response = table.delete_item(
            Key={
                'JobId': jobid     
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])

# method that send message to SNS topic
def publishToSNSevent(topic, message):
    sns = boto3.client('sns')
    #message = {'message':message}
    if topic:
        response = sns.publish(TopicArn=topic,
        Message=json.dumps({'default': json.dumps(message)}),
        MessageStructure='json')
    
    # Print out the response
    print(message)