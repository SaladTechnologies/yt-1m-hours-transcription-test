import boto3
import json
from job_queue_access import get_access_aws_sqs


def job_delete(thread_ID, sqs_client, QueueUrl, receipt_handle):

    try: 
        response = sqs_client.delete_message(
             QueueUrl=QueueUrl,ReceiptHandle=receipt_handle,
        )
        #print(response['ResponseMetadata']['HTTPStatusCode'])

    except Exception as e:
        print("Thread {} run into trouble:".format(thread_ID))
        print(str(e))


def job_download(thread_ID, sqs_client, QueueUrl):    

    result = []

    try:
        response = sqs_client.receive_message(
                    QueueUrl=QueueUrl, MaxNumberOfMessages=10, WaitTimeSeconds=10)
        temp = response.get("Messages", []) # get [] if empty
        for message in temp:
            temp1 = json.loads(message["Body"])
            temp2 = message['ReceiptHandle']
            result.append([ temp1,temp2 ])

    except Exception as e:
        print("Thread {} run into trouble:".format(thread_ID))
        print(str(e))
        
    return result


sqs_client, QueueUrl = get_access_aws_sqs()

for i in range (1):

    batch_jobs = job_download("Cleaner",sqs_client,QueueUrl)
    if(len(batch_jobs) == 0):
        break

    for onejob in batch_jobs:
        print(onejob[0])
        job_delete("Cleaner", sqs_client, QueueUrl, onejob[1]) 
