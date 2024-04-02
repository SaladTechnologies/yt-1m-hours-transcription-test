import requests
import os
import csv
import time
import queue
import threading
import boto3
import json
import uuid
import random
from job_queue_access import get_access_aws_sqs


sqs_client,QueueUrl = get_access_aws_sqs()


def get_Queue_Status_1min(old = 0):
    result = sqs_client.get_queue_attributes(QueueUrl=QueueUrl, AttributeNames=['All'])
    x = result['Attributes']['ApproximateNumberOfMessages']
    y = result['Attributes']['ApproximateNumberOfMessagesNotVisible']
    #print(result)
    temp = int(x) + int(y)

    if (old-temp) == 0:
        hours = 0
    else:
        hours = round(temp / ((old-temp)*60),2)
    
    print("1  min  ---> {} in the queue".format(temp),end=",")
    print(" * {} available and".format(x), end="")
    print(" {} being processed".format(y), end="")
    print(", {} done in 1  min  (around {} per hours and will run {} hours)".format(old-temp, (old-temp)*60, hours),flush=True)
    return temp

def get_Queue_Status_10mins(old = 0):
    result = sqs_client.get_queue_attributes(QueueUrl=QueueUrl, AttributeNames=['All'])
    x = result['Attributes']['ApproximateNumberOfMessages']
    y = result['Attributes']['ApproximateNumberOfMessagesNotVisible']
    #print(result)
    temp = int(x) + int(y)

    if (old-temp) == 0:
        hours = 0
    else:
        hours = round(temp / ((old-temp)*6),2)
    
    print("10 mins ---> {} in the queue".format(temp),end=",")
    print(" * {} available and".format(x), end="")
    print(" {} being processed".format(y), end="")
    print(", {} done in 10 mins (around {} per hours and will run {} hours)".format(old-temp, (old-temp)*6, hours),flush=True)
    return temp


unit = 60 # 60

def monitor_1min():
    old = 0
    while True:
        old = get_Queue_Status_1min(old)
        time.sleep(unit)
        
def monitor_10mins():
    old = 0

    while True:
        print()
        old = get_Queue_Status_10mins(old)
        print()
        time.sleep(10*unit)



monitor_1min_thread = threading.Thread(target=monitor_1min, args=())
monitor_10mins_thread = threading.Thread(target=monitor_10mins, args=())

monitor_1min_thread.start()
monitor_10mins_thread.start()

monitor_1min_thread.join()
monitor_10mins_thread.join()
