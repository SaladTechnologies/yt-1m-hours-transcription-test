import boto3
import os

def get_access_aws_sqs():
    aws_id = os.getenv("AWS_ID", "")
    aws_key = os.getenv("AWS_KEY", "")
    QueueUrl = os.getenv("QUEUE_URL","")

    sqs_client = boto3.client(
        service_name ="sqs",
        aws_access_key_id = aws_id,
        aws_secret_access_key = aws_key,
        region_name="us-east-2", # Must be one of: wnam, enam, weur, eeur, apac, auto
    )
    
    return sqs_client, QueueUrl

'''

'''


if __name__ == "__main__":

    sqs_client, QueueUrl = get_access_aws_sqs()
    result = sqs_client.get_queue_attributes(QueueUrl=QueueUrl, AttributeNames=['All'])
    print(result)



    # Create a queue
    
    ''''
    response = sqs_client.create_queue(
                QueueName="rx-test.fifo",
                Attributes={
                        "DelaySeconds": "0",
                        "MessageRetentionPeriod": "172800",  # 2 days
                        "VisibilityTimeout": "3600",  # 1 hour
                        "ReceiveMessageWaitTimeSeconds": "10",
                        "FifoQueue": "true",
                 }    
        
                )
    print(response)
    
    '''

    #QueueName =                                                    "rx-test.fifo"
    #QueueUrl  = "https://sqs.us-east-2.amazonaws.com/AWS_ACCOUNT_ID/rx-test.fifo"


    