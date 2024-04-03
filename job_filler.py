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
from datetime import datetime
from job_queue_access import get_access_aws_sqs


sqs_client, QueueUrl = get_access_aws_sqs()


NUMBER_UL_THREAD = 1  # 1 for test, 3 should be enough
BATCH_SIZE = 10
MAX_JOB_NUMBER = 100000  # traffic controll on the local job queue

MIN_DURATION = 1
MAX_DURATION = 48000


index_file_dir = "./"
index_file = "index.csv"
content_dir = "./metadata"


# Check the index file and metadata files
def consistency_check():
    index_file_path = os.path.join(index_file_dir,index_file)
    with open(index_file_path,'r') as f:   
        source = list(csv.reader(f))    
    if source == []:
        print("Something goes wrong!")
        exit(0)
    print("{} Channel files".format( len(source)-1 ), end=", " )

    for one_chan in source[1:]:
        content_file_path = os.path.join(content_dir,one_chan[5])
        Is_existed = os.path.exists(content_file_path) # 
        if Is_existed == False:
            print("---------- Error!!!")
            exit(0)
    print("everything seems good!")


# Filter the content based on channels
def Good_Channel(row):
    Bad_Channel = [
        ['UC1-g8gBGF8OneJh4geY7EDA',' not English'],
        ['UChlI_ODgqa8bcTfl96dkIDw',' not English'],
        ['UC9I5l3CeCsxFdE0p0USWC9g',' not English'],
        ['UCUauaBIGBgiFwm1qC1jOjGw',' not English'],
        ['UCVQ7wwm1vvieyFDkqyx5ZWg',' not English'],
        ['UCMNVOY2PTgn_tUDLSlj7HMg',' not English'],
      	['UC_aEa8K-EOJ3D6gOs7HcyNg',' music'      ],
        ['UC0q47XTdvpfagxyaAovFP5w',' not English'],
        ['UC_QWxA44l36ZSSzVPxPNxBw',' not English'],
        ['UCKmfJkQCC8ny2KeDk-1Ldww',' not English'],
        ['UC8dnBi4WUErqYQHZ4PfsLTg',' not English'] ]
      
    for x in Bad_Channel:
        if row[3] == x[0]:
            #print(row)  
            return False
    return True


# Check the content：based on good channels, min duration and max duration
def content_check(min=1, max= 36000):
    index_file_path = os.path.join(index_file_dir,index_file)
    with open(index_file_path,'r') as f:   
        source = list(csv.reader(f))    
    print("Channel Number: {}".format( len(source)-1 ), end=", " )
    total_number = 0
    total_duration = 0
    shortest = min
    longest = max
    for one_chan in source[1:]:
        if not Good_Channel(one_chan):
            continue
        content_file_path = os.path.join(content_dir,one_chan[5])
        with open(content_file_path,'r') as fc:   
            channel = list(csv.reader(fc))    
            for video in channel[1:]:
                duration = int(video[2])

                if duration < shortest:
                    shortest = duration
                if duration > longest:
                    longest = duration

                if duration < min  or duration > max:
                    continue

                total_number = total_number + 1
                total_duration = total_duration + duration     
    print("Total Video Number: {}, Total Video Duration: {:.2f} hours".format(total_number,total_duration/3600))
    print("Shortest Duration: {:.4f}, Longest Duration: {:.4f} hours".format(shortest/3600,longest/3600))


# Return a job based on good channels, min duration and max duration
# Adopt the circular queue and never stop
def Get_A_Job():
    index_file_path = os.path.join(index_file_dir,index_file)
    with open(index_file_path,'r') as fi:   
        source = list(csv.reader(fi))     

        begin = 1                   
        end = len(source) - 1       
        count = begin - 1
        
        while True:
            count = count + 1
            if count > end:   
                count = begin

            row = source[count]                              
            if not Good_Channel(row):                        # filter on channels
                continue

            file_ID = "{0:04d}".format(count)

            content_file_path = os.path.join(content_dir,row[5])

            with open(content_file_path,'r') as fc:   
                metadata = list(csv.reader(fc))    
            
            for video_metadata in metadata[1:]:                       

                duration = int(video_metadata[2])            # filter on min duration and max duration
                if duration < MIN_DURATION or duration > MAX_DURATION:
                    continue

                video_ID = video_metadata[5]
                playlist_ID = video_metadata[8]
                asset= f"{file_ID}_{playlist_ID}_{video_ID}.txt" # define the output_filename for transcrit

                yield video_metadata[0], duration, asset  # video URL, duration(second), output_filename


# Generate jobs based hours or number, and add them into the local job queue
#    0，1000  -  generate 1000 jobs (video URLs)
#   10,    0  -  generate 10 hours of jobs (video URLs)
def Job_Filler(hours, number, job_queue): 
    last_position = 0
    with open("last_position",'r') as f:   
        last_position = int(f.read())   # load the last postion

    total_number = 0
    total_duration = 0

    index = 0

    for video_url, duration, asset in Get_A_Job():

        index = index + 1

        if index <= last_position:   # generate new jobs based on the last postion
            continue

        total_number = total_number + 1
        total_duration = total_duration + duration
        
        job = { "vid": index, "video_url": video_url, "duration": duration, "asset": asset }        
        
        job_queue.put(job)  # add it into the local job queue

        while job_queue.qsize() >= MAX_JOB_NUMBER: # traffic control
            #print("--------> The queue is full")
            time.sleep(1)  

        if hours != 0 and total_duration >= hours * 3600:
            break          # expected jobs have been generated, hours
        if number != 0 and total_number >= number:
            break          # expected jobs have been generated, number

    for x in range(NUMBER_UL_THREAD):    # Notify the job uploader threads to stop
        job_queue.put(None)   

    print(">>>>>>>>>>>>>>>>> ", end="")
    print("{} videos with {:.2f} hours are put into the local job queue.".format(total_number,total_duration/3600))

    last_position = index
    with open("last_position",'w') as f:   
        f.write(str(last_position))  # save the postion

    return total_number # return the numger of generated jobs 
           

def Batch_Send(thread_ID, sqs_client, QueueUrl, entries):
    response = sqs_client.send_message_batch(QueueUrl=QueueUrl,Entries=entries)
    '''
    if "Successful" in response:
        print("Thread {} has successfully sent:".format(thread_ID))
        for msg_meta in response["Successful"]:
            for x in entries:
                if x['Id'] == msg_meta['Id']:
                    print(x['MessageBody'])
    if "Failed" in response:
        print("Thread {} failed to send:".format(thread_ID))
        for msg_meta in response["Failed"]:
            for x in entries:
                if x['Id'] == msg_meta['Id']:
                    print(x['MessageBody'])
    '''


# Read the jobs from the local job queue, and send them to AWS SQS
def job_upload(job_queue, thread_ID="0"):  
    
    sqs_client,QueueUrl = get_access_aws_sqs()

    RID = str(uuid.uuid4()) 
    total_job_number = 0
    total_duration = 0
    Is_Finish = False

    while True:

        entries = []
        for count in range(BATCH_SIZE): 
            job = job_queue.get()  # block here
            job_queue.task_done()

            if job is None:
                Is_Finish = True    
                break

            total_job_number = total_job_number + 1

            if (total_job_number % 1000) == 0:  # traffic controll to AWS SQS
                print(total_job_number)
                time.sleep(1)

            total_duration = total_duration + job["duration"]

            uid = RID + "-thread" + thread_ID + "-no" + str(total_job_number)
            entries.append(  {"Id": uid,
                             "MessageBody": json.dumps(job),
                             "MessageAttributes": {},
                             "MessageDeduplicationId": uid, 
                             "MessageGroupId": uid} )

        if len(entries) > 0:  # send a batch of jobs to AWS SQS
            Batch_Send(thread_ID,sqs_client,QueueUrl,entries) # block

        if Is_Finish == True: # All generated jobs have been sent to AWS SQS
            break


    print("----------------> Thread {} sent {} jobs with duration of {:.2f} hours.".format(thread_ID,total_job_number,total_duration/3600),flush=True)

    return


# Get total jobs, available jobs and invisible jobs from AWS SQS
def get_Queue_Status():
    sqs_client,QueueUrl = get_access_aws_sqs()
    result = sqs_client.get_queue_attributes(QueueUrl=QueueUrl, AttributeNames=['All'])
    x = result['Attributes']['ApproximateNumberOfMessages']
    y = result['Attributes']['ApproximateNumberOfMessagesNotVisible']
    temp = int(x) + int(y)
    return temp,int(x),int(y)


# Create the local job queue, Create the uploader threads, and generate jobs
def Generate_jobs(duration, video_number):

    job_queue = queue.Queue()

    ul_threads = []
    for x in range(NUMBER_UL_THREAD):  
        ul_thread = threading.Thread(target=job_upload, args=(job_queue, str(x)))
        ul_thread.start()
        ul_threads.append(ul_thread)

    start = time.perf_counter()

    temp = Job_Filler(duration,video_number,job_queue) # duration，number

    for x in range(NUMBER_UL_THREAD):
        ul_threads[x].join()

    end = time.perf_counter()

    print(">>>>>>>>>>>>>>>>> All jobs are sent to AWS",end=", ")
    print("Time: {:.2f} seconds".format( end-start ),end=", ")
    print("Efficiency: {:.2f} jobs/second".format(temp/(end-start)),flush=True)
    print()


def Send_Monitor_Replenish_jobs(duration, video_number, min_job_number, new_inject_hours):
    
    Generate_jobs(duration,video_number)

    injected = 0
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    inject_hours = new_inject_hours # hours

    with open("last_position",'r') as f:   
        last_position = int(f.read())   

    while True:

        print("Have injected {} hours of video URLs since {}, and the current postion is {}.".format(injected,dt_string,last_position))
        time.sleep(60)
    
        total, available, invisible = get_Queue_Status()
        if available <  min_job_number:  # jobs  
            print()
            print("Warning: only {} Available jobs, {} jobs being processed, totally {} jobs in the queue.".format(available, invisible, total))
            print("Starting to inject {} hours of video URLs ...... ".format(inject_hours))
            print()

            Generate_jobs(inject_hours,0)  # inject hours
            injected = injected + inject_hours

            with open("last_position",'r') as f:   
                last_position = int(f.read())   



if __name__ == "__main__":

            
    consistency_check()
    content_check(MIN_DURATION,MAX_DURATION)
    
    #Send_Monitor_Replenish_jobs(1,0,10,1)


    Generate_jobs(0,50)