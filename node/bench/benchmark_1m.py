import threading
import queue
import requests
import os
import uuid
import subprocess
import shlex
import json
import time
import random
import math
import re
from pytube import Playlist, YouTube
import boto3


Is_Debug = True

start_time = time.time()
        
# The maximum length of audio that can be processed; audio longer than this will be chunked
MAX_LENGTH = 600  # 60, 600, 900 seconds; 

INITIAL_AUDIO_NUMBER = 50   # max length limit for low-performance nodes
NORMAL_AUDIO_NUMBER  = 100  # max length limit for regular nodes

MAX_AUDIO_NUMBER = INITIAL_AUDIO_NUMBER # transcripting queue length limit
                       
NUMBER_DL_THREAD = 3 # 1, 2, 3, 4, 5; The maximum number of downloading threads 
BATCH_SIZE = 3 # 3, 4, 5;  Receive 3 jobs per time from the job queue

TOTAL_LENGTH = 0  # total video duration processed
TOTAL_NUMBER = 0  # total video number processed
BigBuffer = {}   # buffer to keep the generated texts for merging


# The directory for chunking, each thread has a its own folder inside this directory
audio_dir = os.getenv("AUDIO_DIR", "./audio")
download_dir = os.path.abspath(audio_dir)
os.makedirs(download_dir, exist_ok=True)


# the inference server: inference & health check
api_url = "http://localhost:8000/asr"
hc_url = "http://localhost:8000/hc"


# Access to the Job Queue System
queue_url = os.getenv("QUEUE_URL","")
aws_id = os.getenv("AWS_ID", "")
aws_key = os.getenv("AWS_KEY", "")


# Access to the Job Reporting System
benchmark_url = os.getenv("REPORTING_API_URL", "")
benchmark_id = os.getenv("BENCHMARK_ID", "")
benchmark_auth_header = os.getenv("REPORTING_AUTH_HEADER", "")
benchmark_auth_value = os.getenv("REPORTING_API_KEY", "")
benchmark_headers = {
    benchmark_auth_header: benchmark_auth_value,
}

#  Access to the Cloud Storage for generated assets
cloudflare_url = os.getenv("CLOUDFLARE_URL", "")
cloudflare_id = os.getenv("CLOUDFLARE_ID", "")
cloudflare_key = os.getenv("CLOUDFLARE_KEY", "")


# Create queues
transcribing_queue = queue.Queue()
reporting_queue = queue.Queue()


def get_access_cloudflare():
    if cloudflare_id == "":
        return None,'test/'

    s3 = boto3.client(
        service_name ="s3",
        endpoint_url = cloudflare_url,
        aws_access_key_id = cloudflare_id,
        aws_secret_access_key = cloudflare_key,
        region_name="auto"
    )
    return s3,'test/'


def get_access_aws_sqs():
    sqs_client = boto3.client(
                    service_name ="sqs",
                    aws_access_key_id = aws_id,
                    aws_secret_access_key = aws_key,                
                    region_name="us-east-2")
    QueueUrl = queue_url

    return sqs_client,QueueUrl


def yt_download(url, download_dir, file_id, file_ext):
    if True:
        try:
            yt = YouTube(url)
            audio = yt.streams.filter(only_audio=True).first() 
            
            # print(audio.audio_codec)
            format = audio.audio_codec.split('.')[0]
            
            if format == "mp4a":
                #print("-------------------------> mp4a: " + url) # removed for test
                audio_file_name = f"{file_id}.mp4a"
                audio.download(output_path=download_dir, filename=audio_file_name)

                try: # convert mp4a into mp3
                    source  = os.path.join(download_dir, audio_file_name)
                    target = os.path.join(download_dir,f"{file_id}.{file_ext}")
                    command = f"ffmpeg -i {shlex.quote(source)} -y -acodec libmp3lame -aq 9 {shlex.quote(target)}"
                    subprocess.run(command, shell=True, check=True, stderr=subprocess.PIPE)
                    os.remove(source) # delete the 'mp4a' file and keep the mp3 file
                except subprocess.CalledProcessError as e:
                    print(f"An error occurred while converting the mp4a file: {e}", flush=True)
                    os.remove(source) # delete the mp4a file because it cannot be converted
                    return False# download ok, convert nok
            
            elif format == "opus":
                print("-------------------------> opus: " + url) # removed for test
                audio_file_name = f"{file_id}.opus"
                audio.download(output_path=download_dir, filename=audio_file_name)

                try:
                    source  = os.path.join(download_dir, audio_file_name)
                    target = os.path.join(download_dir,f"{file_id}.{file_ext}")
                    command = f"ffmpeg -i {shlex.quote(source)} {shlex.quote(target)}"
                    subprocess.run(command, shell=True, check=True, stderr=subprocess.PIPE)
                    os.remove(source) # delete the 'opus' file and keep the mp3 file
                except subprocess.CalledProcessError as e:
                    print(f"An error occurred while converting the opus file: {e}", flush=True)
                    os.remove(source) # delete the opus file because it cannot be converted
                    return False # download ok, convert nok
            
            else:  # assuming the remaining is mp3, not confirmed yet
                print("-------------------------> " + audio.audio_codec + ": " + url) # removed for test
                audio_file_name = f"{file_id}.{file_ext}"
                audio.download(output_path=download_dir, filename=audio_file_name)

        except Exception as error:
            print(f"An error occurred while downloading the metadata or audio file: {error}")
            # no downloaded file
            return False

    return True
    

def get_audio_length(file_path):
    command = f"ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 {shlex.quote(file_path)}"
    # print("get_audio_length: " +command)
    try:
        output = subprocess.check_output(command, shell=True, text=True).strip()
        return float(output) # return the length
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while getting the length of audio file: {e}", flush=True)
        return None


def job_delete(thread_ID, sqs_client, QueueUrl, receipt_handle):
    if aws_id == "":
        return

    try:  # protection for visibility timeout
        response = sqs_client.delete_message(
             QueueUrl=QueueUrl,ReceiptHandle=receipt_handle,
        )
        #print("Thread {} removing: ".format(thread_ID), end="")
        #print(response['ResponseMetadata']['HTTPStatusCode'])
    except Exception as e:
        print("Thread {} run into trouble:".format(thread_ID))
        print(str(e))
        

def job_download(thread_ID, sqs_client, QueueUrl):    
    result = []

    while True:
        if aws_id == "":
            print("No way to get jobs!")
            time.sleep(10)
            continue

        try:
            response = sqs_client.receive_message(
                    QueueUrl=QueueUrl, MaxNumberOfMessages=BATCH_SIZE, WaitTimeSeconds=10)

            temp = response.get("Messages", []) # return [] if empty
            if len(temp)== 0:
                continue
    
            for message in temp:
                temp1 = json.loads(message["Body"])
                temp2 = message['ReceiptHandle']
                result.append([ temp1,temp2 ])
                #print("Thread {} downloading: ".format(thread_ID), end="")
                #print(temp1)

            break

        except Exception as e:
            print("Thread {} run into trouble:".format(thread_ID))
            print(str(e))
        
    return result


def downloader(transcribing_queue, thread_ID="0"):

    sqs_client,QueueUrl = get_access_aws_sqs()

    temp_dir = download_dir + "/thread" + thread_ID    
    os.makedirs(temp_dir, exist_ok=True)

    while True:
 
        while transcribing_queue.qsize() >= MAX_AUDIO_NUMBER: #  MAX_AUDIO_NUMBER of audio and json files allowed
            time.sleep(NUMBER_DL_THREAD)  # Stop downloading if so many downloaded audio files are waiting for transcription.

        batch_jobs = job_download(thread_ID,sqs_client,QueueUrl)

        for onejob in batch_jobs:  
        
            file_url = onejob[0]['video_url']
            asset = onejob[0]['asset']
            vid = onejob[0]['vid']
            mID = onejob[1]

            file_ext = "mp3"   # for YouTube, the final audio format should be *.mp3
            file_id = str(uuid.uuid4())  # Unique ID for each URL 
            audio_file_name = f"{file_id}.{file_ext}"
            downloaded_file_path = os.path.join(download_dir, audio_file_name)

            temp = yt_download(file_url, download_dir, file_id, file_ext) # Download the file_url
            if temp == False:
                print(onejob[0])
                job_delete(thread_ID, sqs_client, QueueUrl, mID) # bad video, delete the job
                continue # go back to the above for loop

            clip_length = get_audio_length(downloaded_file_path)          # Get the length
            if clip_length is None:
                #print(f"{file_url} is invalid and failed to get its length.")
                print(onejob[0])
                job_delete(thread_ID, sqs_client, QueueUrl, mID) ## bad video, delete the job
                os.remove(downloaded_file_path) 
                continue # go back to the above for loop

            if clip_length <= MAX_LENGTH:
                
                clip_length = round(clip_length,1)

                job = {     "file_id": file_id,
                            "clip_number": 1, # not chunking
                            "file_url": file_url,
                            "downloaded_file_path": downloaded_file_path,
                            "clip_length": clip_length, # integer
                            "clip_id": 0,  # # integer，0 ~ clip_number-1
                            "message_id": mID,
                            "vid": vid,
                            "asset": asset
                        } 
                transcribing_queue.put(job)  # Put the path of the downloaded file in the queue

            else: # chunking 
                # print(f"{file_url} is too long and exceeds {MAX_LENGTH} seconds, chunking it ...")

                clip_length = int(clip_length)  # 600.01 -> 600

                # %3d, supporting 000 ~ 999, total 1000 clips;  1000 x 10 min = 166 hours
                temp_file_path= os.path.join(temp_dir, f"%3d.{file_ext}")
                command = f"ffmpeg -i {shlex.quote(downloaded_file_path)} -f segment -segment_time {MAX_LENGTH} -c copy {shlex.quote(temp_file_path)}"

                try:
                    subprocess.run(command, shell=True, check=True, stderr=subprocess.PIPE)

                    files = os.listdir(temp_dir)
                    files.sort()  # make sure 000, 001, 002 ....
                    
                    clip_number =  int ( math.ceil(clip_length / MAX_LENGTH) )
                    clip_id = -1

                    for file_name in files:
                        tSource = os.path.join(temp_dir, file_name)
                        tName = file_name.split(".")[0]
                        tTarget = os.path.join(download_dir,f"{file_id}-{tName}.{file_ext}")
                        os.rename(tSource,tTarget)
 
                        if clip_length >= MAX_LENGTH:
                            current_length = MAX_LENGTH
                            clip_length = clip_length - MAX_LENGTH
                        else:
                            current_length = clip_length # the real length of last clip
                            if current_length < 1:       # drop the last clip if too small
                                os.remove(tTarget)
                                continue

                        clip_id = clip_id + 1

                        job = {   "file_id": file_id, 
                                  "clip_number": clip_number, # integer
                                  "file_url": file_url,
                                  "downloaded_file_path": tTarget,
                                  "clip_length": current_length, # integer
                                  "clip_id": clip_id, # integer，0 ~ clip_number-1
                                  "message_id": mID,
                                  "vid": vid,
                                  "asset": asset
                                }                         
                        transcribing_queue.put(job)  # Put the path of the downloaded file in the queue

                except subprocess.CalledProcessError as e:
                    print(f"An error occurred while chunking the long audio file: {e}", flush=True)
                    print(onejob[0])
                    job_delete(thread_ID, sqs_client, QueueUrl, mID) 
                    # chunking failed,  delete the job

                os.remove(downloaded_file_path) # 
                

def caller(transcribing_queue, reporting_queue):
     
    global MAX_AUDIO_NUMBER 
    
    sqs_client,QueueUrl = get_access_aws_sqs()

    eStart = time.perf_counter()
    Number_of_Failure = 0
    
    pStart = time.perf_counter()
    Sum_RTF = 0
    Number_of_RTF = 0

    while True:

        # Get the next file path from the queue
        metadata = transcribing_queue.get() # Get a metadata from the queue
        if metadata is None:
            # Sentinel value received, stop the thread
            break

        try:
            response = requests.post( # Sync Operation
                api_url, json={ "url": metadata["downloaded_file_path"], 
                                "clip_length": metadata["clip_length"], 
                                "clip_number": metadata["clip_number"],
                                "clip_id": metadata["clip_id"],
                                "asset": metadata["asset"],
                                "vid": metadata["vid"],
                                "qsize":transcribing_queue.qsize() } )
        except Exception as e:  
            print(str(e),flush=True)
            os._exit(1) # 

        if response.status_code != 200: 

            Number_of_Failure = Number_of_Failure + 1
            eEnd = time.perf_counter()

            if (eEnd - eStart) <= 300 and Number_of_Failure >= 1:
                print("More than 1 errors within 5 minutes on the api server !",flush=True)
                os._exit(1)
            if (eEnd - eStart) > 300:
                eStart = time.perf_counter()
                Number_of_Failure = 0

            print(f"Error: {response.status_code} {response.reason}")
            print(response.text)  


            job_delete("C0", sqs_client, QueueUrl, metadata["message_id"]) 
            transcribing_queue.task_done() # the current job is processed but failed
            os.remove(metadata["downloaded_file_path"]) # remove the audio file
                                               
            continue

        body = response.json() # contains the generated transcription
        result = {
            "downloaded_file_path": metadata["downloaded_file_path"],  # will drop it
            "text": body["text"],                            
            "file_id": metadata["file_id"],                  # will drop it
            "clip_number": metadata["clip_number"],          # will drop it
            "audio-url": metadata["file_url"],               # change key name
            "clip_id": metadata["clip_id"],                  # will drop it
            "audio-length": metadata["clip_length"],          # change key name 
            "message_id": metadata["message_id"],
            "vid": metadata["vid"],
            "asset": metadata['asset']
        }

        for header in [
            "x-salad-machine-id",
            "x-salad-container-group-id",
            "x-gpu-name",
            "x-model-id",
            "x-processing-time",
            "x-realtime-factor"
        ]:
            result[header.replace("x-", "")] = response.headers.get(header) # HTTP header names are not case sensitive    


        transcribing_queue.task_done() # the current job is handled and done

        reporting_queue.put(result) # Because it is done, report the job info to the DyanmoDB    


        # should move the below code to the reporter thread
        Number_of_RTF = Number_of_RTF + 1
        Sum_RTF = Sum_RTF + float(result['realtime-factor'])
        pEnd = time.perf_counter()

        if (pEnd - pStart) >= 300: #300:
            Average_RTF_5MIN = Sum_RTF / Number_of_RTF

            pStart = time.perf_counter()
            Sum_RTF = 0
            Number_of_RTF = 0

            if Average_RTF_5MIN <= 10:
                print("Too slow, bad node !",flush=True)
                os._exit(1)
            elif Average_RTF_5MIN <= 50:    #  10 < RTF <= 50  
                MAX_AUDIO_NUMBER = INITIAL_AUDIO_NUMBER   # 50
            else:    
                MAX_AUDIO_NUMBER = NORMAL_AUDIO_NUMBER    # 100
     

def Merger(result):
    key = result['file_id']
    if key in BigBuffer.keys():  
        
        length = len(BigBuffer[key])
        

        if length >= result['clip_number'] - 1: # the result will be the last one
            audio_length = 0.0
            processing_time = 0.0
            word_count = 0
            text = ""
            realtime_factor = 0.0

            # calculate the history without the last one
            for i in range(length):
                audio_length = audio_length + BigBuffer[key][i]['audio-length']
                processing_time = processing_time + float(BigBuffer[key][i]['processing-time'])
                word_count = word_count + BigBuffer[key][i]['word-count']
                text = text + " " + BigBuffer[key][i]['text']

            # add the last one         
            audio_length = audio_length + result['audio-length']
            processing_time = processing_time + float(result['processing-time'])
            word_count = word_count + result['word-count'] 
            text = text + " " + result['text'] 
            realtime_factor = audio_length / processing_time

            # use the last one to return
            result['audio-length'] = audio_length
            result['processing-time'] = str(processing_time)
            result['word-count'] = word_count
            result['text'] = text
            result['realtime-factor'] = str(realtime_factor)

            # clear the buffer
            del BigBuffer[key]
            return result  # use the last clip to return the entire audio 
        else:                                   # the result is not the last one
            BigBuffer[key].append(result)
            return None

    else:
        BigBuffer[key] = []                     # the result is the first one    
        BigBuffer[key].append(result)
        return None


def asset_upload(thread_ID, s3, prefix,asset,text):
    if cloudflare_id == "":
        return ""

    localfile  = os.path.join(download_dir,asset)

    with open(localfile,"w") as f:
        f.write(text)
    
    try:
        s3.upload_file(localfile,"transcripts", prefix+asset)
        os.remove(localfile)
        return "https://salad-public-transcripts.com/"+prefix+asset  # 
    
    except Exception as e:
        print("Thread {} run into trouble:".format(thread_ID))
        print(str(e))

    os.remove(localfile)

    return ""         
    

def reporter(reporting_queue):
    global TOTAL_LENGTH,TOTAL_NUMBER

    sqs_client,QueueUrl = get_access_aws_sqs()
    s3, prefix = get_access_cloudflare()

    while True:

        # Get the next file path from the queue
        result = reporting_queue.get()
        if result is None:
            # Sentinel value received, stop the thread
            break


        os.remove(result["downloaded_file_path"]) # remove the audio file
        del result["downloaded_file_path"]


        result["word-count"] = len(result["text"].split()) # move here because it takes time
 
        if result["clip_number"] > 1:
            result = Merger(result)
            if result == None: 
                reporting_queue.task_done() # Mark the task as done
                continue
        
        # Case 1： result - short audio
        # Case 2： result - merged result from chunked long audio

        del result["file_id"]
        del result["clip_number"]
        del result["clip_id"]

        if Is_Debug == True:
            print(result["text"])

        cloudflare_url = asset_upload("R0", s3, prefix, result["asset"], result["text"])
        result["transcript"] = cloudflare_url 

        del result["text"]
        del result["asset"]  
        
        job_delete("R0", sqs_client, QueueUrl, result["message_id"])
        del result["message_id"]

        TOTAL_LENGTH = TOTAL_LENGTH + result['audio-length'] 
        TOTAL_NUMBER = TOTAL_NUMBER + 1

        if Is_Debug == True:
            print(result)
            #print(result["vid"],", ",result['audio-url'],", ",result['audio-length'],", ",result['word-count'],", ",result['transcript'])
            
        if benchmark_url != "":
            requests.post( f"{benchmark_url}/{benchmark_id}",
                           json=result,headers=benchmark_headers)
        
        reporting_queue.task_done() # Mark the task as done
    

def monitor(transcribing_queue, reporting_queue):
    while True:
        time.sleep(5)
        print("Transcribing Q: {}, Reporting Q: {}".format(transcribing_queue.qsize(),reporting_queue.qsize()), end="" )
        print(", BigBuffer: {}, Video Number: {}, Duration {} hours".format(len(BigBuffer), TOTAL_NUMBER, round(TOTAL_LENGTH/3600,2)))


def wait_for_healthcheck_to_pass():
    max_retries = 500  # the warm-up process may take time
    tries = 0
    while tries < max_retries:
        tries += 1
        try:
            print("Healthcheck {}/500".format(tries),flush=True)
            response = requests.get(hc_url)
            if response.status_code == 200:
                print("Healthcheck passed !",flush=True)
                return
        except Exception as e:
            pass
        time.sleep(1)

    print("Healthcheck failed !!!",flush=True)
    os._exit(1)
    #raise Exception("Healthcheck failed") # the process will be terminated
    

# Create and start the downloader threads
downloader_threads = []
for x in range(NUMBER_DL_THREAD):
    downloader_thread = threading.Thread(target=downloader, args=(transcribing_queue, str(x)))
    downloader_thread.start()
    downloader_threads.append(downloader_thread)


wait_for_healthcheck_to_pass()
MAX_AUDIO_NUMBER = NORMAL_AUDIO_NUMBER # 

#temp=input("--------------------------------> continue the caller")


if Is_Debug:
    monitor_thread = threading.Thread(target=monitor, args=(transcribing_queue, reporting_queue))
    monitor_thread.start()


caller_thread = threading.Thread(target=caller, args=(transcribing_queue, reporting_queue))
caller_thread.start()


reporter_thread = threading.Thread(target=reporter, args=(reporting_queue,))
reporter_thread.start()


# Wait for the downloader threads to finish
for x in range(NUMBER_DL_THREAD):
    downloader_threads[x].join()
    

# Once downloading is finished, use a sentinel value to stop the uploader thread
transcribing_queue.put(None)    
caller_thread.join()

reporting_queue.put(None)
reporter_thread.join()


end_time = time.time()
print("Totally {} s, {:.3f} hours; took {:.3f} s, {:.3f} hours".format( TOTAL_LENGTH, TOTAL_LENGTH/3600, end_time-start_time, (end_time-start_time)/3600))

print("---------> The BigBuffer", end=" ")
print(BigBuffer)