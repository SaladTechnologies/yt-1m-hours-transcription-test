
from googleapiclient.discovery import build
from pytube import Playlist, YouTube
import time,os
import csv
from job_utils import Save_Metadata, Save_Index, Is_Existed, Get_Topics, video_summary
import multiprocessing


g_API_KEY = os.getenv("GCP_API_KEY")

temp_cache = os.getenv("TEMP_CACHE", "./temp_files")

# Manually add the channel title and its ID
named_channels = [
      ["Carnegie Mellon University","UCf-COwmdNelXvbj-KaMCuLw"],
      #["Massachusetts Institute of Technology (MIT)","UCFe-pfe0a9bDvWy74Jd7vFg"],
      # .....
      # .....
]


def rx_print(tList):
    for x in tList:
        print(x)


# Use YouTube Data API, get channels from a search
def get_channels_from_search(keywords, API_KEY):  

    youtube = build('youtube', 'v3', developerKey=API_KEY)
    channels = []
    first = True

    for x in range(4): # Get 4 x 50 channels; may fail because of network or quota issue

        if first:
            request = youtube.search().list( q=keywords, part='snippet', type='channel', 
                                             relevanceLanguage='en', maxResults=50 )
            response = request.execute()
            first = False
            totalResults = response['pageInfo']['totalResults']
        else:
            request = youtube.search().list( q=keywords, part='snippet', type='channel', 
                                             relevanceLanguage='en', maxResults=50, pageToken=next_page )
            response = request.execute()    
        
        for chan in response['items']:
            channels.append([chan['snippet']['title'],chan['id']['channelId']])
        
        if 'nextPageToken' in response:
            next_page = response['nextPageToken']
        else:
            break 

    return channels # return 200 channels, and could be empty []


# Use YouTube Data API, get playlists from a channel
def get_playlists_from_channel(channel_id, API_KEY):

    youtube = build('youtube', 'v3', developerKey=API_KEY)
    playlists = []
    first = True
    no = 0

    while True:  # Get all playlists withiin a channel; may fail because of network or quota issue

        if first:
            request = youtube.playlists().list( part='snippet,contentDetails', channelId=channel_id,maxResults=50 )
            response = request.execute()
            first = False
            totalResults = response['pageInfo']['totalResults']
        else:
            request = youtube.playlists().list( part='snippet,contentDetails', channelId=channel_id,maxResults=50,
                                                pageToken=next_page )
            response = request.execute()
            
        for pl in response['items']:
            playlistURL = f"https://www.youtube.com/playlist?list={pl['id']}"
            channelURL = f"https://www.youtube.com/channel/{pl['snippet']['channelId']}"
            playlists.append([playlistURL, channelURL,
                              totalResults, no,
                              pl['snippet']['channelTitle'], pl['snippet']['title'],
                             ])
            no = no + 1
        
        if 'nextPageToken' in response:
            next_page = response['nextPageToken']
        else:
            break 

    return playlists # return all playlist, and could be empty []
   

# Deprecated, very slow and not using multiprocessing approach to download video metadata from a channel
# Check, to check whether the 1st video for each playlist supports the audio codec.
def get_videos_from_channel(channel_id, API_KEY, check=False): 
    videos = []
    all_playlists = get_playlists_from_channel(channel_id,g_API_KEY)
    
    for plt in all_playlists: # could be []

        try:
            no = 0                
            temp_pl = Playlist(plt[0])
            for url in temp_pl.video_urls:
                yt = YouTube(url)
        
                if check == True:
                    if(no == 0):
                        video = yt.streams.filter(only_audio=True).first() 
                        format = video.audio_codec
                    else:
                        format = "_UNKNOWN_"
                else:
                    format = "_UNKNOWN_"

                temp = [ yt.watch_url, format, yt.length, 
                         plt[0], temp_pl.length, no, 
                         plt[1], plt[2], plt[3],
                         plt[4], plt[5], yt.title]

                if(no == 0):
                    print("Video: {}, Format: {}, Duration: {}, {}/{} {}/{}".format(temp[0],temp[1],temp[2],temp[5],temp[4],temp[8],temp[7]))

                videos.append(temp)
                no = no + 1

        except Exception as e:
            print(str(e))
            print("Playlist Metadata: ",end= "")
            print(plt)

    return videos  # cloud be []


# This function is run by 1 process
# Use Pytube, get video metadata from some playlists
def hard_worker(worker,input_file_path,output_file_path,check): # check is not used, doesn't check the audio codec to save time
    videos = []

    with open(input_file_path,'r') as f: # load the tasks, parts of the platlists within a channel
        all_playlists = list(csv.reader(f))
        for plt in all_playlists: # for each playlist
            try:
                no = 0                
                temp_pl = Playlist(plt[0])      # retrieve all video metadata within a playlist, taking time
                                                # will fail if a playlist contains only a video ??????
                
                for video in temp_pl.videos:  
                    format = "_UNKNOWN_"
                    temp = [ video.watch_url, format, video.length, 
                             plt[0], temp_pl.length, no, 
                             plt[1], plt[2], plt[3],
                             plt[4], plt[5], video.title]

                    if(no == 0): # print the metadata of 1st video for each playlist
                        print("Worker {} ---> 1st video: {}, duration: {}; Number: {}, Playlist: {}/{}".format(worker,temp[0],temp[2],temp[4],temp[8],temp[7]), flush=True)

                    videos.append(temp)
                    no = no + 1

            except Exception as e:
               print()
               print(str(e))
               print("Worker {} -------> Not avaliable: {}, Playlist: {}/{}".format(worker,plt[5],plt[3],plt[2]), flush=True)
               print()

    os.remove(input_file_path)  # remove the input file

    with open(output_file_path,'w') as f: # 
        write = csv.writer(f)            
        write.writerows(videos)   # the file will be empty if videos is []
    

# Run multiple processes (1~10) to download video metadata from a channel 
def multiprocess_get_videos_from_channel(channel_id, API_KEY, check=False):

    video_metadata = []
    my_processes = []

    all_playlists = get_playlists_from_channel(channel_id,API_KEY)
    playlist_number = len(all_playlists)

    if( playlist_number == 0):
        return video_metadata   # []
    elif playlist_number <= 3:    #  length <= 3, 1
        worker_number = 1
    elif playlist_number <= 8:    #  length <= 8, 2
        worker_number = 2
    elif playlist_number <= 15:   #  length <= 15, 3
        worker_number = 3
    elif playlist_number <= 25:   #  length <= 25, 4
        worker_number = 4
    elif playlist_number <= 35:   #  length <= 35, 5
        worker_number = 5
    elif playlist_number <= 45:   #  length <= 45, 6
        worker_number = 6
    elif playlist_number <= 55:   #  length <= 55, 7
        worker_number = 7
    elif playlist_number <= 75:   #  length <= 75, 8
        worker_number = 8
    elif playlist_number <= 100:  #  length <= 100, 9
        worker_number = 9
    else:                         # 
        worker_number = 10

    delta =  playlist_number / worker_number

    for count in range(worker_number):
        begin = round(count * delta)
        end =   round((count+1) * delta)
        playlists = all_playlists[begin:end]  # get parts of the playlists 
        input_file_path = os.path.join(temp_cache,f"input{count}.csv")    
        output_file_path = os.path.join(temp_cache,f"output{count}.csv")
        
        with open(input_file_path,'w') as f:  #  
            write = csv.writer(f)
            write.writerows(playlists)            

        temp = multiprocessing.Process(target=hard_worker, args=(str(count),input_file_path,output_file_path,check) )
        my_processes.append(temp)

    for count in range(worker_number):
        my_processes[count].start()  # start each child process
    for count in range(worker_number):
        my_processes[count].join()   # the main thread is waiting here

    for count in range(worker_number): # merge the results
        output_file_path = os.path.join(temp_cache,f"output{count}.csv")
        with open(output_file_path,'r') as f:    
            video_metadata = video_metadata + list(csv.reader(f))  # could be [] 
        os.remove(output_file_path)

    return video_metadata 


# Collect video metadata based on named channels
def Generate_From_Named_Channel():
    for c in named_channels:
        if not Is_Existed(c[1]):
            videos = multiprocess_get_videos_from_channel(c[1],g_API_KEY,check=True) # could be []
            if (len(videos) > 0):
                try:
                    fname = Save_Metadata(videos)  
                    Save_Index(fname, type='named') 
                    video_summary()
                    print("Generated: " +fname)
                except Exception as e:
                    print(str(e))
                    print("Something goes wrong when saving !!!!!")
            else:
                    print("Something goes wrong because no videos !!!!!")
        else:
            print("---------------------------> Error")
            print("This videos from channel have already been downloaded !!!!! ",end="")
            print(c)


# Collect video metadata using search
def Generate_From_Search():
    topics = Get_Topics()

    for topic in topics:
        noChan = 0                                            
        channels = get_channels_from_search(topic[0],g_API_KEY) # 200 channels, could be []
        for c in channels: 
            if not Is_Existed(c[1]):
                print()
                print("---------------------------> topic: {}, No: {}, ".format(topic[0],noChan),end="")
                print(c)
                videos = multiprocess_get_videos_from_channel(c[1],g_API_KEY,check=True) # cloud be []
                if (len(videos) > 0):
                    try:
                        fname = Save_Metadata(videos) 
                        Save_Index(fname, type=topic[0])  
                        video_summary()
                        print("Generated: " +fname)
                    except Exception as e:
                        print(str(e))
                        print("Something goes wrong when saving !!!!!")
                else:
                    print("Something goes wrong because no videos !!!!!")
            else:
                print("---------------------------> Error")
                print("This videos from channel have already been downloaded !!!!! ",end="")
                print(c)
        
            noChan = noChan + 1



if __name__ == "__main__":

    #Generate_From_Named_Channel() 
    Generate_From_Search()         