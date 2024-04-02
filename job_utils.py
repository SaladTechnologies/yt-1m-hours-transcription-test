import os
import csv


content_dir = os.getenv("CONTENT_DIR", "./metadata")
g_index = [] # synced with the index.csv, to reduce access to the file system


# list all files within a folder, excluding hidden files
def list_visible_files_with_for_loop(directory):
    visible_files = []
    for file in os.listdir(directory):
        if not file.startswith('.'):
            visible_files.append(file)
    return visible_files


# Calculate the total metadata file number, total video number and total video duration from the folder
# Mainly for the consistency check
def video_summary_from_the_folder(): 
    total_duration = 0               
    total_number = 0
    filenames = list_visible_files_with_for_loop(content_dir)
    for fname in filenames:
        temp = fname[:-4]

        duration= float(temp.split("_")[-1])
        number = int(temp.split("_")[-2])

        total_duration = round(total_duration + duration,2)
        total_number = total_number + number
    print("File Number: {}, Video Number: {}, Duration: {} hours - checked by the folder".format(len(filenames),total_number,total_duration))


# Calculate the total metadata file number, total video number and total video duration from the index file and index variable
# Mainly for the consistency check
def video_summary():        
    total_duration = 0
    total_number = 0
    filename =  "index.csv"
    with open(filename,'r') as f:
        data = list(csv.reader(f))
        for x in data[1:]:                      
            total_duration = total_duration + float( x[4] )
            total_number = total_number + int( x[3] )
        total_duration = round(total_duration,2)    
        print("File Number: {}, Video Number: {}, Duration: {} hours - checked by the index file".format(len(data)-1,total_number,total_duration))

    total_duration = 0
    total_number = 0
    if len(g_index) != 0:
        for x in g_index[1:]:                  
            total_duration = total_duration + float( x[4] )
            total_number = total_number + int( x[3] )
        total_duration = round(total_duration,2)    
        print("File Number: {}, Video Number: {}, Duration: {} hours - checked by the index variable".format(len(g_index)-1,total_number,total_duration))


# Check whether a channel is already existed
# Load the index.csv into the index variable for the first time        
def Is_Existed(channel_id):
    if len(g_index) == 0:
        filename =  "index.csv"
        with open(filename,'r') as f:
            data = list(csv.reader(f)) 
            for x in data:                
                g_index.append(x) # load the index file
        
    if len(g_index) == 1:  # only contains the column names without channel records
        return False

    for x in g_index[1:]:               
        if x[1] == channel_id:
            return True
        
    return False


# when a new metadata file is created, update the index file and the index variable
def Save_Index(fname, type="named"):    

    temp = fname[:-4]
    duration= float(temp.split("_")[-1]) 
    number = int(temp.split("_")[-2])          # get the duration and number from the metadata filename

    file_path = os.path.join(content_dir,fname)   
    with open(file_path,'r') as f:             # get the channel id and title from a record inside the metadata filename
        data = list(csv.reader(f))
        channel = [type, data[1][6].split("/")[-1], data[1][9], number, duration, fname,""]

        filename =  "index.csv"
        with open(filename,'a') as f:
            write = csv.writer(f)
            write.writerow(channel)  # update the index file
            g_index.append(channel)  # update the index variable


# Create a new metadata file is created, update the index file
def Save_Metadata(videos):
    fields = ["video_url","format","duration","playlist_url","video_number","video_no","channel_url","playlist_number","playlist_no","channel_title","playlist_title","video_title"]

    length = 0
    for video in videos:    # calculate the total video duration
        length = length + float(video[2])
    length = str(round(length / 3600,2))
    tempname = videos[0][9] # channel title
    number = len(videos)    # total video number

    filename =  f"{tempname}_{number}_{length}.csv"
    save_file_path = os.path.join(content_dir,filename)
    with open(save_file_path,'w') as f:
        write = csv.writer(f)
        write.writerow(fields)
        write.writerows(videos)

    return filename


# Not used anymore
# It is to create the index file based on existing metadata files
def Init():
    channels = []

    filenames = list_visible_files_with_for_loop(content_dir)
    for fname in filenames:
        temp = fname[:-4]
        duration= float(temp.split("_")[-1])
        number = int(temp.split("_")[-2])
        
        file_path = os.path.join(content_dir,fname)
        
        with open(file_path,'r') as f:
            data = list(csv.reader(f))
            channels.append(["named",data[1][6].split("/")[-1], data[1][9], number, duration, fname,""])

    fields = ["type","channel_id","channel_title","video_number","duration","filename","cloudflare_url"]

    filename =  "index.csv"
    with open("index.csv",'w') as f:
        write = csv.writer(f)
        write.writerow(fields)
        write.writerows(channels)


def Get_Topics():    
    file_path = "topics.csv"  
    with open(file_path,'r') as f:
        data = list(csv.reader(f))
        return data

#Init()


if __name__ == "__main__":
    #Init()
    video_summary_from_the_folder()
