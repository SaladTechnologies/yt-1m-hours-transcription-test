import speedtest
from pythonping import ping
import logging


def network_test(Is_Debug = True):
    dlspeed = 0
    ulspeed = 0
    latency = 0
    country = ""
    location = ""
    print("Test the network speed .....", flush=True)

    if Is_Debug == True:
        return country, location, latency, dlspeed, ulspeed

    try:
        speed_test = speedtest.Speedtest()
        bserver = speed_test.get_best_server()
        dlspeed = speed_test.download() / (1000 * 1000)  # Convert to Mbps
        ulspeed = speed_test.upload() / (1000 * 1000)  # Convert to Mbps
        latency = bserver['latency']
        country = bserver['country'] 
        location = bserver['name']
    except Exception as e:  
        logging.exception("Error during network test")
        print(str(e))

    return country, location, latency, dlspeed, ulspeed


def ping_test(Is_Debug=True, tCount=10):
    latency_useast1 = 0
    latency_eucentral1 = 0
    latency_youtube = 0
    print("Test the latency .....", flush=True)

    if Is_Debug == True:
        return latency_useast1, latency_eucentral1, latency_youtube

    try:
        temp = ping('ec2.us-east-1.amazonaws.com', interval=1, count=tCount, verbose=True)
        latency_useast1 = temp.rtt_avg_ms    
    except Exception as e:  
        logging.exception("Error during the ping test - us east 1")
        print(str(e))

    try:
        temp = ping('ec2.eu-central-1.amazonaws.com', interval=1, count=tCount,verbose=True)
        latency_eucentral1 = temp.rtt_avg_ms

    except Exception as e:  
        logging.exception("Error during the ping test - eu central 1")
        print(str(e))

    try:
        temp = ping('www.youtube.com', interval=1, count=tCount,verbose=True)
        latency_youtube = temp.rtt_avg_ms
    except Exception as e:  
        logging.exception("Error during the ping test - youtube")
        print(str(e))

    return latency_useast1, latency_eucentral1, latency_youtube

#g_country, g_location, g_latency, g_dlspeed, g_ulspeed = network_test()
#g_latency_useast1, g_latency_eucentral1, g_latency_youtube = ping_test()

#print(g_country, g_location, g_latency, g_dlspeed,g_ulspeed)
#print(g_latency_useast1, g_latency_eucentral1, g_latency_youtube)