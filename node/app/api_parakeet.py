import time
from pydantic import BaseModel, HttpUrl
from fastapi import FastAPI, Response, Request
import uvicorn
import os
from typing import Optional
from model_parakeet import load_model,get_cpu_gpu,show_cpu_gpu,model_id
from io import BytesIO
import torch
from __version__ import version
import logging
import requests
from network import network_test,ping_test


Is_Debug = True


g_start = time.perf_counter()
g_Infer_Number = 0

host = os.environ.get("HOST", "*")
port = int(os.environ.get("PORT", "8000")) # Called by the benchmark worker process

salad_machine_id = os.getenv("SALAD_MACHINE_ID", "")  # Passed by SaladCloud
salad_container_group_id = os.getenv("SALAD_CONTAINER_GROUP_ID", "") # Passed by SaladCloud


# Passed by SaladCloud
benchmark_url = os.getenv("REPORTING_API_URL", "")
benchmark_id = os.getenv("BENCHMARK_ID", "")
benchmark_auth_header = os.getenv("REPORTING_AUTH_HEADER", "")
benchmark_auth_value = os.getenv("REPORTING_API_KEY", "")
benchmark_headers = {
    benchmark_auth_header: benchmark_auth_value,
}


# Test the network 
g_country, g_location, g_latency, g_dlspeed, g_ulspeed = network_test(Is_Debug)
g_latency_useast1, g_latency_eucentral1, g_latency_youtube = ping_test(Is_Debug,10)


# Read the state before loading the model 
g_cpu_utilization, g_total_ram, g_available_ram1, g_gpu_utilization, g_total_vram, g_available_vram1, _ = get_cpu_gpu()


start = time.perf_counter()
asr_model = load_model(True)   # Load the model
end = time.perf_counter()
print(f"Initialized ASR Pipeline in { round(end - start,2) } seconds", flush=True)


def get_gpu_name():
    if torch.cuda.is_available():
        return torch.cuda.get_device_name(0)
    else:
        return "CUDA is not available"


gpu_name = get_gpu_name()


default_response_headers = {
    "X-Salad-Machine-ID": salad_machine_id,
    "X-Salad-Container-Group-ID": salad_container_group_id,
    "X-GPU-Name": gpu_name,
    "X-Model-ID": model_id,
}


# Read the state after the model is loaded 
_, _, g_available_ram2, _, _, g_available_vram2, g_process_used_vram = get_cpu_gpu()


g_end = time.perf_counter()


result = {
    "salad-machine-id": salad_machine_id,
    "salad-container-group-id": salad_container_group_id,
    "model-id": model_id,
    "gpu-name": gpu_name,
    "cpu-utilization":   "{:.3f}".format(g_cpu_utilization),   # The initial GPU usage
    "total-ram":         "{:.3f}".format(g_total_ram),
    "available-ram1":    "{:.3f}".format(g_available_ram1),
    "available-ram2":    "{:.3f}".format(g_available_ram2),    # After the model is loaded and the first inference is done
    "gpu-utilization":   "{:.3f}".format(g_gpu_utilization),   # The initial GPU usage
    "total-vram":        "{:.3f}".format(g_total_vram),
    "available-vram1":   "{:.3f}".format(g_available_vram1),   # The inital GPU VRAM usage
    "available-vram2":   "{:.3f}".format(g_available_vram2),   # After the model is loaded and the first inference is done
    "process-used-vram": "{:.3f}".format(g_process_used_vram), # After the model is loaded and the first inference is done
    "model-loading-time": "{:.3f}".format(end - start),
    "init-time":          "{:.3f}".format(g_end - g_start),   
    "country":                            g_country,
    "location":                           g_location,
    "latency":            "{:.3f}".format(g_latency),
    "dlspeed":            "{:.3f}".format(g_dlspeed),
    "ulspeed":            "{:.3f}".format(g_ulspeed),
    "latency-useast1":    "{:.3f}".format(g_latency_useast1),
    "latency-eucentral1": "{:.3f}".format(g_latency_eucentral1),
    "latency-youtube":    "{:.3f}".format(g_latency_youtube)  
} 


if benchmark_url == "":
    print(result)
else:
    print(result)
    benchmark_id = benchmark_id + "-is"
    requests.post( f"{benchmark_url}/{benchmark_id}",
                   json=result,headers=benchmark_headers )


app = FastAPI(
    title="Automatic Speech Recognition API",
    description="A minimalist, performance-oriented inference server for automatic speech recognition.",
    version=version )


@app.get("/hc")
async def healthcheck():
    global g_Infer_Number
    g_Infer_Number = 0
    return {"status": "ok", "version": version}


class ASRRequest(BaseModel):
    url: HttpUrl


@app.post("/asr")
async def asr(request: Request, response: Response):
    global g_Infer_Number

    start = time.perf_counter()

    try:
        body = await request.json()
        if "url" not in body or "clip_length" not in body:
            raise Exception("Missing required field 'url' or 'clip_length'")
        input_file = body["url"]         # local file 
        audio_len = body["clip_length"]  # to calculate the realtime factor here
        vid = body["vid"]
        asset = body["asset"]
        clip_number = body["clip_number"]
        clip_id = body["clip_id"]
        qsize = body["qsize"]

    except Exception as e:   
        logging.exception("Wrong input")        
        response.status_code = 400
        return {"error": str(e)} # return error early
    
    try:
        
        result = asr_model.transcribe([input_file],channel_selector='average',verbose=False)

        '''
        if Is_Debug == False:
            result = asr_model.transcribe([input_file],channel_selector='average',verbose=False)
        else:
            time.sleep(audio_len/80)
        '''
        #print(result[0][0]) # No.0 value of No.0 list
        #show_gpu_vram()

    except Exception as e:  # May run out of VRAM and trigger the exception, or some audio formats are not supported!
        logging.exception("Error during ASR inference")
        response.status_code = 500
        return {"error": str(e)} # return error early
    
    end = time.perf_counter()

    g_Infer_Number = g_Infer_Number + 1
    temp_rtf = (audio_len / (end - start))

    print(f"{vid}, {asset} ({clip_id}/{clip_number}), L {audio_len}s, No {g_Infer_Number} in {round(end - start,1)}s, RTF {int(temp_rtf)}, Q {qsize}", flush=True)

    response.headers.update(default_response_headers)
    response.headers["X-Processing-Time"] = str(end - start)
    response.headers["X-Realtime-Factor"] = str(temp_rtf)

    return {'text':result[0][0]}

    '''
    if Is_Debug == False:
        return {'text':result[0][0]} # return the transcription
    else:
        temp_text = ""
        for x in range (int(audio_len * 2)):
            temp_text = temp_text + "test "
        return {'text': temp_text} 
    '''

if __name__ == "__main__":
    uvicorn.run(app, host=host, port=port)
