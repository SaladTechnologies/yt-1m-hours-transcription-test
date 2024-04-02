import nemo.collections.asr as nemo_asr
import torch
import time
import os
import psutil


def get_cpu_gpu():
    temp1gb = 1024.0 ** 3

    cu = psutil.cpu_percent()
    tRAM = psutil.virtual_memory()
    tr = tRAM.total/temp1gb
    ar = tRAM.available/temp1gb

    i = 0
    gu = torch.cuda.utilization(i)
    tv = torch.cuda.get_device_properties(i).total_memory/temp1gb
    av = torch.cuda.mem_get_info(i)[0]/temp1gb
    puv = torch.cuda.memory_reserved(i)/temp1gb
 
    return cu,tr,ar,gu,tv,av,puv 
    # CPU util, Total RAM, Avail RAM, GPU Uitl, Total VRAM, Avail VRAM, Process Used VRAM


def show_cpu_gpu(description=""):
    if description != "":
        print("-----------------------> " + description)    
    cu,tr,ar,gu,tv,av,puv = get_cpu_gpu()
    print("CPU Utilization: {:.3f}, Total RAM: {:.3f} GB, Available RAM {:.3f} GB".format(cu,tr,ar))
    message = "; Run out of VRAM !!!!!" if av < 0.001 else ""
    print("GPU Utilization: {:.3f}, Total VRAM: {:.3f} GB, Available VRAM: {:.3f} GB, Process Used VRAM: {:.3f} {}".format(gu,tv,av,puv,message), flush=True)


if torch.cuda.is_available():
    device = "cuda:0"
    device_properties = torch.cuda.get_device_properties(0)
    compute_capability = float(f"{device_properties.major}.{device_properties.minor}")
    print(f"GPU Compute Capability: {compute_capability}", flush=True)
    show_cpu_gpu("The initial state before loading the model")  
else:
    device = "cpu"


model_id = "nvidia/parakeet-tdt-1.1b"


def load_model(warmup: bool = False):
    print(f"Loading model {model_id} on device {device}", flush=True)
    start = time.perf_counter()
    #asr_model = nemo_asr.models.ASRModel.from_pretrained(model_name=model_id)
    #temp = "data/model.nemo"
    #asr_model.save_to(temp)
    temp = "data/model.nemo"
    asr_model = nemo_asr.models.EncDecRNNTBPEModel.restore_from(temp)
    asr_model.change_attention_model(self_attention_model="rel_pos_local_attn", att_context_size=[128, 128])
    asr_model.change_subsampling_conv_chunking_factor(1)
    end = time.perf_counter()
    print(f"Loaded model in {end - start} seconds", flush=True)
    show_cpu_gpu("The state after the model is loaded") 

    if not warmup:
        return asr_model
    
    print("Warming up model...", flush=True)
    start = time.perf_counter()
    result = asr_model.transcribe(['voice_for_warm_up.mp3'],channel_selector='average',verbose=True)
    end = time.perf_counter()
    print(f"Warmed up model in {end - start} seconds", flush=True)
    print("Transcription from the warm-up: " + result[0][0])
    show_cpu_gpu("The state after the first inference is done") 
    return asr_model
