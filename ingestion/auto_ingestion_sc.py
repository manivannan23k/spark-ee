import os
import json
from datetime import datetime
from ingestion import partition_data

RAW_DATA_PATH = '/projects/data/shift_culti/raw_data'
IN_DIR = 'G:/Bhawna/SC/Reprojected'

scenes = []
for o in os.listdir(IN_DIR):
    scene_path = os.path.join(IN_DIR, o)
    spls = o.replace(".tif", "").split("_")
    year = int(spls[3])
    month = int(spls[4])
    ts = int(datetime.timestamp(datetime(year=year, month=month, day=1))*1000)
    scenes.append({
        "filePath": scene_path,
        "ts": ts,
        "sensorName": "Landsat8_SC"
    })

sc_index = 0

for scene in scenes:
    print(scene["filePath"], scene["ts"])
    partition_data(scene["filePath"], scene["ts"], scene["sensorName"])
    sc_index += 1
#     if(sc_index==10):
#         break