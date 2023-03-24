import os
import json
from datetime import datetime
from ingestion import partition_data

RAW_DATA_PATH = '/projects/data/shift_culti/raw_data'
IN_DIR = '/projects/data/shift_culti/reprojected'

scenes = []
for o in os.listdir(RAW_DATA_PATH):
    scene_path = os.path.join(RAW_DATA_PATH,o)
    name = o[:-3]
    if os.path.isdir(scene_path):
        meta_path = scene_path + "/" + name + "_MTL.json"
        with open(meta_path) as meta_file:
            meta_data = json.load(meta_file)
            date_time = meta_data['LANDSAT_METADATA_FILE']['IMAGE_ATTRIBUTES']['DATE_ACQUIRED'] + " " + meta_data['LANDSAT_METADATA_FILE']['IMAGE_ATTRIBUTES']['SCENE_CENTER_TIME'][:8]
        ts = int(datetime.timestamp(datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S'))*1000)
        scenes.append({
            "filePath": os.path.join(IN_DIR, f"{name}.TIF"),
            "ts": ts,
            "sensorName": "Landsat8"
        })

sc_index = 0

for scene in scenes:
    print(scene["filePath"], scene["ts"])
    partition_data(scene["filePath"], scene["ts"], scene["sensorName"])
    sc_index += 1
    if(sc_index==10):
        break