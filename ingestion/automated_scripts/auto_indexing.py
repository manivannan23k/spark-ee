from sindexing import generate_index
import json
import os
import config as app_config

config = app_config.config

shapefile_paths = [

]
for f in os.listdir(config['grid_dir']):
    if(f.endswith(".shp")):
        shapefile_paths.append(os.path.join(config['grid_dir'], f))
for shapefile_path in shapefile_paths:
    print(shapefile_path)
    generate_index(
        shapefile_path
    )