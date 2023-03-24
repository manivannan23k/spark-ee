from osgeo import gdal, ogr, osr
import os
import osgeo
import json
from datetime import datetime
import math
import glob

import random
import string

def generate_string():
    name = ''.join(random.choices(string.ascii_uppercase, k=16))
    return name

config = json.load(open("config.json"))

def load_data(tile, tindex, sensor_name):
    dir_path = config["tile_dir"]
    print(os.path.join(dir_path, sensor_name, f"{tile[0]}/{tile[1]}/{tile[2]}.tif"))
    ds = gdal.Open(os.path.join(dir_path, sensor_name, f"{tile[0]}/{tile[1]}/{tile[2]}/{tindex}.tif"))
    return ds

def merge_tiles(merge_ds, projWin):
    vrt_options = gdal.BuildVRTOptions(resampleAlg='cubic')
    merge_vrt = gdal.BuildVRT('', merge_ds, options=vrt_options)
    merge_ds = gdal.Warp("", merge_vrt, format="VRT")
    out_path = os.path.join(config["temp_dir"], f'{generate_string()}.tif')
    merge_ds = gdal.Translate(out_path, merge_ds, format='GTiff', projWin = projWin, projWinSRS='EPSG:4326')
    return out_path

def clean_tmp_dir():
    files = glob.glob(config["temp_dir"]+"/*tif")
    for f in files:
        os.remove(f)
    return True