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
    # print(os.path.join(dir_path, sensor_name, f"{tile[0]}/{tile[1]}/{tile[2]}.tif"))
    file_path = os.path.join(dir_path, sensor_name, f"{tile[0]}/{tile[1]}/{tile[2]}/{tindex}.tif")
    if(not os.path.exists(file_path)):
        return None
    ds = gdal.Open(file_path)
    return ds

def merge_tiles(merge_ds, projWin):
    vrt_options = gdal.BuildVRTOptions(resampleAlg='cubic')
    merge_vrt = gdal.BuildVRT('', merge_ds, options=vrt_options)
    merge_ds = gdal.Warp("", merge_vrt, format="VRT")
    out_path = os.path.join(config["temp_dir"], f'{generate_string()}.tif')
    merge_ds = gdal.Translate(out_path, merge_ds, format='GTiff', projWin = projWin, projWinSRS='EPSG:4326')
    return out_path

def merge_tiles_tmp(merge_ds, projWin):
    vrt_options = gdal.BuildVRTOptions(resampleAlg='cubic')
    merge_vrt = gdal.BuildVRT('', merge_ds, options=vrt_options)
    merge_ds = gdal.Warp("", merge_vrt, format="VRT")
    merge_ds = gdal.Translate("", merge_ds, format='VRT', projWin = projWin, projWinSRS='EPSG:4326')
    return merge_ds

def clean_tmp_dir():
    files = glob.glob(config["temp_dir"]+"/*tif")
    for f in files:
        os.remove(f)
    return True

def time_indexes(sensor_name):
    start_ts = int(datetime.timestamp(datetime.strptime(config["start_time"], '%Y-%m-%d %H:%M:%S'))*1000)
    results = json.load(open(os.path.join(config["tindex_dir"], f"{sensor_name}.json")))
    results = results["data"]
    return [r for r in results]

def time_indexes_ts(sensor_name):
    start_ts = int(datetime.timestamp(datetime.strptime(config["start_time"], '%Y-%m-%d %H:%M:%S'))*1000)
    results = json.load(open(os.path.join(config["tindex_dir"], f"{sensor_name}.json")))
    results = results["data"]
    results = list(set([start_ts + r*1000 for r in results]))
    results.sort()
    return results

def ts_to_tindex(ts):
    start_ts = int(datetime.timestamp(datetime.strptime(config["start_time"], '%Y-%m-%d %H:%M:%S'))*1000)
    return [int((t-start_ts)/1000) for t in ts]


def tindex_to_ts(ts):
    start_ts = int(datetime.timestamp(datetime.strptime(config["start_time"], '%Y-%m-%d %H:%M:%S'))*1000)
    return [start_ts + r*1000 for r in ts]

def tilenum2deg(xtile, ytile, zoom):
    n = 2.0 ** zoom
    lon_deg1 = xtile / n * 360.0 - 180.0
    lat_rad1 = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
    lat_deg1 = math.degrees(lat_rad1)
    
    lon_deg2 = (xtile+1) / n * 360.0 - 180.0
    lat_rad2 = math.atan(math.sinh(math.pi * (1 - 2 * (ytile+1) / n)))
    lat_deg2 = math.degrees(lat_rad2)

    return [lon_deg1, lat_deg1, lon_deg2, lat_deg2]

def get_xyz_tile_res(bbox):
    return (bbox[2]-bbox[0])/256
