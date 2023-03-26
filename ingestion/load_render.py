import numpy as np
import scipy.ndimage
from PIL import Image
import io
import json
import os
import rtree
from osgeo import gdal
import math
import multiprocessing


config = json.load(open("config.json"))

def load_indexes(level_ids):
    index_data = {}
    for level_id in level_ids:
        f_path = os.path.join(config['sindex_dir'], f"spatial_index_{level_id}")
        if not os.path.exists(f_path):
            index_data[level_id] = rtree.index.Index(f_path)
    return index_data

def tilenum2deg(xtile, ytile, zoom):
    n = 2.0 ** zoom
    lon_deg1 = xtile / n * 360.0 - 180.0
    lat_rad1 = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
    lat_deg1 = math.degrees(lat_rad1)
    
    lon_deg2 = (xtile+1) / n * 360.0 - 180.0
    lat_rad2 = math.atan(math.sinh(math.pi * (1 - 2 * (ytile+1) / n)))
    lat_deg2 = math.degrees(lat_rad2)

    return [lon_deg1, lat_deg1, lon_deg2, lat_deg2]


def load_data(tile, tindex, sensor_name):
    dir_path = config["tile_dir"]
    # print(os.path.join(dir_path, sensor_name, f"{tile[0]}/{tile[1]}/{tile[2]}.tif"))
    file_path = os.path.join(dir_path, sensor_name, f"{tile[0]}/{tile[1]}/{tile[2]}/{tindex}.tif")
    if(not os.path.exists(file_path)):
        return None
    ds = gdal.Open(file_path)
    return ds

def merge_tiles_tmp(merge_ds, projWin):
    vrt_options = gdal.BuildVRTOptions(resampleAlg='cubic')
    merge_vrt = gdal.BuildVRT('', merge_ds, options=vrt_options)
    merge_ds = gdal.Warp("", merge_vrt, format="VRT")
    merge_ds = gdal.Translate("", merge_ds, format='VRT', projWin = projWin, projWinSRS='EPSG:4326')
    return merge_ds


def load_render_data(x, y, z, tIndex, sensorName, bands, vmax):
    
    config = json.load(open("config.json"))
    level_ids = [4, 5, 6, 7, 8, 9, 10, 11, 12]
    index_dat = load_indexes(level_ids)
    bbox = tilenum2deg(x, y, z)
    level = z-1
    if(level > 12):
        level = 12
    if(level < 4):
        level = 4
    tiles = [n.object for n in index_dat[level].intersection([bbox[0], bbox[3], bbox[2], bbox[1]], objects=True)] # get_tile_intersection(level, [bbox[0], bbox[3], bbox[2], bbox[1]]) #[n.object for n in index_dat[level].intersection([bbox[0], bbox[3], bbox[2], bbox[1]], objects=True)]
    tiles = [[int(i) for i in tile.split("#")] for tile in tiles]
    merge_ds = []
    for tile in tiles: 
        ds = load_data(tile, tIndex, sensorName)
        if(ds is not None):
            merge_ds.append(ds)
        
    if len(merge_ds) > 0:
        out_ds = merge_tiles_tmp(merge_ds, bbox)
        
        if(bands is None):
            rgb_bands = [2,3,4]
        else:
            rgb_bands = [int(b) for b in bands.split(',')]
        
        rgb_data = []
        alpha_data = []
        for bid in rgb_bands:
            data = out_ds.GetRasterBand(bid).ReadAsArray()
            # data[np.isnan(data)] = 0
            zoom_factors = (256/out_ds.RasterYSize, 256/out_ds.RasterXSize)
            data = scipy.ndimage.zoom(data, zoom=zoom_factors, order=0)
            data = np.nan_to_num(data)
            data = data.astype(np.float64) / vmax #np.nanmax(data)
            data = 255 * data
            data[data<0] = 0
            # print(np.nanmax(data), data[211,164])
            data = data.astype(np.uint8)
            rgb_data.append(data)
        
        alpha_data = np.sum(np.array(rgb_data), axis=0)
        alpha_data[alpha_data>0] = 255
        rgb_data.append(alpha_data)

        rgb_data = np.array(rgb_data, dtype=np.uint8)

        rgb_data = np.array(rgb_data)
        
        rgb_data = np.ascontiguousarray(rgb_data.transpose(1,2,0))
        return rgb_data
    else:
        return None

def run(x, y, z, tIndex, sensorName, bands, vmax):
    num_cpus = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=num_cpus)
    result = pool.apply_async(load_render_data, (x, y, z, tIndex, sensorName, bands, vmax))
    output = result.get()
    return output
    # load_render_data(x, y, z, tIndex, sensorName, bands, vmax)