from osgeo import gdal, ogr, osr
from landsatxplore.api import API
import json
import asyncio
from landsatxplore.earthexplorer import EarthExplorer
import tarfile
import os
import requests

#N1tCL!U_CSqV6!Dtc5!2n9REA6wVlvYOcQtYEV9pL!UAiR095gTXAElNFm5APcBI

aoi_bbox = None
ds_name = 'landsat_ot_c2_l2'
download_path = './data/landsat_data/'

api = API("manivannan23111996@gmail.com", "Manivannan23,")
ee = EarthExplorer("manivannan23111996@gmail.com", "Manivannan23,")
tiff_driver = gdal.GetDriverByName('GTiff')

def get_aoi(aoi_shp_path):
    aoi_ds = ogr.Open(aoi_shp_path)
    for feature in aoi_ds.GetLayer():
        aoi_bbox = feature.GetGeometryRef().GetEnvelope()
        aoi_bbox = (aoi_bbox[0], aoi_bbox[2], aoi_bbox[1], aoi_bbox[3])
        break
    return aoi_bbox

async def download_data(loop, scene_id):
    return await loop.run_in_executor(None, ee.download, scene_id, download_path, ds_name)

async def download_scenes(loop, scenes):
    download_tasks = []
    for i in range(0, len(scenes)):
        download_task = asyncio.ensure_future(
            download_data(loop, scenes[i]['entity_id'])
        )
        download_tasks.append(download_task)
    responses = await asyncio.gather(*download_tasks)
    return responses

aoi_bbox = get_aoi("data/aoi/uk_boundary.shp")

scenes = api.search(
    dataset=ds_name,
    bbox=aoi_bbox, 
    start_date='2021-01-01',
    end_date='2021-01-10'
)

print(f"{len(scenes)} scenes found.")

loop = asyncio.get_event_loop()
# loop.run_until_complete(download_scenes(loop, scenes))

ingestion_inputs = []
for scene in scenes:
    file_path = download_path + scene['display_id'] + '.tar'
    out_file_path = download_path + scene['display_id'] + '.tif'
    dir_path = download_path + scene['display_id']
    scene_date_time = scene['start_time']
    scene_tar = tarfile.open(file_path)
    scene_tar.extractall(dir_path)
    scene_tar.close()

    """
    Merge to single tif - B01 - B07
    """
    no_of_bands = 7

    out_ds = None
    gt = None
    for band in range(no_of_bands):
        print(f"---Writing Band {band+1}---")
        in_ds = gdal.Open(f"{dir_path}/{scene['display_id']}_SR_B{band+1}.TIF")
        data_arr = in_ds.GetRasterBand(1).ReadAsArray()
        
        if out_ds is None:
            width = data_arr.shape[1]
            height = data_arr.shape[0]
            out_ds = tiff_driver.Create(out_file_path, width, height, no_of_bands, gdal.GDT_Float32)
            gt = in_ds.GetGeoTransform()
            out_ds.SetGeoTransform(gt)
            out_ds.SetProjection(in_ds.GetProjection())
        
        raster_band = out_ds.GetRasterBand(band + 1)
        raster_band.WriteArray(data_arr)
        raster_band.FlushCache()
    
    out_ds = None

    scene_date_time = scene_date_time.strftime("%Y-%m-%dT%H:%M:%SZ") #2021-05-02T00:00:00Z
    ingestion_inputs.append({
        "dataTime": scene_date_time,
        "srcPath": os.path.abspath(out_file_path),
        "sensor": "Landsat8"
    })
    # print({
    #     "dataTime": scene_date_time,
    #     "srcPath": os.path.abspath(out_file_path),
    #     "sensor": "Landsat8"
    # })
    # break


# for ingestion_input in ingestion_inputs:
#     url = 'http://localhost:8087/ingest'
#     resp = requests.post(url, json = ingestion_input)
#     print(resp.text)
#     break

api.logout()
ee.logout()