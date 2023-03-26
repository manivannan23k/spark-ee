import time
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import numpy as np
import scipy.ndimage
from PIL import Image
import io
from starlette.responses import StreamingResponse
from sindexing import generate_index, get_tile_intersection
from ingestion import partition_data
from retrieval import load_data, merge_tiles, clean_tmp_dir, time_indexes, time_indexes_ts, ts_to_tindex, tindex_to_ts, tilenum2deg, merge_tiles_tmp
# import load_render as lr

clean_tmp_dir()
print("Tmp dir cleaned")
app = FastAPI()
origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class GenerateSIndex(BaseModel):
    shapefile_path: str

@app.post("/generateSpatialIndex")
async def generate_spatial_index(index_request: GenerateSIndex):
    try:
        generate_index(
            index_request.shapefile_path
        )
        return {
            "error": False,
            "message": "Spatial Index Generated"
        }
    except Exception as e:
        print(e)
        return {
            "error": True,
            "message": "Error"
        }

@app.get("/getSpatialIndex/{level}")
def get_spatial_index(level: int, xmin: float, xmax: float, ymin: float, ymax: float):
    results = get_tile_intersection(level, [xmin, ymin, xmax, ymax]) #[n.object for n in index_dat[level].intersection([xmin, ymin, xmax, ymax], objects=True)]
    return {
        "error": False,
        "message": "Success",
        "data": results
    }

@app.get("/getDataForBbox/{level}")
def get_data_for_bbox(sensorName: str, level: int, tIndex: int, xmin: float, xmax: float, ymin: float, ymax: float):
    start_time = time.time()
    tindex = tIndex
    tiles = get_tile_intersection(level, [xmin, ymin, xmax, ymax]) # [n.object for n in index_dat[level].intersection([xmin, ymin, xmax, ymax], objects=True)]
    tiles = [[int(i) for i in tile.split("#")] for tile in tiles]
    merge_ds = []
    for tile in tiles: 
        ds = load_data(tile, tindex, sensorName)
        merge_ds.append(ds)
    
    out_path = merge_tiles(merge_ds, [xmin, ymax, xmax, ymin])
    print("--- %s seconds ---" % (time.time() - start_time))
    return {
        "error": False,
        "message": "Success",
        "data": out_path
    }


@app.get("/ingestData")
def ingest_data(filePath: str, sensorName: str, ts: int):
    result = partition_data(filePath, ts, sensorName)
    return {
        "error": False,
        "message": "Success",
        "data": result
    }


@app.get("/getTimeIndexes")
def get_time_indexes(sensorName: str, fromTs: int = None, toTs: int = None):
    if(fromTs is None and toTs is not None):
        result = [i for i in time_indexes_ts(sensorName) if (i<=toTs)]
    elif(fromTs is not None and toTs is None):
        result = [i for i in time_indexes_ts(sensorName) if (i>=fromTs)]
    elif(fromTs is not None and toTs is not None):
        result = [i for i in time_indexes_ts(sensorName) if (i>=fromTs and i<=toTs)]
    else:
        result = [i for i in time_indexes_ts(sensorName)]
    result = list(set(result))
    result.sort()
    return {
        "error": False,
        "message": "Success",
        "data": {
            "ts": result,
            "tIndexes": ts_to_tindex(result)
        }
    }


@app.get("/getPixelAt")
def get_pixel_at(sensorName: str, x: float, y: float, fromTs: int = None, toTs: int = None):
    start_time = time.time()
    if(fromTs is None and toTs is not None):
        result = [i for i in time_indexes_ts(sensorName) if (i<=toTs)]
    elif(fromTs is not None and toTs is None):
        result = [i for i in time_indexes_ts(sensorName) if (i>=fromTs)]
    elif(fromTs is not None and toTs is not None):
        result = [i for i in time_indexes_ts(sensorName) if (i>=fromTs and i<=toTs)]
    else:
        result = [i for i in time_indexes_ts(sensorName)]
    result = list(set(result))
    print(result)
    tindexes = ts_to_tindex(result)
    tiles = get_tile_intersection(12, [x, y, x, y]) #[n.object for n in index_dat[12].intersection([x,y,x,y], objects=True)]
    tiles = [[int(i) for i in tile.split("#")] for tile in tiles]
    result = []
    for tindex in tindexes:
        ds = load_data(tiles[0], tindex, sensorName)
        geotransform = ds.GetGeoTransform()
        x_offset = int((x - geotransform[0]) / geotransform[1])
        y_offset = int((y - geotransform[3]) / geotransform[5])
        data = [ds.GetRasterBand(i).ReadAsArray(x_offset, y_offset, 1, 1)[0][0] for i in range(1, ds.RasterCount+1)]
        data = np.array(data)
        data[np.isnan(data)] = 0
        result.append(data.tolist())
    print("--- %s seconds ---" % (time.time() - start_time))
    return {
        "error": False,
        "message": "Success",
        "data": result
    }

def image_to_byte_array(image: Image):
  # BytesIO is a fake file stored in memory
  imgByteArr = io.BytesIO()
  # image.save expects a file as a argument, passing a bytes io ins
  image.save(imgByteArr, format='PNG')
  
  # Turn the BytesIO object back into a bytes object
  imgByteArr = imgByteArr#.getvalue()
  imgByteArr.seek(0)
  return imgByteArr

@app.get("/tile/{sensorName}/{z}/{x}/{y}.png")
def get_tile(tIndex: int, z: int, x: int, y: int, sensorName: str, bands: str = None, vmin: float = 0, vmax: float = 0.75):
    start_time = time.time()
    # rgb_data = lr.run(x, y, z, tIndex, sensorName, bands, vmax)
    # if(rgb_data is None):
    #     return None
    # img = Image.fromarray(rgb_data, 'RGBA')
    # print("--- %s seconds ---" % (time.time() - start_time))
    # return StreamingResponse(image_to_byte_array(img), media_type="image/png")

    bbox = tilenum2deg(x, y, z)
    level = z-1
    if(level > 12):
        level = 12
    if(level < 4):
        level = 4
    tiles = get_tile_intersection(level, [bbox[0], bbox[3], bbox[2], bbox[1]]) #[n.object for n in index_dat[level].intersection([bbox[0], bbox[3], bbox[2], bbox[1]], objects=True)]
    if(tiles is None):
        print("Failed to read sIndex")
        return None
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
        img = Image.fromarray(rgb_data, 'RGBA')
    else:
        return None
    print("--- %s seconds ---" % (time.time() - start_time))
    return StreamingResponse(image_to_byte_array(img), media_type="image/png")