import time
from fastapi import FastAPI
from pydantic import BaseModel
from sindexing import generate_index, load_indexes
from ingestion import partition_data
from retrieval import load_data, merge_tiles, clean_tmp_dir

level_ids = [4, 5, 6, 7, 8, 9, 10, 11, 12]
index_dat = load_indexes(level_ids)
print(f"Spatial Index loaded for {level_ids}")
clean_tmp_dir()
print("Tmp dir cleaned")
app = FastAPI()

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
    results = [n.object for n in index_dat[level].intersection([xmin, ymin, xmax, ymax], objects=True)]
    return {
        "error": False,
        "message": "Success",
        "data": results
    }

@app.get("/getDataForBbox/{level}")
def get_data__for_bbox(sensorName: str, level: int, xmin: float, xmax: float, ymin: float, ymax: float):
    start_time = time.time()
    tindex = 889417419
    tiles = [n.object for n in index_dat[level].intersection([xmin, ymin, xmax, ymax], objects=True)]
    tiles = [[int(i) for i in tile.split("#")] for tile in tiles]
    merge_ds = []
    for tile in tiles: 
        ds = load_data(tiles[0], tindex, sensorName)
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
