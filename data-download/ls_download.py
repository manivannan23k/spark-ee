import asyncio
from landsatxplore.earthexplorer import EarthExplorer
import csv
from mpi4py import MPI
import shutil
import tarfile
import os
import json
from osgeo import gdal

# from landsatxplore.api import API

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
name = MPI.Get_processor_name()
nprocs = comm.Get_size()

base_path = f"/mnt/data/{name}"
download_path = f"{base_path}/raw_data/"

# print(name, rank)

send_data = []
ds_name = "landsat_ot_c2_l2"
tiff_driver = gdal.GetDriverByName("GTiff")


def split(a, n):
    k, m = divmod(len(a), n)
    return list(a[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n))


if rank == 0:
    print("Parsing params...", name, rank)
    scenes = []
    with open("/mnt/data/common/data-download/datameta.csv", mode="r") as file:
        csv_file = csv.DictReader(file)
        for lines in csv_file:
            scenes.append(
                {
                    "entity_id": lines["Landsat Scene Identifier"],
                    "start_time": lines["Start Time"],
                    "display_id": lines["Landsat Product Identifier L2"],
                }
            )
            if len(scenes) >= 2:
                break
    send_data = split(scenes, nprocs)
else:
    print("Other node", name, rank)
    send_data = None

scenes = comm.scatter(send_data)

print("Do work with the data", len(scenes), download_path)

ee = EarthExplorer("manivannan23111996@gmail.com", "Manivannan23,")


async def download_data(loop, scene_id):
    return await loop.run_in_executor(
        None, ee.download, scene_id, download_path, ds_name
    )


async def download_scenes(loop, scenes):
    download_tasks = []
    for i in range(0, len(scenes)):
        download_task = asyncio.ensure_future(
            download_data(loop, scenes[i]["entity_id"])
        )
        download_tasks.append(download_task)
    responses = await asyncio.gather(*download_tasks)
    return responses


print("Download started", name, rank)

loop = asyncio.get_event_loop()
loop.run_until_complete(download_scenes(loop, scenes))

ee.logout()

print("Download completed", name, rank)

ingestion_inputs = []
for scene in scenes:
    file_path = download_path + scene["display_id"] + ".tar"
    out_file_path = download_path + scene["display_id"] + ".tif"
    dir_path = download_path + scene["display_id"]
    scene_date_time = scene["start_time"]
    scene_tar = tarfile.open(file_path)
    scene_tar.extractall(dir_path)
    scene_tar.close()

    # delete the tar file
    if os.path.isfile(file_path) or os.path.islink(file_path):
        os.unlink(file_path)

    """
    Merge to single tif - B01 - B07
    """
    no_of_bands = 7

    out_ds = None
    gt = None
    for band in range(no_of_bands):
        # print(f"---Writing Band {band+1}---")
        in_ds = gdal.Open(f"{dir_path}/{scene['display_id']}_SR_B{band+1}.TIF")
        data_arr = in_ds.GetRasterBand(1).ReadAsArray()

        if out_ds is None:
            width = data_arr.shape[1]
            height = data_arr.shape[0]
            out_ds = tiff_driver.Create(
                out_file_path, width, height, no_of_bands, gdal.GDT_Float32
            )
            gt = in_ds.GetGeoTransform()
            out_ds.SetGeoTransform(gt)
            out_ds.SetProjection(in_ds.GetProjection())

        raster_band = out_ds.GetRasterBand(band + 1)
        raster_band.WriteArray(data_arr)
        raster_band.FlushCache()

    out_ds = None

    # scene_date_time = scene_date_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    ingestion_inputs.append(
        {
            "dataTime": scene_date_time,
            "srcPath": os.path.abspath(out_file_path),
            "sensor": "Landsat8",
        }
    )
    shutil.rmtree(dir_path, ignore_errors=True)
    print("Merge complete", len(ingestion_inputs), name, rank)

with open(
    f"/mnt/data/common/data-download/ingestion_inputs_{name}_{rank}.json", "w"
) as fp:
    json.dump(ingestion_inputs, fp)

# mpiexec -machinefile /mnt/data/machinefile -n 2 /mnt/data/env/project/bin/python /mnt/data/common/data-download/test.py
# mpiexec -machinefile /mnt/data/machinefile -n 2 -x LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH /mnt/data/env/project/bin/python /mnt/data/common/data-download/test.py
