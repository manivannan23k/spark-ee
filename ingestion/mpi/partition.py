from osgeo import gdal, ogr, osr
import os
import osgeo
import json
from datetime import datetime
import time
import zCurve as z
import psycopg2
import random
import string
from psycopg2.extras import RealDictCursor
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
name = MPI.Get_processor_name()
nprocs = comm.Get_size()

tiff_driver = gdal.GetDriverByName("GTiff")
shp_driver = ogr.GetDriverByName("ESRI Shapefile")
config = json.load(open("/mnt/data/common/config.json"))


def get_db_conn():
    try:
        connection = psycopg2.connect(
            user=config["db_user"],
            password=config["db_password"],
            host="10.128.0.2",  # config['db_host'],
            port="5432",
            database=config["db_database"],
        )
    except Exception as error:
        print(error)
    return connection


def add_db_ingestion(data):
    qry = f"""
        insert into ingest_master (
            dataset_id,
            time_index,
            geom,
            date_time
        )
        values (
            {data['dataset_id']},
            {data['time_index']},
            {data['geom']},
            {data['date_time']}
        ) RETURNING ingest_id;
    """
    conn = get_db_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(qry)
    conn.commit()
    row = cursor.fetchone()["ingest_id"]
    cursor.close()
    conn.close()
    # row = cursor.fetchone()
    return row


def add_db_tile(tile_data):
    qry = f"""
        insert into tiles_local(
            ingest_id,
            dataset_id,
            zoom_level,
            time_index,
            spatial_index_x,
            spatial_index_y,
            z_index,
            file_path
        )
        values (
            {tile_data['ingest_id']},
            {tile_data['dataset_id']},
            {tile_data['zoom_level']},
            {tile_data['time_index']},
            {tile_data['spatial_index_x']},
            {tile_data['spatial_index_y']},
            {tile_data['z_index']},
            {tile_data['file_path']}
        ) returning tile_id
    """
    conn = get_db_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(qry)
    conn.commit()
    row = cursor.fetchone()["tile_id"]
    cursor.close()
    conn.close()
    # row = cursor.fetchone()
    return row


def get_db_dataset_def_by_name(dataset_name):
    conn = get_db_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        f"""
            select * from {config['tbl_dataset_def']}
                where ds_name = '{dataset_name}'
                limit 1
        """
    )
    row = cursor.fetchone()
    return row


def get_extent_of_ds(dataset):
    geotransform = dataset.GetGeoTransform()
    cols = dataset.RasterXSize
    rows = dataset.RasterYSize

    xmin = geotransform[0]
    ymax = geotransform[3]
    xmax = xmin + geotransform[1] * cols
    ymin = ymax + geotransform[5] * rows

    # print('Extent:', xmin, ymin, xmax, ymax)
    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint(xmin, ymin)
    ring.AddPoint(xmin, ymax)
    ring.AddPoint(xmax, ymax)
    ring.AddPoint(xmax, ymin)
    ring.AddPoint(xmin, ymin)

    geom = ogr.Geometry(ogr.wkbPolygon)
    geom.AddGeometry(ring)
    return geom


def reproject(geometry, in_srs, out_srs):
    src_srs = osr.SpatialReference()
    src_srs.ImportFromEPSG(in_srs)
    target_srs = osr.SpatialReference()
    if int(osgeo.__version__[0]) >= 3:
        target_srs.SetAxisMappingStrategy(osgeo.osr.OAMS_TRADITIONAL_GIS_ORDER)
        src_srs.SetAxisMappingStrategy(osgeo.osr.OAMS_TRADITIONAL_GIS_ORDER)
    target_srs.ImportFromEPSG(out_srs)
    transform = osr.CoordinateTransformation(src_srs, target_srs)
    geometry.Transform(transform)
    return geometry


def reproject_extent(xmin, ymin, xmax, ymax, in_srs, out_srs):
    src_srs = osr.SpatialReference()
    src_srs.ImportFromEPSG(in_srs)
    target_srs = osr.SpatialReference()
    if int(osgeo.__version__[0]) >= 3:
        target_srs.SetAxisMappingStrategy(osgeo.osr.OAMS_TRADITIONAL_GIS_ORDER)
        src_srs.SetAxisMappingStrategy(osgeo.osr.OAMS_TRADITIONAL_GIS_ORDER)
    target_srs.ImportFromEPSG(out_srs)
    transform = osr.CoordinateTransformation(src_srs, target_srs)

    input_geom = ogr.Geometry(ogr.wkbPolygon)
    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint(xmin, ymin)
    ring.AddPoint(xmax, ymin)
    ring.AddPoint(xmax, ymax)
    ring.AddPoint(xmin, ymax)
    ring.AddPoint(xmin, ymin)
    input_geom.AddGeometry(ring)
    input_geom.AssignSpatialReference(src_srs)

    # Create a geometry transformer
    transformer = osr.CoordinateTransformation(src_srs, target_srs)

    # Transform the input geometry to the output SRS
    output_geom = input_geom.Clone()
    output_geom.Transform(transformer)

    # Get the transformed extent
    xmin, xmax, ymin, ymax = output_geom.GetEnvelope()
    return (xmin, ymin, xmax, ymax)


def partition_data(pdatas, in_file):
    i = 0
    start_time = time.time()
    dataset = gdal.Open(in_file)
    for pdata in pdatas:
        print(f"[Tile {i}] {len(pdatas)}", end="\r")
        # print(f"Opened", end='\r')
        tile_id = add_db_tile(pdata["tile_data"])
        # print(f"Added to db", end='\r')
        tmp_ds = gdal.Translate(
            pdata["out_path"],
            dataset,
            width=pdata["tile_size"],
            height=pdata["tile_size"],
            format="GTiff",
            projWin=pdata["bbox"],
            outputSRS="EPSG:4326",
            projWinSRS="EPSG:4326",
            noData=0,
            resampleAlg="cubic",
            outputType=gdal.GDT_Int16,
        )
        tmp_ds = None
        i += 1
    dataset = None
    print(f"DONE", name, rank)
    # print("DONE", name, rank, "--- %s seconds ---" % (time.time() - start_time))


def get_partition_data(input_tiff, input_time_str, sensor_name):
    work_data = []
    start_ts = int(
        datetime.timestamp(datetime.strptime(config["start_time"], "%Y-%m-%d %H:%M:%S"))
        * 1000
    )
    input_ts = int(
        datetime.timestamp(datetime.strptime(input_time_str, "%Y-%m-%d %H:%M:%S"))
        * 1000
    )
    time_index = int((input_ts - start_ts) / 1000)
    # print(f"Saving tiles to {config['tile_dir']}, Sc:{input_tiff}, Ti:{time_index}")

    temp_file_name = input_tiff[:-4] + "_4326.tif"
    dataset = gdal.Open(temp_file_name)
    if dataset is None:
        return False

    extent_geom = get_extent_of_ds(dataset)
    proj = osr.SpatialReference(wkt=dataset.GetProjection())
    src_src_code = int(proj.GetAttrValue("AUTHORITY", 1))
    extent_geom = reproject(extent_geom, src_src_code, 4326)
    extent_geom.FlattenTo2D()

    # random_txt = ''.join(random.choices(string.ascii_uppercase, k=16))
    # temp_file_name = input_tiff[:-4] + "_4326.tif" #f'/mnt/data/temp_data/temp_{random_txt}.tif'
    # if os.path.isfile(temp_file_name) or os.path.islink(temp_file_name):
    #     os.unlink(temp_file_name)

    # if(src_src_code!=4326):
    #     # print(f"Reprojecting: {src_src_code} -> {4326}")
    #     dataset = gdal.Warp(temp_file_name, dataset, format='GTiff', dstSRS="EPSG:4326", outputType=gdal.GDT_Int16)
    #     # print(f"Reprojection completed")

    ds_def = get_db_dataset_def_by_name(sensor_name)
    ingest_data = {
        "dataset_id": ds_def["dataset_id"],
        "geom": f"st_setsrid(st_geomfromgeojson('{extent_geom.ExportToJson()}'), 4326)",
        "time_index": time_index,
        "date_time": f"to_timestamp('{input_ts/1000}')::timestamp without time zone",
    }
    ingest_id = add_db_ingestion(ingest_data)

    dir_path = config["tile_dir"]
    tile_size = 256

    for i in range(4, 13):
        lcount = 0
        level_id = i
        filename = f"IndiaWGS84GCS{str(i)}.shp"
        shp_file = os.path.join(config["grid_dir"], filename)
        shp_ds = shp_driver.Open(shp_file)
        shp_lyr = shp_ds.GetLayer()

        aoi_extent = shp_lyr.GetExtent()
        # print(aoi_extent)
        aoi_extent = reproject_extent(
            aoi_extent[0], aoi_extent[2], aoi_extent[1], aoi_extent[3], 4326, 3857
        )
        fea_count = shp_lyr.GetFeatureCount()
        f_index = 0
        for feature in shp_lyr:
            geometry = feature.GetGeometryRef()
            spatial_partition_index = tuple(
                map(
                    int,
                    feature.GetField("id").replace("(", "").replace(")", "").split(","),
                )
            )
            level_id = spatial_partition_index[2]
            x_id = spatial_partition_index[0]
            y_id = spatial_partition_index[1]
            if geometry.Intersects(extent_geom):
                lcount += 1
                _e = geometry.GetEnvelope()
                tile_extent = (
                    geometry.GetEnvelope()
                )  # reproject(geometry, 4326, src_src_code).GetEnvelope()
                xmin = tile_extent[0]
                ymin = tile_extent[2]
                xmax = tile_extent[1]
                ymax = tile_extent[3]
                rel_file_path = (
                    f"{sensor_name}/{level_id}/{x_id}/{y_id}/" + f"{time_index}.tif"
                )
                tile_file_dir = os.path.join(
                    dir_path, f"{sensor_name}/{level_id}/{x_id}/{y_id}/"
                )
                if not os.path.exists(tile_file_dir):
                    try:
                        os.makedirs(tile_file_dir)
                    except Exception as e:
                        print("Dir excep")

                out_path = os.path.join(dir_path, rel_file_path)
                # print(out_path)
                work_data.append(
                    {
                        "out_path": out_path,
                        "in_path": temp_file_name,
                        "tile_size": tile_size,
                        "bbox": [xmin, ymax, xmax, ymin],
                        "tile_data": {
                            "ingest_id": ingest_id,
                            "dataset_id": ds_def["dataset_id"],
                            "zoom_level": i,
                            "time_index": time_index,
                            "spatial_index_x": x_id,
                            "spatial_index_y": y_id,
                            "z_index": f"'{z.interlace(time_index, y_id, x_id)}'",
                            "file_path": f"'{rel_file_path}'",
                        },
                    }
                )
                # print(f"[Level {i}] {f_index}/{fea_count}" + f' Features, Total tiles: {lcount}', end='\r')
            f_index += 1
        shp_ds = None
    return work_data, temp_file_name


def split(a, n):
    k, m = divmod(len(a), n)
    return list(a[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n))


send_data = []

if rank == 0:
    print("Parsing params...", name, rank)
    ingestion_data = json.load(open("/mnt/data/common/ingestion/injestiondata.json"))
    send_data = split(ingestion_data[0:10], nprocs)
else:
    print("Other node", name, rank)
    send_data = None

in_datas = comm.scatter(send_data)

print("Do work with the data", len(in_datas), name, rank)

image_idx = 0
for idata in in_datas:
    start_time = time.time()
    work_data, temp_file_name = get_partition_data(
        idata["srcPath"], idata["dataTime"], "Landsat_OLI"
    )
    image_idx += 1
    print(
        f"Image {image_idx}/{len(in_datas)} processed",
        name,
        rank,
        "--- %s seconds ---" % (time.time() - start_time),
    )
    partition_data(work_data, temp_file_name)
    # if os.path.isfile(temp_file_name) or os.path.islink(temp_file_name):
    #     os.unlink(temp_file_name)
    #     print("deleted: ", temp_file_name)
    print(
        f"Image {image_idx}/{len(in_datas)} ingested",
        name,
        rank,
        "--- %s seconds ---" % (time.time() - start_time),
    )

print("DONE for", name, rank)


# mpiexec -machinefile /mnt/data/machinefile -n 34 -x LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH /mnt/data/env/project/bin/python /mnt/data/common/ingestion/partition.py > /mnt/data/common/partout.log
# /mnt/data/common/data-download/ingestion_inputs_instance-1_1.json

# mpiexec -machinefile /mnt/data/machinefile -n 34 -x LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH /mnt/data/env/project/bin/python /mnt/data/common/ingestion/partition.py > /mnt/data/common/out.log
