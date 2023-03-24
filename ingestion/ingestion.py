from osgeo import gdal, ogr, osr
import os
import osgeo
import json
from datetime import datetime
import math


tiff_driver = gdal.GetDriverByName('GTiff')
shp_driver = ogr.GetDriverByName("ESRI Shapefile")

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

def partition_data(input_tiff, input_ts, sensor_name):
    config = json.load(open("config.json"))
    start_ts = int(datetime.timestamp(datetime.strptime(config["start_time"], '%Y-%m-%d %H:%M:%S'))*1000)
    time_index = int((input_ts - start_ts)/1000)
    print(f"Saving tiles to {config['tile_dir']}, Sc:{input_tiff}, Ti:{time_index}")

    dataset = gdal.Open(input_tiff)
    if(dataset is None):
        return False
    
    extent_geom = get_extent_of_ds(dataset)
    proj = osr.SpatialReference(wkt=dataset.GetProjection())
    src_src_code = int(proj.GetAttrValue('AUTHORITY',1))
    extent_geom = reproject(extent_geom, src_src_code, 4326)


    dir_path = config["tile_dir"]
    tindex_file = os.path.join(config["tindex_dir"], f"{sensor_name}.json")
    tile_size = 256

    tindex_data = {"data": []}
    if os.path.exists(tindex_file):
        tindex_data = json.load(open(tindex_file))
    
    tindex_data["data"].append(time_index)

    with open(tindex_file, "w") as tifile:
        tifile.write(json.dumps(tindex_data, indent=4))

    for i in range(4, 13):
        lcount = 0
        level_id = i
        filename = f"IndiaWGS84GCS{str(i)}.shp"
        shp_file = os.path.join(config["grid_dir"], filename)
        shp_ds = shp_driver.Open(shp_file)
        shp_lyr = shp_ds.GetLayer()

        aoi_extent = shp_lyr.GetExtent()
        # print(aoi_extent)
        aoi_extent = reproject_extent(aoi_extent[0], aoi_extent[2], aoi_extent[1], aoi_extent[3], 4326, 3857)
        fea_count = shp_lyr.GetFeatureCount()
        f_index = 0
        for feature in shp_lyr:
            geometry = feature.GetGeometryRef()
            spatial_partition_index = tuple(map(int, feature.GetField('id').replace('(','').replace(')','').split(',')))
            level_id = spatial_partition_index[2]
            x_id = spatial_partition_index[0]
            y_id = spatial_partition_index[1]
            if(geometry.Intersects(extent_geom)):
                lcount += 1
                _e = geometry.GetEnvelope()
                tile_extent = geometry.GetEnvelope() #reproject(geometry, 4326, src_src_code).GetEnvelope()
                xmin = tile_extent[0]
                ymin = tile_extent[2]
                xmax = tile_extent[1]
                ymax = tile_extent[3]
                tile_file_dir = os.path.join(dir_path, f"{sensor_name}/{level_id}/{x_id}/{y_id}/")
                if not os.path.exists(tile_file_dir):
                    os.makedirs(tile_file_dir)

                out_path = os.path.join(tile_file_dir, f"{time_index}.tif")
                # print(out_path)
                # break

                tmp_ds = gdal.Translate(out_path, dataset, width=tile_size, height=tile_size, format='GTiff', projWin = [xmin, ymax, xmax, ymin], outputSRS="EPSG:4326", projWinSRS="EPSG:4326", noData=0, resampleAlg='lanczos')
                print(f"[Level {i}] {f_index}/{fea_count}" + f' Features, Total tiles: {lcount}', end='\r')
            f_index += 1
        # print(f"Level {i}: {lcount}")
        shp_ds = None