import rtree
from osgeo import ogr, osr
import osgeo
import json
import os


index_dat2 = None

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

def generate_index(shapefile_path):
    ds = ogr.Open(shapefile_path)
    idx = None
    lyr = ds.GetLayer()
    f_count = lyr.GetFeatureCount()
    f_i = 1
    attr_data = {}
    for feature in lyr:
        g = feature.GetGeometryRef()
        spatial_partition_index = tuple(map(int, feature.GetField('id').replace('(','').replace(')','').split(',')))
        level_id = spatial_partition_index[2]
        x_id = spatial_partition_index[0]
        y_id = spatial_partition_index[1]
        uid = f"{level_id}#{x_id}#{y_id}"
        g = g.GetEnvelope()
        g = (g[0], g[2], g[1], g[3])
        print(f"{f_i}/{f_count}, {g}", end="\r")

        if(idx is None):
            p = rtree.index.Property()
            # p.dat_extension = 'data'
            # p.idx_extension = 'index'
            idx = rtree.index.Index(os.path.join(config['sindex_dir'], f"spatial_index_{level_id}"), properties=p)
        idx.insert(f_i, g, obj=uid)
        f_i += 1
    idx.close()
    return True

def load_indexes(level_ids):
    index_data = {}
    for level_id in level_ids:
        f_path = os.path.join(config['sindex_dir'], f"spatial_index_{level_id}")
        if not os.path.exists(f_path):
            index_data[level_id] = rtree.index.Index(f_path)
    return index_data

def load_indexes2(level_ids):
    index_dat2 = {}
    for level_id in level_ids:
        f_path = os.path.join(config['sindex_dir_2'], f"spatial_index_{level_id}")
        print(f_path)
        if os.path.exists(f_path):
            index_dat2[level_id] = rtree.index.Index(f_path)
    return index_dat2

def load_indexes3(level_ids):
    index_data = {}
    for level_id in level_ids:
        f_path = os.path.join(config['sindex_dir_2'], f"spatial_index_{level_id}")
        if not os.path.exists(f_path):
            index_data[level_id] = rtree.index.Index(f_path)
    return index_data

def get_tile_intersection(level, bbox):
    try:
        return [n.object for n in index_dat[level].intersection(bbox, objects=True)]
    except:
        print("Using Index 2")
        if(index_dat2 is None):
            index_dat2 = load_indexes2(level_ids)
        tiles = [n.object for n in index_dat2[level].intersection(bbox, objects=True)]
        return tiles    
        # return [n.object for n in index_dat2[level].intersection(bbox, objects=True)]



config = json.load(open("config.json"))
level_ids = [4, 5, 6, 7, 8, 9, 10, 11, 12]
index_dat = load_indexes(level_ids)
# index_dat2 = load_indexes2(level_ids)
# index_dat3 = load_indexes3(level_ids)
print(f"Spatial Index loaded for {level_ids}")