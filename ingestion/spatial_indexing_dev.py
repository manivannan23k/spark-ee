import rtree
from osgeo import ogr, osr
import osgeo
import json
import os

def get_index(shapefile_path):
    ds = ogr.Open(shapefile_path)
    idx = None
    lyr = ds.GetLayer()
    f_count = lyr.GetFeatureCount()
    f_i = 1
    # index_data = []
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
            idx = rtree.index.Index(properties=p)
        idx.insert(f_i, g, obj=uid)
        # index_data.append({
        #     "id": f_i, "bbox": g, "obj": uid
        # })
        f_i += 1
    return idx

idx = get_index(r"E:\Mani\ProjectData\grids\IndiaWGS84GCS10.shp")
print(idx)
