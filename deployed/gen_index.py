import json
import os
import rtree
from osgeo import ogr

config = json.load(open("/mnt/data/common/config.json"))


def generate_index(shapefile_path):
    ds = ogr.Open(shapefile_path)
    idx = None
    lyr = ds.GetLayer()
    f_count = lyr.GetFeatureCount()
    f_i = 1
    for feature in lyr:
        g = feature.GetGeometryRef()
        spatial_partition_index = tuple(
            map(
                int, feature.GetField("id").replace("(", "").replace(")", "").split(",")
            )
        )
        level_id = spatial_partition_index[2]
        x_id = spatial_partition_index[0]
        y_id = spatial_partition_index[1]
        uid = f"{level_id}#{x_id}#{y_id}"
        g = g.GetEnvelope()
        g = (g[0], g[2], g[1], g[3])
        print(f"{f_i}/{f_count}, {g}", end="\r")
        if idx is None:
            p = rtree.index.Property()
            idx = rtree.index.Index(
                os.path.join(config["sindex_dir"], f"spatial_index_{level_id}"),
                properties=p,
            )
        idx.insert(f_i, g, obj=uid)
        f_i += 1
    idx.close()
    return True


shapefile_paths = []
for f in os.listdir(config["grid_dir"]):
    if f.endswith(".shp"):
        shapefile_paths.append(os.path.join(config["grid_dir"], f))
for shapefile_path in shapefile_paths:
    print(shapefile_path)
    generate_index(shapefile_path)
