import rtree
from osgeo import ogr, osr
import osgeo

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
            idx = rtree.index.Index(f"index_dat/spatial_index_{level_id}", properties=p)
        idx.insert(f_i, g, obj=uid)
        f_i += 1
    idx.close()
    return True

def load_indexes(level_ids):
    index_data = {}
    for level_id in level_ids:
        index_data[level_id] = rtree.index.Index(f"index_dat/spatial_index_{level_id}")
    return index_data