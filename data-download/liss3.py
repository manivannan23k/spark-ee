import zipfile
import os
from datetime import datetime, timedelta
from landsatxplore.earthexplorer import EarthExplorer
from landsatxplore.api import API
from osgeo import gdal, ogr, osr
import osgeo
import math

l3_dir = "G:/ProjectData/Liss3/"
l3_zip = os.path.join(l3_dir, "1983747101.zip")
with zipfile.ZipFile(l3_zip, "r") as zip_ref:
    l3_name = l3_zip.split("/")[-1][:-4]
    l3sc_dir = os.path.join(l3_dir, l3_name)
    # if not os.path.exists(l3sc_dir):
    #     os.makedirs(l3sc_dir)
    # zip_ref.extractall(l3_dir)

print("Files extracted.")

meta_file = os.path.join(l3sc_dir, "BAND_META.txt")


m_params = ["ProductSceneEndTime", "SceneEndTime", "Path", "Row", "DateOfPass"]
meta_data = {}
with open(meta_file) as f:
    for line in f.readlines():
        _k = line.split("=")[0]
        if _k in m_params:
            meta_data[_k] = line.split("=")[1][1:-1]
print(meta_data)


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


def get_dist(bbox1, bbox2):
    c1 = ((bbox1[0] + bbox1[2]) / 2, (bbox1[1] + bbox1[3]) / 2)
    c2 = ((bbox2[0] + bbox2[2]) / 2, (bbox2[1] + bbox2[3]) / 2)
    dist = math.sqrt(
        (((c1[0] - c2[0]) * (c1[0] - c2[0])) + ((c1[1] - c2[1]) * (c1[1] - c2[1])))
    )
    return dist


def closest_scene(scenes, aoi_bbox):
    min_dist = 100000.0
    m_scene = None
    for scene in scenes:
        d = get_dist(scenes[0]["spatial_bounds"], aoi_bbox)
        if d < min_dist:
            min_dist = d
            m_scene = scene
    print("Min dist: ", min_dist)
    return m_scene


# def get_aoi(aoi_shp_path):
#     aoi_ds = ogr.Open(aoi_shp_path)
#     for feature in aoi_ds.GetLayer():
#         aoi_bbox = feature.GetGeometryRef().GetEnvelope()
#         aoi_bbox = (aoi_bbox[0], aoi_bbox[2], aoi_bbox[1], aoi_bbox[3])
#         break
#     return aoi_bbox

sc_date = datetime.strptime(meta_data["DateOfPass"], "%d-%b-%Y")
s_date = sc_date + timedelta(days=-150)
e_date = sc_date + timedelta(days=150)
print(s_date.strftime("%Y-%m-%d"), e_date.strftime("%Y-%m-%d"))

in_ds = gdal.Open(os.path.join(l3sc_dir, "BAND2.tif"))
extent_geom = get_extent_of_ds(in_ds)
proj = osr.SpatialReference(wkt=in_ds.GetProjection())
src_src_code = int(proj.GetAttrValue("AUTHORITY", 1))
extent_geom = reproject(extent_geom, src_src_code, 4326)
extent_geom.FlattenTo2D()
aoi_geom = extent_geom.GetEnvelope()
aoi_geom = (aoi_geom[0], aoi_geom[2], aoi_geom[1], aoi_geom[3])

ds_name = "landsat_ot_c2_l2"
api = API("manivannan23111996@gmail.com", "Manivannan23,")
scenes = api.search(
    dataset=ds_name,
    bbox=aoi_geom,
    start_date=s_date.strftime("%Y-%m-%d"),
    end_date=e_date.strftime("%Y-%m-%d"),
    max_cloud_cover=10,
)

# print(scenes[0]["spatial_bounds"])
# print(aoi_geom)
scene = closest_scene(scenes, aoi_geom)
print(scene)

ee = EarthExplorer("manivannan23111996@gmail.com", "Manivannan23,")
ee.download(scene["entity_id"], l3_dir, ds_name)


api.logout()
ee.logout()
