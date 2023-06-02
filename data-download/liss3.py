import zipfile
import os
import time
from datetime import datetime, timedelta
from landsatxplore.earthexplorer import EarthExplorer
from landsatxplore.api import API
import json
from osgeo import gdal, ogr, osr
import osgeo
import math
import tarfile
import subprocess
import glob
from mpi4py import MPI


def mk_dir(pt):
    if not os.path.exists(pt):
        os.makedirs(pt)


def get_sref_file(pt):
    for fname in glob.glob(os.path.join(pt, "*_sref.kea")):
        return fname
    return None


def get_extent_of_ds(dataset):
    geotransform = dataset.GetGeoTransform()
    cols = dataset.RasterXSize
    rows = dataset.RasterYSize

    xmin = geotransform[0]
    ymax = geotransform[3]
    xmax = xmin + geotransform[1] * cols
    ymin = ymax + geotransform[5] * rows

    center_x = (xmin + xmax) / 2
    center_y = (ymin + ymax) / 2
    xmax = center_x + 1000
    xmin = center_x - 1000
    ymax = center_y + 1000
    ymin = center_y - 1000

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


def run_sprocess(cmd):
    process = subprocess.Popen((cmd).split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    print(output)


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
name = MPI.Get_processor_name()
nprocs = comm.Get_size()
l3_dir = "/mnt/data/liss3_data/processing/"
l3_zip_dir = "/mnt/data/liss3_data/zip_files/"
send_data = []


def split(a, n):
    k, m = divmod(len(a), n)
    return list(a[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n))


def process_data(l3_zip):
    try:
        start_time = time.time()
        l3sc_dir = None
        with zipfile.ZipFile(l3_zip, "r") as zip_ref:
            l3_name = l3_zip.split("/")[-1][:-4]
            l3sc_dir = os.path.join(l3_dir, l3_name)
            # if not os.path.exists(l3sc_dir):
            #     os.makedirs(l3sc_dir)
            if not os.path.exists(l3sc_dir):
                zip_ref.extractall(l3_dir)

        print("Files extracted", "--- %s seconds ---" % (time.time() - start_time))

        meta_file = os.path.join(l3sc_dir, "BAND_META.txt")

        m_params = ["ProductSceneEndTime", "SceneEndTime", "Path", "Row", "DateOfPass"]
        meta_data = {}
        with open(meta_file) as f:
            for line in f.readlines():
                _k = line.split("=")[0]
                if _k in m_params:
                    meta_data[_k] = line.split("=")[1][1:-1]
        print(meta_data)

        scene_end_dtme = datetime.strptime(
            meta_data["SceneEndTime"][:20], "%d-%b-%Y %H:%M:%S"
        )
        sc_date = datetime.strptime(meta_data["DateOfPass"], "%d-%b-%Y")
        s_date = sc_date + timedelta(days=-150)
        e_date = sc_date + timedelta(days=150)
        # print(s_date.strftime("%Y-%m-%d"), e_date.strftime("%Y-%m-%d"))

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
        l8_tar_file = os.path.join(l3_dir, scene["display_id"] + ".tar")
        l8_dir = os.path.join(l3_dir, scene["display_id"])

        if not os.path.exists(l8_tar_file):
            print("Downloading...")
            ee.download(scene["entity_id"], l3_dir, ds_name)
        else:
            print("Already downloaded")
        print(
            "Landsat Ref Scene ready", "--- %s seconds ---" % (time.time() - start_time)
        )
        if not os.path.exists(l8_dir):
            print("Extracting...")
            os.makedirs(l8_dir)
            file = tarfile.open(l8_tar_file)
            file.extractall(l8_dir)
            file.close()
        else:
            print("Already extracted")

        print(
            "Landsat scene extracted", "--- %s seconds ---" % (time.time() - start_time)
        )

        api.logout()
        ee.logout()

        """Start processing"""
        INPUT_DIR = l3sc_dir  #'data/input/1983747221/'
        REF_IMG = os.path.join(
            l8_dir, scene["display_id"] + "_SR_B2.TIF"
        )  #'data/references/1983747221/LC08_L2SP_144039_20171118_20200902_02_T1_SR_B2.TIF'
        AOT_VALUE = 0.5  # float(args.AOT) #0.5
        AEROPRO = "NoAerosols"  # args.AEROPRO #NoAerosols
        ATMOSPRO = "NoGaseousAbsorption"  # args.ATMOSPRO #NoGaseousAbsorption
        COREG_DIR = os.path.join(
            l3sc_dir, "coreg"
        )  # os.path.join(os.getcwd(), 'data/output/coreg/')
        ATM_DIR = os.path.join(
            l3sc_dir, "atm_corrected"
        )  # args.ATM_DIR #os.path.join(os.getcwd(), 'data/output/atm_corrected/')

        mk_dir(COREG_DIR)
        mk_dir(ATM_DIR)

        # ATM CORRECTION
        run_sprocess(
            "conda run -n arcsi arcsi.py -s L3 -f KEA --stats -p RAD TOA SREF --aeropro "
            + AEROPRO
            + " --atmospro "
            + ATMOSPRO
            + " --aot "
            + str(AOT_VALUE)
            + " -o "
            + ATM_DIR
            + " -i "
            + meta_file
            + ""
        )
        print("Atm corr. completed", "--- %s seconds ---" % (time.time() - start_time))
        print(ATM_DIR)
        sref_kea = get_sref_file(ATM_DIR)
        if sref_kea is None:
            raise Exception("[Error] SREF")
        print(sref_kea)

        # REPROJECT
        sref_tif_7755 = sref_kea + "_7755.tif"
        run_sprocess(
            "conda run -n arcsi gdalwarp -t_srs EPSG:7755 "
            + sref_kea
            + " "
            + sref_tif_7755
        )
        print("SR reprojected", "--- %s seconds ---" % (time.time() - start_time))
        REF_IMG_7755 = REF_IMG + "_7755.tif"
        run_sprocess(
            "conda run -n arcsi gdalwarp -t_srs EPSG:7755 "
            + REF_IMG
            + " "
            + REF_IMG_7755
        )
        print(
            "Landsat Ref reprojected", "--- %s seconds ---" % (time.time() - start_time)
        )

        # COREG
        sref_coreg = os.path.join(COREG_DIR, "coreg.tif")
        run_sprocess(
            "conda run -n arosics python /mnt/data/common/liss3processing/img_reg.py -cod "
            + COREG_DIR
            + " -i "
            + sref_tif_7755
            + " -r "
            + REF_IMG_7755
            + " -o "
            + sref_coreg
        )
        print("Coreg completed", "--- %s seconds ---" % (time.time() - start_time))
        band_files = [
            os.path.join(COREG_DIR, "corrected_band_1.tif"),
            os.path.join(COREG_DIR, "corrected_band_2.tif"),
            os.path.join(COREG_DIR, "corrected_band_3.tif"),
            os.path.join(COREG_DIR, "corrected_band_4.tif"),
        ]
        out_file = os.path.join(l3sc_dir, "output.tif")
        run_sprocess(
            "gdalbuildvrt -separate stack.vrt "
            + band_files[0]
            + " "
            + band_files[1]
            + " "
            + band_files[2]
            + " "
            + band_files[3]
        )
        run_sprocess("gdal_translate stack.vrt " + out_file)
        print("Output generated", "--- %s seconds ---" % (time.time() - start_time))
        return {
            "ts": scene_end_dtme.timestamp() * 1000,
            "sensor": "LISS3",
            "filePath": out_file,
        }
    except Exception as e:
        print("Exception", e)
        return None


oa_start_time = time.time()

if rank == 0:
    scenes = []
    print("Splitting image tasks...", name, rank)
    for l3_zip_fn in os.listdir(l3_zip_dir):
        l3_zip = os.path.join(l3_zip_dir, l3_zip_fn)
        # process_data(l3_zip)
        scenes.append(l3_zip)
        # break
    send_data = split(scenes, nprocs)
else:
    print("Other node", name, rank)
    send_data = None

zip_files_to_process = comm.scatter(send_data)

print("Do process with the data", len(zip_files_to_process), name, rank)

ingestion_inputs = []

for l3_zip_fn_1 in zip_files_to_process:
    # print(l3_zip_fn_1)
    process_out = process_data(l3_zip_fn_1)
    if process_out is not None:
        ingestion_inputs.append(process_out)

with open(
    f"/mnt/data/common/liss3processing/ingestion_inputs_{name}_{rank}.json", "w"
) as fp:
    json.dump(ingestion_inputs, fp)

print(
    "All processes completed",
    name,
    rank,
    "--- %s seconds ---" % (time.time() - oa_start_time),
)

# mpiexec -machinefile /mnt/data/machinefile -n 8 -x LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH -x PATH=/mnt/data/common/anaconda3/bin:$PATH /mnt/data/env/project/bin/python /mnt/data/common/liss3processing/mprocess.py > /mnt/data/common/liss3processing/out.logs
# /mnt/data/common/liss3processing
