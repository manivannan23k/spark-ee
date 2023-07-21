const Config = {
    DATA_HOST: "http://35.239.22.76:8082",
    PROCESS_HOST: "http://localhost:8081"
}
export default Config;

// with aoi as
// (
//     select ST_Envelope(geom) as geom from user_aoi where aoi_code='OBRXTEDWGRARTOYM'
// )
// select ingest_master.ingest_id, ingest_master.dataset_id, ingest_master.time_index, ingest_master.date_time from ingest_master, aoi
//     where ingest_master.dataset_id=1