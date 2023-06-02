package com.gishorizon.reader

import com.gishorizon.operations.{FpcaTemporal, ProcessInput, ProcessOperation}
import com.gishorizon.{Logger, RddUtils, Spark}
import geotrellis.layer.{Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import geotrellis.spark.ContextRDD
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import play.api.libs.json._
import geotrellis.raster.io.geotiff

import scala.collection.mutable.Map
import java.sql.{Connection, DriverManager}
import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import scala.collection.{immutable, mutable}


object InputReader {
  val fps:Array[String] = Array(
//    "G:/9/9/368/209/822979094.tif",
//    "G:/9/9/368/211/822979094.tif",
//    "G:/9/9/368/210/822979094.tif",
//    "G:/9/9/366/212/822979094.tif",
//    "G:/9/9/366/211/822979094.tif",
//    "G:/9/9/366/210/822979094.tif",
//    "G:/9/9/366/209/822979094.tif",
//    "G:/9/9/366/209/822979071.tif",
//    "G:/9/9/366/208/822979071.tif",
//    "G:/9/9/366/207/822979071.tif",
//    "G:/9/9/366/206/822979071.tif",
//    "G:/9/9/367/214/823756344.tif",
//    "G:/9/9/367/213/823756344.tif",
//    "G:/9/9/367/212/822979094.tif",
//    "G:/9/9/367/212/820991548.tif",
//    "G:/9/9/367/211/822979094.tif",
//    "G:/9/9/367/209/822979094.tif",
//    "G:/9/9/367/210/822979094.tif",
//    "G:/9/9/367/209/822979071.tif",
//    "G:/9/9/367/208/822979071.tif",
//    "G:/9/9/367/207/822979071.tif",
//    "G:/9/9/367/206/822979071.tif",
//    "G:/9/9/368/209/822979071.tif",
//    "G:/9/9/368/208/822979071.tif",
//    "G:/9/9/368/207/828508646.tif",
//    "G:/9/9/368/213/820991548.tif",
//    "G:/9/9/368/212/820991548.tif",
//    "G:/9/9/368/211/820991548.tif",
//    "G:/9/9/368/211/820991524.tif",
//    "G:/9/9/368/210/820991524.tif",
//    "G:/9/9/368/209/822373925.tif",
//    "G:/9/9/368/207/822979071.tif",
//    "G:/9/9/368/206/822979071.tif",
//    "G:/9/9/369/214/820991548.tif",
//    "G:/9/9/369/213/820991548.tif",
//    "G:/9/9/369/212/820991548.tif",
//    "G:/9/9/369/211/820991524.tif",
//    "G:/9/9/369/210/820991524.tif",
//    "G:/9/9/369/209/820991524.tif",
//    "G:/9/9/369/207/828508646.tif",
//    "G:/9/9/370/214/820991548.tif",
//    "G:/9/9/370/213/820991548.tif",
//    "G:/9/9/370/212/820991548.tif",
//    "G:/9/9/370/211/821768754.tif",
//    "G:/9/9/370/211/820991524.tif",
//    "G:/9/9/370/210/821768754.tif",
//    "G:/9/9/370/210/820991524.tif",
//    "G:/9/9/370/209/820991524.tif",
//    "G:/9/9/371/210/825915942.tif",
//    "G:/9/9/371/211/825915942.tif"
    "G:/9/9/370/210/821768754.tif",
    "G:/9/9/370/210/820991524.tif",
//    "G:/9/9/370/211/821768754.tif",
//    "G:/9/9/370/211/820991524.tif",
  )
  val fps2:Array[String] = Array(
    "G:/9/9/370/210/971067969.tif",
    "G:/9/9/370/210/959231487.tif",
//    "G:/9/9/370/211/971067969.tif",
//    "G:/9/9/370/211/959231487.tif",
  )
  private def getConnection: Connection = {
    classOf[org.postgresql.Driver]
    val connStr = "jdbc:postgresql://localhost:5432/project_master?user=postgres&password=manichan"
    val conn = DriverManager.getConnection(connStr)
    conn
  }

  private def getAoi(aoiCode: String): Unit = {
    val conn = getConnection
    val st = conn.createStatement()
    val resultSet = st.executeQuery("select aoi_code, st_asgeojson(geom) as geom from user_aoi where aoi_code='"+aoiCode+"';")
    while (resultSet.next()) {
      val aoiCode = resultSet.getString("aoi_code")
      val geom = resultSet.getString("geom")
      println(aoiCode, geom)
    }
  }



  private def getInputRdd(sc: SparkContext, processInput: ProcessInput): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
    val data = HttpUtils.getRequestSync(s"http://localhost:8082/getDataRefForAoi/?sensorName=${processInput.dsName}&tIndex=${processInput.tIndexes(0)}&level=12&aoiCode=${processInput.aoiCode}")
    val filePaths = data.asInstanceOf[JsObject].value("data").asInstanceOf[JsArray]
    var rdds: Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]] = Array()
    for (i <- filePaths.value.indices) {
      val filePath = filePaths(i).asInstanceOf[JsString].value
      var rdd = RddUtils.getMultiTiledRDDWithMeta(sc, filePath, 256)
      rdds = rdds :+ rdd
    }
    RddUtils.mergeRdds(rdds)
  }

  private def getInputRdd1Temporal(sc: SparkContext, processInput: ProcessInput): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {
    val data = HttpUtils.getRequestSync(s"http://localhost:8082/getDataRefForAoi/?sensorName=${processInput.dsName}&tIndex=${processInput.tIndexes(0)}&level=12&aoiCode=${processInput.aoiCode}")
    val filePaths = data.asInstanceOf[JsObject].value("data").asInstanceOf[JsArray]
    var rdds: Array[RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]] = Array()
    for (i <- filePaths.value.indices) {
      val filePath = filePaths(i).asInstanceOf[JsString].value
      val tIndex = filePath.split("/").last.split(".tif").head.toInt
      val sTs = ZonedDateTime.parse(f"1990-01-01T00:00:00Z", DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.ofHoursMinutes(0, 0))).toInstant.toEpochMilli
      val dt = ZonedDateTime.ofInstant(
        Instant.ofEpochMilli((sTs + tIndex * 1000))
        , DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.ofHoursMinutes(0, 0)).getZone
      )
      var rdd = RddUtils.getMultiTiledTemporalRDDWithMeta(sc, filePath, 256, dt)
      rdds = rdds :+ rdd
    }
    RddUtils.mergeTemporalRdds(rdds)
  }
var iii = 0
  private def getInputRddTemporal(sc: SparkContext, processInput: ProcessInput)
  : RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]
  = {

//    val data = HttpUtils.getRequestSync(s"http://localhost:8082/getDataRefsForAoi/?sensorName=${processInput.dsName}&tIndexes=${processInput.tIndexes.mkString("", ",", "")}&level=12&aoiCode=${processInput.aoiCode}")
//    val filePaths = data.asInstanceOf[JsObject].value("data").asInstanceOf[JsArray]
    var rdds: Array[RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]] = Array()
    val _fps = if(iii%2==0) fps else fps2
    iii += 1
    for (i <- _fps.indices) {
      val filePath = _fps(i)
//      val filePath = filePaths(i).asInstanceOf[JsString].value
      Logger.log("Reading " + filePath)
      val tIndex = filePath.split("/").last.split(".tif").head.toInt
      val sTs = ZonedDateTime.parse(f"1990-01-01T00:00:00Z", DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.ofHoursMinutes(0, 0))).toInstant.toEpochMilli
      val dt = ZonedDateTime.ofInstant(
        Instant.ofEpochMilli((sTs + tIndex * 1000L))
        , DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.ofHoursMinutes(0, 0)).getZone
      )
      var rdd = RddUtils.getMultiTiledTemporalRDDWithMeta(sc, filePath, 256, dt)
      rdds = rdds :+ rdd
    }
    val merged = RddUtils.mergeTemporalRdds(rdds)
    Logger.log("Merged: " + processInput.id)
    merged
  }

//  def main(args: Array[String]): Unit = {
//    /**
//     * {
//     * "id": "I1",
//     * "tIndexes": [914925604, 913543204],
//     * "isTemporal": true,
//     * "aoiCode": "qwertyuiopasdfgh",
//     * "dsName": "Landsat_OLI"
//     * }
//     */
//    val sc = Spark.context
//    getAoi("qwertyuiopasdfgh")
//    HttpUtils.getRequest("http://localhost:8082/getDataRefsForAoi/?sensorName=Landsat_OLI&tIndexes=914925604,913543204&level=12&aoiCode=qwertyuiopasdfgh", {
//      (flag, data) => {
//        val filePaths = data.asInstanceOf[JsObject].value("data").asInstanceOf[JsArray]
//        var rdds: Array[RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]] = Array()
//        for (i <- filePaths.value.indices){
//          val filePath = filePaths(i).asInstanceOf[JsString].value
//          val tIndex = filePath.split("/").last.split(".tif").head.toInt
//          val sTs = ZonedDateTime.parse(f"1990-01-01T00:00:00Z", DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.ofHoursMinutes(0, 0))).toInstant.toEpochMilli
//          val dt = ZonedDateTime.ofInstant(
//            Instant.ofEpochMilli((sTs + tIndex*1000))
//            ,DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.ofHoursMinutes(0, 0)).getZone
//          )
//          var rdd = RddUtils.getMultiTiledTemporalRDDWithMeta(sc, filePath, 256, dt)
//          rdds = rdds :+ rdd
//        }
//        val mergedRdd = RddUtils.mergeTemporalRdds(rdds)
//        val mds = immutable.Map(
//          "1" -> mergedRdd,
//          "2" -> mergedRdd
//        )
//        val po = new ProcessOperation()
////        po.inputs =
//        FpcaTemporal.runProcess(mds, po)
//        println(rdds)
//      }
//    })
////    val inputFilePath = "G:\\ProjectData\\temp_data\\RPXIIRQIXNJDCXYF.tif"
////    val rdd = RddUtils.getMultiTiledRDD(sc, inputFilePath, 256)
//
//  }

  def getInputs(sc: SparkContext, inputs: Array[ProcessInput]): mutable.Map[String, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]] = {
    var rddMap = mutable.Map[String, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]]()
    for(input <- inputs){
      val rdd: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = if (input.isTemporal) getInputRddTemporal(sc, input) else getInputRdd1Temporal(sc, input)
      rddMap += (
        input.id -> rdd
      )
    }
    rddMap
  }

}
