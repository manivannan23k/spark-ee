package com.gishorizon.reader

import com.gishorizon.operations.ProcessInput
import com.gishorizon.{RddUtils, Spark}
import geotrellis.layer.{Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import geotrellis.spark.ContextRDD
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import play.api.libs.json._

import scala.collection.mutable.Map
import java.sql.{Connection, DriverManager}
import scala.collection.mutable

object InputReader {

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



  private def getInputRdd(sc: SparkContext, processInput: ProcessInput): Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]] = {
    val data = HttpUtils.getRequestSync(s"http://localhost:8082/getDataRefForAoi/?sensorName=${processInput.dsName}&tIndex=${processInput.tIndexes(0)}&level=12&aoiCode=${processInput.aoiCode}")
    val filePaths = data.asInstanceOf[JsObject].value("data").asInstanceOf[JsArray]
    var rdds: Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]] = Array()
    for (i <- filePaths.value.indices) {
      val filePath = filePaths(i).asInstanceOf[JsString].value
      var rdd = RddUtils.getMultiTiledRDDWithMeta(sc, filePath, 256)
      rdds = rdds :+ rdd
    }
    rdds
  }

  def main(args: Array[String]): Unit = {
    /**
     * {
     * "id": "I1",
     * "tIndexes": [914925604, 913543204],
     * "isTemporal": true,
     * "aoiCode": "qwertyuiopasdfgh",
     * "dsName": "Landsat_OLI"
     * }
     */
    val sc = Spark.context
    getAoi("qwertyuiopasdfgh")
    HttpUtils.getRequest("http://localhost:8082/getDataRefForAoi/?sensorName=Landsat_OLI&tIndex=914925604&level=12&aoiCode=qwertyuiopasdfgh", {
      (flag, data) => {
        val filePaths = data.asInstanceOf[JsObject].value("data").asInstanceOf[JsArray]
        var rdds: Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]] = Array()
        for (i <- filePaths.value.indices){
          val filePath = filePaths(i).asInstanceOf[JsString].value
          var rdd = RddUtils.getMultiTiledRDDWithMeta(sc, filePath, 256)
          rdds = rdds :+ rdd
        }
        println(rdds)
      }
    })
//    val inputFilePath = "G:\\ProjectData\\temp_data\\RPXIIRQIXNJDCXYF.tif"
//    val rdd = RddUtils.getMultiTiledRDD(sc, inputFilePath, 256)

  }

  def getInputs(sc: SparkContext, inputs: Array[ProcessInput]): mutable.Map[String, Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]]] = {
    var rddMap = mutable.Map[String, Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]]]()
    for(input <- inputs){
      val rdd = getInputRdd(sc, input)
      rddMap += (
        input.id -> rdd
      )
    }
    rddMap
  }

}
