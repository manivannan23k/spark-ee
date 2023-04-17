package com.gishorizon.reader

import com.gishorizon.{RddUtils, Spark}
import geotrellis.layer.{Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.raster.MultibandTile
import geotrellis.spark.ContextRDD
import org.apache.spark.rdd.RDD
import play.api.libs.json._

import java.sql.{Connection, DriverManager}

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



  def getInputRdd(): Unit = {

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
    HttpUtils.getRequest{
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
    }
//    val inputFilePath = "G:\\ProjectData\\temp_data\\RPXIIRQIXNJDCXYF.tif"
//    val rdd = RddUtils.getMultiTiledRDD(sc, inputFilePath, 256)

  }

}
