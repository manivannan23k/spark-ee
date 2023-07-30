package com.gishorizon.operations

import com.gishorizon.{DataConfigs, Logger, Spark}
import com.gishorizon.reader.{HttpUtils, InputReader}
import geotrellis.layer.TileLayerMetadata.toLayoutDefinition
import geotrellis.layer.{Bounds, FloatingLayoutScheme, LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TemporalKey, TileLayerMetadata}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.{MultibandTile, Raster, Tile, io => _, _}
import org.apache.spark.rdd.RDD
import geotrellis.spark._
import geotrellis.spark.stitch._
import geotrellis.spark.store.RasterReader
import geotrellis.spark.store.hadoop.HadoopGeoTiffRDD
import play.api.libs.json.{JsArray, JsObject, JsValue, Json}
import geotrellis.vector.ProjectedExtent
import org.apache.hadoop.fs.Path
import org.locationtech.proj4j.CoordinateReferenceSystem
import play.api.libs.json.{JsValue, Json}

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.ZonedDateTime
import scala.collection.mutable
import scala.util.Random

object WorkProcess {

  def sampleProcess(): ProcessConfig = {
    val processConfig = new ProcessConfig(

    )

    val input = new ProcessInput()
    input.id = "I1"
    input.tIndexes = Array(914925604, 913543204)
    input.dsName = "Landsat_OLI"
    input.aoiCode = "qwertyuiopasdfgh"
    input.isTemporal = true

    val input2 = new ProcessInput()
    input2.id = "I1"
    input2.band = 5

    val input3 = new ProcessInput()
    input3.id = "I1"
    input3.band = 4

    val output2 = new ProcessOutput()
    output2.id = "O1"

    val operation = new ProcessOperation()
    operation.id = "Op1"
    operation.inputs = Array(input2, input3)
    operation.opType = "NDI"
    operation.output = output2

    val inputs = Array[ProcessInput](
      input
    )
    val operations = Array[ProcessOperation](
      operation
    )

    val output1 = new ProcessOutput()
    output1.id = "O1"

    processConfig.inputs = inputs
    processConfig.operations = operations
    processConfig.output = output1

    processConfig
  }

  def run(process: ProcessConfig): Array[String] = {
    implicit val sc = Spark.context
    val inputsData: mutable.Map[String, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]] = InputReader.getInputs(sc, process.inputs)
    for (operation <- process.operations) {
      if(operation.opType=="op_ndi"){
        val result = NDI.runProcess(inputsData.toMap, operation)
        inputsData += (operation.output.id -> result)
      }
      if (operation.opType == "op_local_avg") {
        val result = LocalAvg.runProcess(inputsData.toMap, operation)
        inputsData += (operation.output.id -> result)
      }
      if (operation.opType == "op_local_dif") {
        val result = LocalDif.runProcess(inputsData.toMap, operation)
        inputsData += (operation.output.id -> result)
      }
      if (operation.opType == "op_savgol") {
        val result = SavGolFilter.runProcess(inputsData.toMap, operation)
        inputsData += (operation.output.id -> result)
      }
      if (operation.opType == "op_fpca") {
        val result = FpcaTemporal.runProcess(inputsData.toMap, operation)
        inputsData += (operation.output.id -> result)
      }
      if (operation.opType == "op_cd") {
        val result = ChangeDetection.runProcess(inputsData.toMap, operation)
        inputsData += (operation.output.id -> result)
      }
      if (operation.opType == "op_mosaic") {
        val result = Mosaic.runProcess(inputsData.toMap, operation)
        inputsData += (operation.output.id -> result)
      }
      if (operation.opType == "op_bandsel") {
        val result = BandSelector.runProcess(inputsData.toMap, operation)
        inputsData += (operation.output.id -> result)
      }
      if (operation.opType == "op_tstomb") {
        val result = TimeSeriesToMultiBand.runProcess(inputsData.toMap, operation)
        inputsData += (operation.output.id -> result)
      }
    }
    val outputId = process.output.id
    val outMeta = inputsData(outputId).metadata
    // RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]
//    val rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(inputsData(outputId).map { case (key, tile) => (key.spatialKey, tile) }, TileLayerMetadata(outMeta.cellType, outMeta.layout, outMeta.extent, outMeta.crs, outMeta.bounds.asInstanceOf[Bounds[SpatialKey]]))
//    val raster: Raster[MultibandTile] = rdd.stitch()
//    Logger.log("Stitch complete")
//    val fPath = f"${DataConfigs.TEMP_PATH}${
//      Iterator.continually(Random.nextPrintableChar)
//        .filter(_.isLetter)
//        .take(16)
//        .mkString
//    }.tif"
//    GeoTiff(raster, outMeta.crs).write(fPath)
//    Array(fPath)
    val tPath = DataConfigs.TEMP_PATH
    val fPaths = inputsData(outputId)
      .map { case (key, tile) => (key.instant, (key.spatialKey, tile)) }
      .groupByKey()
      .map{
      case (t, d) => {
        val od: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(Spark.context.parallelize(d.toSeq), TileLayerMetadata(outMeta.cellType, outMeta.layout, outMeta.extent, outMeta.crs, outMeta.bounds.asInstanceOf[Bounds[SpatialKey]]))
        val raster: Raster[MultibandTile] = od.stitch
        println("Stitch complete")
        val fPath = f"$tPath${
          Iterator.continually(Random.nextPrintableChar)
            .filter(_.isLetter)
            .take(16)
            .mkString
        }.tif"
        GeoTiff(raster, outMeta.crs).write(fPath)
        fPath
      }
    }.collect()
    fPaths
  }

  def runJob(strJsonData: String, processId: String): Unit = {
    val json: JsValue = Json.parse(strJsonData)
    val inJsArray = json.asInstanceOf[JsObject].value("inputs").asInstanceOf[JsArray]
    val opJsArray = json.asInstanceOf[JsObject].value("operations").asInstanceOf[JsArray]

    val processConfig = new ProcessConfig()
    var inputs: Array[ProcessInput] = Array()
    var operations: Array[ProcessOperation] = Array()
    val output = new ProcessOutput()
    output.id = json.asInstanceOf[JsObject].value("output").asInstanceOf[JsObject].value("id").asInstanceOf[play.api.libs.json.JsString].value

    for (inJs <- inJsArray.value.indices) {
      val iId = inJsArray.value(inJs).asInstanceOf[JsObject].value("id").asInstanceOf[play.api.libs.json.JsString].value
      val tIndexes = inJsArray.value(inJs).asInstanceOf[JsObject].value("tIndexes").asInstanceOf[JsArray].value.map(e => e.as[BigInt]).toArray
      val aoiCode = inJsArray.value(inJs).asInstanceOf[JsObject].value("aoiCode").asInstanceOf[play.api.libs.json.JsString].value
      val dsName = inJsArray.value(inJs).asInstanceOf[JsObject].value("dsName").asInstanceOf[play.api.libs.json.JsString].value
      val pIn = new ProcessInput()
      pIn.id = iId
      pIn.tIndexes = tIndexes
      pIn.isTemporal = true
      pIn.aoiCode = aoiCode
      pIn.dsName = dsName
      inputs = inputs :+ pIn
    }
    for (opJs <- opJsArray.value.indices) {
      val iId = opJsArray.value(opJs).asInstanceOf[JsObject].value("id").asInstanceOf[play.api.libs.json.JsString].value
      val opInputs = opJsArray.value(opJs).asInstanceOf[JsObject].value("inputs").asInstanceOf[JsArray].value.map(e => {
        val lId = e.asInstanceOf[JsObject].value("id").asInstanceOf[play.api.libs.json.JsString].value
        val lBand = e.asInstanceOf[JsObject].value("band").asInstanceOf[play.api.libs.json.JsNumber].value
        val opIn = new ProcessInput()
        opIn.id = lId
        opIn.band = lBand.toInt
        opIn
      }).toArray
      val opType = opJsArray.value(opJs).asInstanceOf[JsObject].value("type").asInstanceOf[play.api.libs.json.JsString].value
      val opParams = opJsArray.value(opJs).asInstanceOf[JsObject].value("params").asInstanceOf[play.api.libs.json.JsString].value
      val opOutput = new ProcessOutput()
      val opId = opJsArray.value(opJs).asInstanceOf[JsObject].value("output").asInstanceOf[JsObject].value("id").asInstanceOf[play.api.libs.json.JsString].value
      opOutput.id = opId
      val pIn = new ProcessOperation()
      pIn.id = iId
      pIn.opType = opType
      pIn.inputs = opInputs
      pIn.output = opOutput
      pIn.params = opParams
      operations = operations :+ pIn
    }

    processConfig.inputs = inputs
    processConfig.output = output
    processConfig.operations = operations

    val result = WorkProcess.run(processConfig)
    print(result.mkString(","))
    Files.write(Paths.get(DataConfigs.OUT_PATH + processId + ".out"), result.mkString(", ").getBytes(StandardCharsets.UTF_8))
    print("Process completed")
  }

  def main(args: Array[String]): Unit = {
    val processData: String = args(0)
    val processId: String = args(1)
    if(args.length>2){
      //tile path
      DataConfigs.TILE_PATH = args(2)
    }
    if (args.length > 3) {
      //tile path
      DataConfigs.TEMP_PATH = args(3)
    }
    if (args.length > 4) {
      //tile path
      DataConfigs.DATA_HOST = args(4)
    }
    if (args.length > 5) {
      //tile path
      DataConfigs.OUT_PATH = args(5)
    }
    if (args.length > 6) {
      //tile path
      DataConfigs.SPARK_MASTER = args(6)
    }
    if (args.length > 7) {
      //tile path
      DataConfigs.DRI_MEM = args(7)
    }
    if (args.length > 7) {
      //tile path
      DataConfigs.EXE_MEM = args(7)
    }
    runJob(processData, processId)
//val layer: RDD[(ProjectedExtent, MultibandTile)] = HadoopGeoTiffRDD[ProjectedExtent, ProjectedExtent, MultibandTile](
//  path = new Path("G:\\ProjectData\\tiles\\Landsat_OLI\\12\\3064\\1735\\914925604.tif"),
//  uriToKey = {
//    case (uri, projectedExtent) =>
//      projectedExtent
//  },
//  options = HadoopGeoTiffRDD.Options.DEFAULT
//)(Spark.context, RasterReader.multibandGeoTiffReader)
//    val (zoom, meta) = CollectTileLayerMetadata.fromRDD[ProjectedExtent, MultibandTile, SpatialKey](layer, FloatingLayoutScheme(256))
//    val tiled: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(
//      layer.tileToLayout(meta.cellType, meta.layout),
//      meta
//    )
  }

}
