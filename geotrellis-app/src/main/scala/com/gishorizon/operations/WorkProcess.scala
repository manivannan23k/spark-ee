package com.gishorizon.operations

import com.gishorizon.Spark
import com.gishorizon.reader.{HttpUtils, InputReader}
import geotrellis.layer.{LayoutDefinition, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.{MultibandTile, Raster, Tile}
import org.apache.spark.rdd.RDD
import geotrellis.raster.{io => _, _}
import geotrellis.spark._
import geotrellis.spark.stitch._
import geotrellis.vector.ProjectedExtent

import java.io.File
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

  def run(process: ProcessConfig): Unit = {
    implicit val sc = Spark.context
    val inputsData: mutable.Map[String, Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]]] = InputReader.getInputs(sc, process.inputs)
    for (operation <- process.operations) {
      if(operation.opType=="op_ndi"){
        val result = NDI.runProcess(inputsData.toMap, operation)
        inputsData += (operation.output.id -> result)
      }
      if (operation.opType == "op_lavg") {
//        val result = NDI.runProcess(inputsData.toMap, operation)
//        inputsData += (operation.output.id -> result)
      }
      if (operation.opType == "op_savgol") {
                val result = SavGolFilter.runProcess(inputsData.toMap, operation)
                inputsData += (operation.output.id -> result)
      }


    }
    val outputData = inputsData(process.output.id)
    var meta = outputData(0).metadata
    outputData.foreach{
      o => {
        meta = meta.merge(o.metadata)
      }
    }
    val ratio = Math.round(((meta.extent.xmax - meta.extent.xmin)/(meta.extent.ymax - meta.extent.ymin))/(((outputData(0).metadata.extent.xmax-outputData(0).metadata.extent.xmin))/((outputData(0).metadata.extent.ymax-outputData(0).metadata.extent.ymin))))
    val xTileSize = 256 * (outputData.length*ratio)
    val yTileSize = 256 * (outputData.length/ratio)
    meta = TileLayerMetadata(meta.cellType, new LayoutDefinition(meta.layout.extent, new TileLayout(1,1,yTileSize.toInt, xTileSize.toInt)), meta.extent, meta.crs, meta.bounds)
    println(meta)
    val outProj: Array[RDD[(ProjectedExtent, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]] = outputData.map(o=>{
      ContextRDD(
        o.map {
          case (k, t) => {
            (ProjectedExtent(o.metadata.mapTransform(k), o.metadata.crs), t)
          }
        }, o.metadata
      )
    })
    var out: RDD[(ProjectedExtent, MultibandTile)] = outProj(0)
    outProj.foreach{
      o=>{
        out = out.merge(o)
      }
    }
    val result = ContextRDD(out, meta)
    val rdd = result.tileToLayout(meta)
    val raster: Raster[MultibandTile] = rdd.stitch
    val fPath = f"E:\\Mani\\ProjectData\\temp_data\\${
      Iterator.continually(Random.nextPrintableChar)
        .filter(_.isLetter)
        .take(16)
        .mkString}.tif"
    GeoTiff(raster, rdd.metadata.crs).write(fPath)

    //ingest
    val ts = ZonedDateTime.now().toInstant.toEpochMilli

//    val url = s"http://localhost:8082/ingestData?filePath=${fPath}&ts=${ts}&sensorName=SingleRasterBand"
//    val ingestResult = HttpUtils.getRequestSync(url)
//    val f = new File(fPath)
//    println(fPath)
//    if(f.exists()){
//      f.delete()
//    }
//    println(ingestResult)
  }

}
