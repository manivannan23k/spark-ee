package com.gishorizon.operations

import com.gishorizon.RddUtils.mergeRdds
import geotrellis.layer.{LayoutDefinition, Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.raster.{CellType, MultibandTile, TileLayout}
import geotrellis.spark.{ContextRDD, withTileRDDMergeMethods}
import geotrellis.vector.ProjectedExtent
import org.apache.spark.rdd.RDD
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

object LocalAvg {

  def runProcess(inputs: Map[String, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]], operation: ProcessOperation): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {

    var inRdds: Array[RDD[(SpatialKey, MultibandTile)]] = Array()
    var meta: TileLayerMetadata[SpatialKey] = null
    val iCount = operation.inputs.length

    for(i <- operation.inputs.indices){
      val op = operation.inputs(i)
      val rdd = inputs(op.id)
      val bid = op.band
      val r: RDD[(SpatialKey, MultibandTile)] = rdd.map {
        case (k: SpatialKey, mt: MultibandTile) => {
          val _mt: MultibandTile = ArrayMultibandTile(mt.band(bid))
          (k, _mt)
        }
      }
      meta = rdd.metadata
      inRdds = inRdds :+ r
    }
    val or: RDD[(SpatialKey, MultibandTile)] = inRdds.reduce{
      (rdd1, rdd2) => {
        rdd1.++(rdd2).reduceByKey(
          (t1: MultibandTile, t2: MultibandTile) => {
            ArrayMultibandTile(t1.band(0).convert(CellType.fromName("float32"))
              .combineDouble(t2.band(0).convert(CellType.fromName("float32"))) {
                (v1, v2) => {
                  v1 + v2
                }
              })
          }
        )
      }
    }.map{
      case (k, mt) => {
        (k, mt.mapDouble(0){
          (v)=>{
            v/iCount
          }
        })
      }
    }
//      .map{
//      case(k, t) => {
//        val mt: MultibandTile = new ArrayMultibandTile(Array(t))
//        (k, mt)
//      }
//    }
    ContextRDD(or, meta)
  }




}
