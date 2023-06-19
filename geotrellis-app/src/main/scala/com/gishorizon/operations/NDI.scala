package com.gishorizon.operations

import geotrellis.layer.{Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.{ArrayMultibandTile, CellType, DoubleArrayTile, MultibandTile, Tile}
import geotrellis.spark.ContextRDD
import org.apache.spark.rdd.RDD

object NDI {

  def runProcess(inputs: Map[String, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]], operation: ProcessOperation): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {
    val id1 = operation.params.split("#")(0).split(':')(0) //operation.inputs(0).id
    val id2 = operation.params.split("#")(1).split(':')(0)
    val b1 = operation.params.split("#")(0).split(':')(1).toInt
    val b2 = operation.params.split("#")(1).split(':')(1).toInt

//    var b1 = operation.inputs(0).band
//    var b2 = operation.inputs(1).band

    var in1: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = inputs(id1)
//      ContextRDD(inputs(operation.inputs(0).id)
//        .map {
//          case (k, v) => {
//            (k.spatialKey, v)
//          }
//        }.reduceByKey(
//        (t1: MultibandTile, t2: MultibandTile) => {
//          var tils: Array[Tile] = Array()
//          for (i <- 0 until t1.bandCount) {
//            tils = tils :+ t1.band(i)
//              .combineDouble(t2.band(i)) {
//                (v1, v2) => {
//                  if (v1 == 0 && v2 == 0) {
//                    0.0
//                  } else if (v1.isNaN) {
//                    v2
//                  } else if (v2.isNaN) {
//                    v1
//                  } else {
//                    (v1 + v2) / 2
//                  }
//                }
//              }
//          }
//          val r: MultibandTile = ArrayMultibandTile(tils)
//          r
//        }
//      ).map {
//        case (k, v) => {
//          (SpaceTimeKey(k, inputs(id1).metadata.bounds.get.maxKey.time), v)
//        }
//      }, inputs(operation.inputs(0).id).metadata)

    var in2: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = inputs(id2)
//      ContextRDD(inputs(operation.inputs(1).id)
//        .map {
//          case (k, v) => {
//            (k.spatialKey, v)
//          }
//        }.reduceByKey(
//        (t1: MultibandTile, t2: MultibandTile) => {
//          var tils: Array[Tile] = Array()
//          for (i <- 0 until t1.bandCount) {
//            tils = tils :+ t1.band(i)
//              .combineDouble(t2.band(i)) {
//                (v1, v2) => {
//                  if (v1 == 0 && v2 == 0) {
//                    0.0
//                  } else if (v1.isNaN) {
//                    v2
//                  } else if (v2.isNaN) {
//                    v1
//                  } else {
//                    (v1 + v2) / 2
//                  }
//                }
//              }
//          }
//          val r: MultibandTile = ArrayMultibandTile(tils)
//          r
//        }
//      ).map {
//        case (k, v) => {
//          (SpaceTimeKey(k, inputs(id2).metadata.bounds.get.maxKey.time), v)
//        }
//      }, inputs(operation.inputs(1).id).metadata)



    val meta = in1.metadata
    val m = TileLayerMetadata(CellType.fromName("float64"), meta.layout, meta.extent, meta.crs, meta.bounds)
    val rdd: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] =
      ContextRDD(in1.++(in2)
      .reduceByKey(
        (t1: MultibandTile, t2: MultibandTile) => {
          val mt = MultibandTile(
            t1.band(b1).convert(CellType.fromName("float64")).combineDouble(t2.band(b2).convert(CellType.fromName("float64"))) {
              (v1, v2) =>
                if (v1.isNaN && v2.isNaN) {
                  v1
                } else if (v1.isNaN) {
                  v1
                } else if (v2.isNaN) {
                  v2
                } else if (v1 + v2 == 0) {
                  0
                } else {
                  (v1 - v2) / ((v1 + v2))
                }
            }
          )
          mt
        }), m)
    rdd
  }


}
