package com.gishorizon.operations

import geotrellis.layer.{Metadata, SpaceTimeKey, SpatialKey, TileLayerMetadata}
import geotrellis.raster.{ArrayMultibandTile, CellType, DoubleArrayTile, MultibandTile, Tile}
import geotrellis.spark.ContextRDD
import geotrellis.spark.util.KryoSerializer
import org.apache.spark.rdd.RDD

object NDI {

  def runProcess(inputs: Map[String, RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]], operation: ProcessOperation): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {
    val id1 = operation.params.split("#")(0).split(':')(0) //operation.inputs(0).id
    val id2 = operation.params.split("#")(1).split(':')(0)
    val b1 = operation.params.split("#")(0).split(':')(1).toInt
    val b2 = operation.params.split("#")(1).split(':')(1).toInt
    var in1: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = inputs(id1)
    var in2: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = inputs(id2)
    val meta = in1.metadata

    val m = TileLayerMetadata(CellType.fromName("float64"), meta.layout, meta.extent, meta.crs, meta.bounds)
    val rdd: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] =
      ContextRDD(in1.++(in2)
      .reduceByKey(
        (t1: MultibandTile, t2: MultibandTile) => {
          MultibandTile(
            (t1.band(b1).convert(CellType.fromName("float64")) - t2.band(b2).convert(CellType.fromName("float64")))/(t1.band(b1).convert(CellType.fromName("float64")) + t2.band(b2).convert(CellType.fromName("float64"))).convert(CellType.fromName("float64"))
          )
//          val mt = MultibandTile(
//            t1.band(b1).convert(CellType.fromName("float64")).combineDouble(t2.band(b2).convert(CellType.fromName("float64"))) {
//              (v1, v2) =>
//                if (v1.isNaN && v2.isNaN) {
//                  v1
//                } else if (v1.isNaN) {
//                  v1
//                } else if (v2.isNaN) {
//                  v2
//                } else if (v1 + v2 == 0) {
//                  0
//                } else {
//                  (v1 - v2) / ((v1 + v2))
//                }
//            }
//          )
//          mt
        }), m)

    rdd.checkpoint()
    rdd.cache()
  }


}
