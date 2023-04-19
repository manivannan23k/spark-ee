package com.gishorizon.operations

import geotrellis.layer.{Metadata, SpatialKey, TileLayerMetadata}
import geotrellis.raster.{CellType, DoubleArrayTile, MultibandTile, Tile}
import geotrellis.spark.ContextRDD
import org.apache.spark.rdd.RDD

object NDI {

  def runProcess(inputs: Map[String, Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]]], operation: ProcessOperation): Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]] = {
    var in1: Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]] = inputs(operation.inputs(0).id)
    var in2: Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]] = inputs(operation.inputs(1).id)

    var b1 = operation.inputs(0).band
    var b2 = operation.inputs(1).band

    var outRdds: Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]] = Array()
    for(i <- in1.indices){
      val meta = in1(i).metadata
      val m = TileLayerMetadata(CellType.fromName("float64"), meta.layout, meta.extent, meta.crs, meta.bounds)
      val rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(in1(i).++(in2(i))
        .reduceByKey(
      (t1: MultibandTile, t2: MultibandTile) => {
        val mt = MultibandTile(
          t1.band(b1).convert(CellType.fromName("float64")).combineDouble(t2.band(b2).convert(CellType.fromName("float64"))) {
            (v1, v2) => {
              if (v1 + v2 == 0) {
                0.toDouble
              } else {
                (v1 - v2) / (v1 + v2)
              }
            }
          }
        )
        mt
      }), m)
      outRdds = outRdds :+ rdd

    }
    outRdds
  }


}
