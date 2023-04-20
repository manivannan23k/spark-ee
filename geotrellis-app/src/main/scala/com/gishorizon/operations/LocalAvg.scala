package com.gishorizon.operations

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

  def runProcess(inputs: Map[String, Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]]], operation: ProcessOperation): Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]] = {

    var inRdds: Array[RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]] = Array()

    for(i <- operation.inputs.indices){
      val op = operation.inputs(i)
      val rdd = mergeRdds(inputs(op.id))
      val r: RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(rdd.map {
        case (k, mt) => {
          (k, mt.band(op.band))
        }
      }, rdd.metadata)
      inRdds = inRdds :+ r
    }


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
                (v1 - v2) / (v1 + v2).toDouble
              }
            }
          }
//          DoubleArrayTile(.toArray().map(e => e.toDouble), meta.layout.tileCols, meta.layout.tileRows)
        )
        mt
      }), m)
      outRdds = outRdds :+ rdd
//        .aggregateByKey(in1(i).first()._2)({
//          (l, t)=>{
////            val t1 = l.band(b1)
////            val t2 = l.band(b2)
//            MultibandTile(l.band(0), t.band(0))
//          }
//        }, {
//          (ti1, ti2) => {
//            val t1 = ti1.band(b1)
//            val t2 = ti2.band(b2)
//            val r = t1-t2
//            MultibandTile(r)
////            MultibandTile(Array.concat(t1.bands.toArray[Tile], t2.bands.toArray[Tile]))//Array.concat(t1.bands.toArray[Tile], t2.bands.toArray[Tile])
//          }
//        })

    }
    outRdds
  }

  def mergeRdds(outputData: Array[RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]]): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
    var meta = outputData(0).metadata
    outputData.foreach {
      o => {
        meta = meta.merge(o.metadata)
      }
    }
    val ratio = Math.round(((meta.extent.xmax - meta.extent.xmin) / (meta.extent.ymax - meta.extent.ymin)) / (((outputData(0).metadata.extent.xmax - outputData(0).metadata.extent.xmin)) / ((outputData(0).metadata.extent.ymax - outputData(0).metadata.extent.ymin))))
    val xTileSize = 256 * (outputData.length * ratio)
    val yTileSize = 256 * (outputData.length / ratio)
    meta = TileLayerMetadata(meta.cellType, new LayoutDefinition(meta.layout.extent, new TileLayout(1, 1, yTileSize.toInt, xTileSize.toInt)), meta.extent, meta.crs, meta.bounds)
    println(meta)
    val outProj: Array[RDD[(ProjectedExtent, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]] = outputData.map(o => {
      ContextRDD(
        o.map {
          case (k, t) => {
            (ProjectedExtent(o.metadata.mapTransform(k), o.metadata.crs), t)
          }
        }, o.metadata
      )
    })
    var out: RDD[(ProjectedExtent, MultibandTile)] = outProj(0)
    outProj.foreach {
      o => {
        out = out.merge(o)
      }
    }
    val result = ContextRDD(out, meta)

    result.tileToLayout(meta)
  }


}
